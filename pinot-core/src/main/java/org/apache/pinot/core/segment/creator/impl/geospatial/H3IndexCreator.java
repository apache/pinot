/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.segment.creator.impl.geospatial;

import com.uber.h3core.H3Core;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.creator.GeoSpatialIndexCreator;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import static org.apache.pinot.core.segment.creator.impl.V1Constants.Indexes.H3_INDEX_FILE_EXTENSION;


/**
 * The creator of H3 index.
 * <p>2 passes are used to create the h3 index via merge sort.
 *  * <ul>
 *  *   <li>
 *  *     In the first pass (adding values phase), when add() method is called, derive the h3 index from the given latitude,
 *  *     and longitude with the specified resolution, and store the index in chunks with each chunk sorted.
 *  *   </li>
 *  *   <li>
 *  *     In the second pass, when seal() method is called, the sorted chunks are merged into the index file.
 *  *   </li>
 *  * </ul>
 * <pre>
 *
 * Layout for H3 inverted index, header & dictionary & offset & bitmap :
 * |-------------------------------------------------------------------------|
 * |                                     version                             |
 * |                               Number of unique H3Ids                    |
 * |                                     resolutions                         |
 * |-------------------------------------------------------------------------|
 * |                                    1st H3Id                             |
 * |                                    2nd H3Id                             |
 * |                                    ...                                  |
 * |                                    last H3Id                            |
 * |-------------------------------------------------------------------------|
 * |                        Offset of the bitmap for 1st H3Id                |
 * |                        Offset of the bitmap for 2nd H3Id                |
 * |                                   ...                                   |
 * |                       Offset of the bitmap for last H3Id                |
 * |-------------------------------------------------------------------------|
 * |                         Serialized bitmap for 1st H3Id                  |
 * |                         Serialized bitmap for 2nd H3Id                  |
 * |                                   ...                                   |
 * |                        Serialized bitmap for last H3Id                  |
 * |-------------------------------------------------------------------------|
 * </pre>
 *
 */
public class H3IndexCreator implements GeoSpatialIndexCreator {
  private static final String CHUNK_PREFIX = "chunk-";

  public static final int VERSION = 1;
  private static final int FLUSH_THRESHOLD = 100_000;
  private final H3Core _h3Core;
  private File _indexDir;
  private FieldSpec _fieldSpec;
  private H3IndexResolution _resolution;
  private int _lowestResolution;

  TreeMap<Long, MutableRoaringBitmap> _h3IndexMap;

  int numChunks = 0;
  List<Integer> chunkLengths = new ArrayList<>();

  public H3IndexCreator(File indexDir, FieldSpec fieldSpec, H3IndexResolution resolution)
      throws IOException {
    _indexDir = indexDir;
    _fieldSpec = fieldSpec;
    _resolution = resolution;
    _h3Core = H3Core.newInstance();
    _resolution = resolution;
    _lowestResolution = resolution.getLowestResolution();
    _h3IndexMap = new TreeMap<>();
  }

  @Override
  public void add(int docId, double lat, double lon) {
    // TODO support multiple resolutions
    Long h3Id = _h3Core.geoToH3(lat, lon, _lowestResolution);
    MutableRoaringBitmap roaringBitmap = _h3IndexMap.get(h3Id);
    if (roaringBitmap == null) {
      roaringBitmap = new MutableRoaringBitmap();
      _h3IndexMap.put(h3Id, roaringBitmap);
    }
    roaringBitmap.add(docId);
    if (_h3IndexMap.size() > FLUSH_THRESHOLD) {
      flush();
    }
  }

  private void flush() {
    File tempChunkFile = new File(_indexDir, CHUNK_PREFIX + numChunks);
    //dump whatever we have in _h3IndexMap in a sorted order
    try {
      DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempChunkFile));
      chunkLengths.add(_h3IndexMap.size());
      for (Map.Entry<Long, MutableRoaringBitmap> entry : _h3IndexMap.entrySet()) {
        Long h3Id = entry.getKey();
        MutableRoaringBitmap bitmap = entry.getValue();
        dos.writeLong(h3Id);
        //write bitmap
        int serializedSizeInBytes = bitmap.serializedSizeInBytes();
        byte[] byteArray = new byte[serializedSizeInBytes];
        bitmap.serialize(ByteBuffer.wrap(byteArray));
        dos.writeInt(serializedSizeInBytes);
        dos.write(byteArray);
      }
      dos.close();
      _h3IndexMap.clear();
      numChunks++;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to create h3 index in %s", tempChunkFile), e);
    }
  }

  public void seal()
      throws IOException {
    flush();

    // merge sort all the chunk files, since they are sorted we can write the dictionary as well
    PriorityQueue<Entry> queue = new PriorityQueue<>();

    ChunkReader[] chunkReaders = new ChunkReader[numChunks];
    for (int chunkId = 0; chunkId < numChunks; chunkId++) {
      File chunkFile = new File(_indexDir, CHUNK_PREFIX + chunkId);
      chunkReaders[chunkId] = new ChunkReader(chunkId, chunkLengths.get(chunkId), chunkFile);
      Entry e = chunkReaders[chunkId].getNextEntry();
      queue.add(e);
    }

    long prevH3Id = -1;
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();

    File headerFile = new File(_indexDir, _fieldSpec.getName() + "-h3index-header.buffer");
    DataOutputStream headerStream = new DataOutputStream(new FileOutputStream(headerFile));

    File dictionaryFile = new File(_indexDir, _fieldSpec.getName() + "-h3index-dictionary.buffer");
    DataOutputStream dictionaryStream = new DataOutputStream(new FileOutputStream(dictionaryFile));

    File offsetFile = new File(_indexDir, _fieldSpec.getName() + "-h3index-offset.buffer");
    DataOutputStream offsetStream = new DataOutputStream(new FileOutputStream(offsetFile));

    File bitmapFile = new File(_indexDir, _fieldSpec.getName() + "-h3index-bitmap.buffer");
    DataOutputStream bitmapStream = new DataOutputStream(new FileOutputStream(bitmapFile));

    Writer writer = new Writer(dictionaryStream, offsetStream, bitmapStream);
    while (queue.size() > 0) {
      Entry poll = queue.poll();
      long currH3Id = poll.h3Id;
      if (prevH3Id != -1 && currH3Id != prevH3Id) {
        writer.add(prevH3Id, bitmap);
        bitmap.clear();
      }
      bitmap.or(poll.bitmap);

      prevH3Id = currH3Id;

      Entry entry = chunkReaders[poll.chunkId].getNextEntry();
      if (entry != null) {
        queue.add(entry);
      }
    }
    if (prevH3Id != -1) {
      writer.add(prevH3Id, bitmap);
    }

    //write header file
    headerStream.writeInt(VERSION);
    headerStream.writeInt(writer.getNumUniqueIds());
    headerStream.writeShort(_resolution.serialize());
    headerStream.close();
    dictionaryStream.close();
    offsetStream.close();
    bitmapStream.close();

    File outputFile = new File(_indexDir, _fieldSpec.getName() + H3_INDEX_FILE_EXTENSION);
    long length = headerStream.size() + dictionaryStream.size() + offsetStream.size() + bitmapStream.size();
    //write the actual file
    PinotDataBuffer h3IndexBuffer =
        PinotDataBuffer.mapFile(outputFile, false, 0, length, ByteOrder.BIG_ENDIAN, "H3 Index Buffer");

    long writtenBytes = 0;
    h3IndexBuffer.readFrom(writtenBytes, headerFile, 0, headerFile.length());
    writtenBytes += headerFile.length();

    h3IndexBuffer.readFrom(writtenBytes, dictionaryFile, 0, dictionaryFile.length());
    writtenBytes += dictionaryFile.length();

    h3IndexBuffer.readFrom(writtenBytes, offsetFile, 0, offsetFile.length());
    writtenBytes += offsetFile.length();

    h3IndexBuffer.readFrom(writtenBytes, bitmapFile, 0, bitmapFile.length());
  }

  @Override
  public void close()
      throws IOException {
    //delete chunk files
    for (int chunkId = 0; chunkId < numChunks; chunkId++) {
      File chunkFile = new File(_indexDir, CHUNK_PREFIX + chunkId);
      FileUtils.forceDelete(chunkFile);
    }
  }

  /**
   * Reader of an index chunk.
   */
  class ChunkReader {
    private int _chunkId;
    private Integer _chunkLength;
    private DataInputStream dataInputStream;
    int index = 0;

    ChunkReader(int chunkId, Integer chunkLength, File chunkFile)
        throws IOException {
      _chunkId = chunkId;
      _chunkLength = chunkLength;
      dataInputStream = new DataInputStream(new FileInputStream(chunkFile));
    }

    private Entry getNextEntry()
        throws IOException {
      if (index >= _chunkLength) {
        return null;
      }
      long h3Id = dataInputStream.readLong();
      int size = dataInputStream.readInt();
      byte[] serializedBytes = new byte[size];
      dataInputStream.read(serializedBytes);
      ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(serializedBytes));
      index++;
      return new Entry(_chunkId, h3Id, bitmap);
    }
  }

  class Entry implements Comparable<Entry> {
    int chunkId;
    long h3Id;
    ImmutableRoaringBitmap bitmap;

    @Override
    public boolean equals(Object o) {
      return h3Id == ((Entry) o).h3Id;
    }

    public Entry(int chunkId, long h3Id, ImmutableRoaringBitmap bitmap) {
      this.chunkId = chunkId;
      this.h3Id = h3Id;
      this.bitmap = bitmap;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(h3Id);
    }

    @Override
    public int compareTo(Entry o) {
      return Long.compare(h3Id, o.h3Id);
    }
  }

  private class Writer {
    private DataOutputStream _dictionaryStream;
    private DataOutputStream _offsetStream;
    private DataOutputStream _bitmapStream;
    private int _offset = 0;
    private int _numUniqueIds = 0;

    Writer(DataOutputStream dictionaryStream, DataOutputStream offsetStream, DataOutputStream bitmapStream) {
      _dictionaryStream = dictionaryStream;
      _offsetStream = offsetStream;
      _bitmapStream = bitmapStream;
    }

    int getNumUniqueIds() {
      return _numUniqueIds;
    }

    void add(long h3Id, ImmutableRoaringBitmap bitmap)
        throws IOException {
      _dictionaryStream.writeLong(h3Id);
      _offsetStream.writeInt(_offset);
      int serializedSizeInBytes = bitmap.serializedSizeInBytes();
      byte[] byteArray = new byte[serializedSizeInBytes];
      bitmap.serialize(ByteBuffer.wrap(byteArray));
      _bitmapStream.write(byteArray);
      _offset += serializedSizeInBytes;
      _numUniqueIds++;
    }
  }
}
