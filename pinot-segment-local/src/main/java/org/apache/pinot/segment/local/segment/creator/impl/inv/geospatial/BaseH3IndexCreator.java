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
package org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial;

import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.H3Utils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.GeoSpatialIndexCreator;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.Container;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


/**
 * Base implementation of the H3 index creator.
 * <p>Index file layout:
 * <ul>
 *   <li>Header</li>
 *   <ul>
 *     <li>Version (int)</li>
 *     <li>Number of unique H3 ids (int)</li>
 *     <li>Resolutions (short)</li>
 *   </ul>
 *   <li>Long dictionary</li>
 *   <li>Bitmap inverted index</li>
 * </ul>
 */
public abstract class BaseH3IndexCreator implements GeoSpatialIndexCreator {
  public static final int VERSION = 1;
  public static final int HEADER_LENGTH = 10;

  static final String TEMP_DIR_SUFFIX = V1Constants.Indexes.H3_INDEX_FILE_EXTENSION + ".tmp";
  static final String DICTIONARY_FILE_NAME = "dictionary.buf";
  static final String BITMAP_OFFSET_FILE_NAME = "bitmap.offset.buf";
  static final String BITMAP_VALUE_FILE_NAME = "bitmap.value.buf";

  final File _indexFile;
  final File _tempDir;
  final File _dictionaryFile;
  final File _bitmapOffsetFile;
  final File _bitmapValueFile;
  final DataOutputStream _dictionaryStream;
  final DataOutputStream _bitmapOffsetStream;
  final DataOutputStream _bitmapValueStream;
  final H3IndexResolution _resolution;
  final int _lowestResolution;
  final Map<Long, RoaringBitmapWriter<RoaringBitmap>> _postingListMap = new TreeMap<>();
  final RoaringBitmapWriter.Wizard<Container, RoaringBitmap> _bitmapWriterWizard =
      RoaringBitmapWriter.writer().runCompress(false);

  int _nextDocId;

  BaseH3IndexCreator(File indexDir, String columnName, H3IndexResolution resolution)
      throws IOException {
    _indexFile = new File(indexDir, columnName + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
    _tempDir = new File(indexDir, columnName + TEMP_DIR_SUFFIX);
    if (_tempDir.exists()) {
      FileUtils.cleanDirectory(_tempDir);
    } else {
      FileUtils.forceMkdir(_tempDir);
    }
    _dictionaryFile = new File(_tempDir, DICTIONARY_FILE_NAME);
    _bitmapOffsetFile = new File(_tempDir, BITMAP_OFFSET_FILE_NAME);
    _bitmapValueFile = new File(_tempDir, BITMAP_VALUE_FILE_NAME);
    _dictionaryStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_dictionaryFile)));
    _bitmapOffsetStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_bitmapOffsetFile)));
    _bitmapValueStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_bitmapValueFile)));
    _resolution = resolution;
    _lowestResolution = resolution.getLowestResolution();
  }

  @Override
  public Geometry deserialize(byte[] bytes) {
    return GeometrySerializer.deserialize(bytes);
  }

  @Override
  public void add(Geometry geometry)
      throws IOException {
    Preconditions.checkState(geometry instanceof Point, "H3 index can only be applied to Point, got: %s",
        geometry.getGeometryType());
    Coordinate coordinate = geometry.getCoordinate();
    // TODO: support multiple resolutions
    long h3Id = H3Utils.H3_CORE.geoToH3(coordinate.y, coordinate.x, _lowestResolution);
    RoaringBitmapWriter<RoaringBitmap> bitmapWriter = _postingListMap.get(h3Id);
    if (bitmapWriter == null) {
      bitmapWriter = _bitmapWriterWizard.get();
      _postingListMap.put(h3Id, bitmapWriter);
    }
    bitmapWriter.add(_nextDocId++);
  }

  /**
   * Writes the bitmap to the temporary index files for the given H3 id.
   */
  void add(long h3Id, BitmapDataProvider bitmap)
      throws IOException {
    _dictionaryStream.writeLong(h3Id);
    _bitmapOffsetStream.writeInt(_bitmapValueStream.size());
    bitmap.serialize(_bitmapValueStream);
  }

  /**
   * Generates the final index file from the temporary index files. Should be called after all the bitmaps are added to
   * the temporary index files.
   */
  void generateIndexFile()
      throws IOException {
    // Write the end offset of the last bitmap
    _bitmapOffsetStream.writeInt(_bitmapValueStream.size());

    _dictionaryStream.close();
    _bitmapOffsetStream.close();
    _bitmapValueStream.close();

    ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_LENGTH);
    headerBuffer.putInt(VERSION);
    headerBuffer.putInt(_dictionaryStream.size() / Long.BYTES);
    headerBuffer.putShort(_resolution.serialize());
    headerBuffer.position(0);

    try (FileChannel indexFileChannel = new RandomAccessFile(_indexFile, "rw").getChannel();
        FileChannel dictionaryFileChannel = new RandomAccessFile(_dictionaryFile, "r").getChannel();
        FileChannel bitmapOffsetFileChannel = new RandomAccessFile(_bitmapOffsetFile, "r").getChannel();
        FileChannel bitmapValueFileChannel = new RandomAccessFile(_bitmapValueFile, "r").getChannel()) {
      indexFileChannel.write(headerBuffer);
      org.apache.pinot.common.utils.FileUtils.transferBytes(dictionaryFileChannel, 0, _dictionaryFile.length(),
          indexFileChannel);
      org.apache.pinot.common.utils.FileUtils.transferBytes(bitmapOffsetFileChannel, 0, _bitmapOffsetFile.length(),
          indexFileChannel);
      org.apache.pinot.common.utils.FileUtils.transferBytes(bitmapValueFileChannel, 0, _bitmapValueFile.length(),
          indexFileChannel);
      indexFileChannel.force(true);
    }
  }

  @Override
  public void close()
      throws IOException {
    _dictionaryStream.close();
    _bitmapOffsetStream.close();
    _bitmapValueStream.close();

    FileUtils.deleteDirectory(_tempDir);
  }
}
