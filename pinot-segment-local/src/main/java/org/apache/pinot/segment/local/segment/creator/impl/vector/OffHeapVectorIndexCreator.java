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
package org.apache.pinot.segment.local.segment.creator.impl.vector;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.utils.VectorUtils;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class OffHeapVectorIndexCreator extends BaseVectorIndexCreator {
  private static final int FLUSH_THRESHOLD = 100_000;

  private static final String POSTING_LIST_FILE_NAME = "posting.buf";

  private final File _postingListFile;
  private final DataOutputStream _postingListOutputStream;
  private final LongList _postingListChunkEndOffsets = new LongArrayList();

  private long _postingListChunkOffset;

  public OffHeapVectorIndexCreator(File indexDir, String columnName, int vectorLength, int vectorValueSize)
      throws IOException {
    super(indexDir, columnName, vectorLength, vectorValueSize);
    _postingListFile = new File(_tempDir, POSTING_LIST_FILE_NAME);
    _postingListOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_postingListFile)));
  }

  @Override
  public void add(float[] vector) {
    super.add(vector);
    if (_postingListMap.size() % FLUSH_THRESHOLD == 0) {
      flush();
    }
  }

  private void flush() {
    try {
      long length = 0;
      for (Map.Entry<float[], RoaringBitmapWriter<RoaringBitmap>> entry : _postingListMap.entrySet()) {
        byte[] vectorBytes = VectorUtils.toBytes(entry.getKey());
        _postingListOutputStream.write(vectorBytes);  // Use Vector's toBytes method
        length += vectorBytes.length;

        RoaringBitmap docIds = entry.getValue().get();
        int bitmapSize = docIds.serializedSizeInBytes();
        _postingListOutputStream.writeInt(bitmapSize);
        docIds.serialize(_postingListOutputStream);
        length += Integer.BYTES + bitmapSize;
      }
      _postingListChunkOffset += length;
      _postingListChunkEndOffsets.add(_postingListChunkOffset);
      _postingListMap.clear();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void seal()
      throws IOException {
    // If all posting lists are on-heap, directly generate the index file from the on-heap posting list map
    if (_postingListChunkEndOffsets.isEmpty()) {
      _postingListOutputStream.close();
      for (Map.Entry<float[], RoaringBitmapWriter<RoaringBitmap>> entry : _postingListMap.entrySet()) {
        add(entry.getKey(), entry.getValue().get());
      }
      generateIndexFile();
      return;
    }

    // Flush the last chunk
    if (_postingListMap.size() % FLUSH_THRESHOLD != 0) {
      flush();
    }
    _postingListOutputStream.close();

    // Merge posting lists to calculate the final posting lists
    try (PinotDataBuffer postingListBuffer = PinotDataBuffer
        .mapFile(_postingListFile, true, 0, _postingListFile.length(), ByteOrder.BIG_ENDIAN,
            "Vector index posting list")) {
      // Create chunk iterators from the posting list file
      int numChunks = _postingListChunkEndOffsets.size();
      List<ChunkIterator> chunkIterators = new ArrayList<>(numChunks);
      long chunkEndOffset = 0;
      for (int i = 0; i < numChunks; i++) {
        long chunkStartOffset = chunkEndOffset;
        chunkEndOffset = _postingListChunkEndOffsets.getLong(i);
        PinotDataBuffer chunkDataBuffer = postingListBuffer.view(chunkStartOffset, chunkEndOffset);
        chunkIterators.add(new ChunkIterator(chunkDataBuffer, i));
      }

      // Merge posting lists from the chunk iterators
      PriorityQueue<OffHeapVectorIndexCreator.PostingListEntry> priorityQueue = new PriorityQueue<>(numChunks);
      for (OffHeapVectorIndexCreator.ChunkIterator chunkIterator : chunkIterators) {
        priorityQueue.offer(chunkIterator.next());
      }
      float[] currentVector = null;
      MutableRoaringBitmap currentDocIds = null;
      while (!priorityQueue.isEmpty()) {
        PostingListEntry leastEntry = priorityQueue.poll();
        if (currentVector == null || currentVector != leastEntry._value) {
          // Find a new value
          if (currentVector != null) {
            add(currentVector, currentDocIds);
          }
          currentVector = leastEntry._value;
          currentDocIds = leastEntry._docIds.toMutableRoaringBitmap();
        } else {
          // Same value
          currentDocIds.or(leastEntry._docIds);
        }
        ChunkIterator chunkIterator = chunkIterators.get(leastEntry._chunkId);
        if (chunkIterator.hasNext()) {
          priorityQueue.offer(chunkIterator.next());
        }
      }
      assert currentDocIds != null;
      add(currentVector, currentDocIds);
      generateIndexFile();
    }
  }

  @Override
  public void close()
      throws IOException {
    _postingListOutputStream.close();
    super.close();
  }

  private static class ChunkIterator implements Iterator<PostingListEntry> {
    final PinotDataBuffer _dataBuffer;
    final long _bufferSize;
    final int _chunkId;

    long _offset;

    ChunkIterator(PinotDataBuffer dataBuffer, int chunkId) {
      _dataBuffer = dataBuffer;
      _bufferSize = dataBuffer.size();
      _chunkId = chunkId;
    }

    @Override
    public boolean hasNext() {
      return _offset < _bufferSize;
    }

    @Override
    public PostingListEntry next() {
      int vectorLength = _dataBuffer.getInt(_offset);
      _offset += Integer.BYTES;

      byte vectorType = _dataBuffer.getByte(_offset);
      _offset += Byte.BYTES;

      int vectorSize = vectorType == 0 ? vectorLength * Float.BYTES : vectorLength * Integer.BYTES;

      float[] vector = VectorUtils.fromBytes(_dataBuffer.toDirectByteBuffer(_offset, vectorSize).array());
      _offset += vectorSize;

      int bitmapSize = _dataBuffer.getInt(_offset);
      _offset += Integer.BYTES;
      ImmutableRoaringBitmap docIds = new ImmutableRoaringBitmap(_dataBuffer.toDirectByteBuffer(_offset, bitmapSize));
      _offset += bitmapSize;

      return new PostingListEntry(vector, docIds, _chunkId);
    }
  }

  private static class PostingListEntry implements Comparable<PostingListEntry> {
    final float[] _value;
    final ImmutableRoaringBitmap _docIds;
    final int _chunkId;

    private PostingListEntry(float[] value, ImmutableRoaringBitmap docIds, int chunkId) {
      _value = value;
      _docIds = docIds;
      _chunkId = chunkId;
    }

    @Override
    public int compareTo(PostingListEntry entry) {
      int length = _value.length;
      for (int i = 0; i < length; i++) {
        float value1 = _value[i];
        float value2 = entry._value[i];
        if (value1 != value2) {
          return Float.compare(value1, value2);
        }
      }
      return 0;
    }
  }
}
