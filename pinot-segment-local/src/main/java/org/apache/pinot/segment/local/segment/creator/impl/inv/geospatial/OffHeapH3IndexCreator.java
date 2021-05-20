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
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.locationtech.jts.geom.Geometry;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * H3 index creator that uses off-heap memory.
 * <p>The posting lists (map from H3 id to doc ids) are initially stored in a TreeMap, then flushed into a file for
 * every 100,000 entries. After all the documents are added, we read all the posting lists from the file and merge them
 * using a priority queue to calculate the final posting lists.
 * <p>Off-heap creator uses less heap memory, but is more expensive on computation and needs flush data to disk which
 * can slow down the creation because of the IO latency. Use off-heap creator in the environment where there is limited
 * heap memory or garbage collection can cause performance issue (e.g. index creation at loading time on Pinot Server).
 */
public class OffHeapH3IndexCreator extends BaseH3IndexCreator {
  private static final int FLUSH_THRESHOLD = 100_000;

  private static final String POSTING_LIST_FILE_NAME = "posting.buf";

  private final File _postingListFile;
  private final DataOutputStream _postingListOutputStream;
  private final LongList _postingListChunkEndOffsets = new LongArrayList();

  private long _postingListChunkOffset;

  public OffHeapH3IndexCreator(File indexDir, String columnName, H3IndexResolution resolution)
      throws IOException {
    super(indexDir, columnName, resolution);
    _postingListFile = new File(_tempDir, POSTING_LIST_FILE_NAME);
    _postingListOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_postingListFile)));
  }

  @Override
  public void add(Geometry geometry)
      throws IOException {
    super.add(geometry);
    if (_postingListMap.size() % FLUSH_THRESHOLD == 0) {
      flush();
    }
  }

  /**
   * Flushes the current posting list map into the file.
   */
  private void flush()
      throws IOException {
    long length = 0;
    for (Map.Entry<Long, RoaringBitmapWriter<RoaringBitmap>> entry : _postingListMap.entrySet()) {
      _postingListOutputStream.writeLong(entry.getKey());
      length += Long.BYTES;

      RoaringBitmap docIds = entry.getValue().get();
      int bitmapSize = docIds.serializedSizeInBytes();
      _postingListOutputStream.writeInt(bitmapSize);
      docIds.serialize(_postingListOutputStream);
      length += Integer.BYTES + bitmapSize;
    }
    _postingListChunkOffset += length;
    _postingListChunkEndOffsets.add(_postingListChunkOffset);
    _postingListMap.clear();
  }

  @Override
  public void seal()
      throws IOException {
    // If all posting lists are on-heap, directly generate the index file from the on-heap posting list map
    if (_postingListChunkEndOffsets.size() == 0) {
      _postingListOutputStream.close();
      for (Map.Entry<Long, RoaringBitmapWriter<RoaringBitmap>> entry : _postingListMap.entrySet()) {
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
        .mapFile(_postingListFile, true, 0, _postingListFile.length(), ByteOrder.BIG_ENDIAN, "H3 index posting list")) {
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
      PriorityQueue<PostingListEntry> priorityQueue = new PriorityQueue<>(numChunks);
      for (ChunkIterator chunkIterator : chunkIterators) {
        priorityQueue.offer(chunkIterator.next());
      }
      long currentH3Id = Long.MIN_VALUE;
      MutableRoaringBitmap currentDocIds = null;
      while (!priorityQueue.isEmpty()) {
        PostingListEntry leastEntry = priorityQueue.poll();
        if (currentH3Id == Long.MIN_VALUE || currentH3Id != leastEntry._h3Id) {
          // Find a new value
          if (currentH3Id != Long.MIN_VALUE) {
            add(currentH3Id, currentDocIds);
          }
          currentH3Id = leastEntry._h3Id;
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
      add(currentH3Id, currentDocIds);
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
      long h3Id = _dataBuffer.getLong(_offset);
      _offset += Long.BYTES;

      int bitmapSize = _dataBuffer.getInt(_offset);
      _offset += Integer.BYTES;
      ImmutableRoaringBitmap docIds = new ImmutableRoaringBitmap(_dataBuffer.toDirectByteBuffer(_offset, bitmapSize));
      _offset += bitmapSize;

      return new PostingListEntry(h3Id, docIds, _chunkId);
    }
  }

  private static class PostingListEntry implements Comparable<PostingListEntry> {
    final long _h3Id;
    final ImmutableRoaringBitmap _docIds;
    final int _chunkId;

    private PostingListEntry(long h3Id, ImmutableRoaringBitmap docIds, int chunkId) {
      _h3Id = h3Id;
      _docIds = docIds;
      _chunkId = chunkId;
    }

    @Override
    public int compareTo(PostingListEntry entry) {
      return Long.compare(_h3Id, entry._h3Id);
    }
  }
}
