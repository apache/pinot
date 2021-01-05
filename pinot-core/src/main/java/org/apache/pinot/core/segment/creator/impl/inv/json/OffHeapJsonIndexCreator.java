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
package org.apache.pinot.core.segment.creator.impl.inv.json;

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
import org.apache.pinot.core.io.util.VarLengthValueWriter;
import org.apache.pinot.core.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.utils.StringUtils;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Implementation of {@link org.apache.pinot.core.segment.creator.JsonIndexCreator} that uses off-heap memory.
 * <p>The posting lists (map from value to doc ids) are initially stored in a TreeMap, then flushed into a file for
 * every 100,000 documents (unflattened records) added. After all the documents are added, we read all the posting lists
 * from the file and merge them using a priority queue to calculate the final posting lists. Then we generate the string
 * dictionary and inverted index from the final posting lists and create the json index on top of them.
 * <p>Off-heap creator uses less heap memory, but is more expensive on computation and needs flush data to disk which
 * can slow down the creation because of the IO latency. Use off-heap creator in the environment where there is limited
 * heap memory or garbage collection can cause performance issue (e.g. index creation at loading time on Pinot Server).
 */
public class OffHeapJsonIndexCreator extends BaseJsonIndexCreator {
  private static final int FLUSH_THRESHOLD = 100_000;

  private static final String POSTING_LIST_FILE_NAME = "posting.buf";
  private static final String FINAL_POSTING_LIST_FILE_NAME = "final.posting.buf";

  private final File _postingListFile;
  private final DataOutputStream _postingListOutputStream;
  private final LongList _postingListChunkEndOffsets = new LongArrayList();

  private int _nextDocId;
  private long _postingListChunkOffset;
  private int _maxBitmapSize;
  private int _numPostingListsInLastChunk;
  private int _numPostingLists;

  public OffHeapJsonIndexCreator(File indexDir, String columnName)
      throws IOException {
    super(indexDir, columnName);
    _postingListFile = new File(_tempDir, POSTING_LIST_FILE_NAME);
    _postingListOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_postingListFile)));
  }

  @Override
  void addFlattenedRecords(List<Map<String, String>> records)
      throws IOException {
    super.addFlattenedRecords(records);
    _nextDocId++;
    if (_nextDocId % FLUSH_THRESHOLD == 0) {
      flush();
    }
  }

  /**
   * Flushes the current posting list map into the file.
   */
  private void flush()
      throws IOException {
    long length = 0;
    for (Map.Entry<String, RoaringBitmapWriter<RoaringBitmap>> entry : _postingListMap.entrySet()) {
      byte[] valueBytes = StringUtils.encodeUtf8(entry.getKey());
      int valueLength = valueBytes.length;
      _maxValueLength = Integer.max(_maxValueLength, valueLength);
      _postingListOutputStream.writeInt(valueLength);
      _postingListOutputStream.write(valueBytes);
      length += Integer.BYTES + valueLength;

      RoaringBitmap docIds = entry.getValue().get();
      int bitmapSize = docIds.serializedSizeInBytes();
      _maxBitmapSize = Integer.max(_maxBitmapSize, bitmapSize);
      _postingListOutputStream.writeInt(bitmapSize);
      docIds.serialize(_postingListOutputStream);
      length += Integer.BYTES + bitmapSize;
    }
    _postingListChunkOffset += length;
    _postingListChunkEndOffsets.add(_postingListChunkOffset);
    _numPostingListsInLastChunk = _postingListMap.size();
    _postingListMap.clear();
  }

  @Override
  public void seal()
      throws IOException {
    if (_nextDocId % FLUSH_THRESHOLD != 0) {
      flush();
    }
    _postingListOutputStream.close();

    // Merge posting lists to create the final posting list file
    File finalPostingListFile;
    byte[] valueBytesBuffer = new byte[_maxValueLength];
    if (_postingListChunkEndOffsets.size() == 1) {
      // No need to merge if there is only 1 chunk
      finalPostingListFile = _postingListFile;
      _numPostingLists = _numPostingListsInLastChunk;
    } else {
      finalPostingListFile = createFinalPostingListFile(valueBytesBuffer);
    }

    // Read the final posting list file and create the dictionary and inverted index file
    try (PinotDataBuffer finalPostingListBuffer = PinotDataBuffer
        .mapFile(finalPostingListFile, true, 0, finalPostingListFile.length(), ByteOrder.BIG_ENDIAN,
            "Json index final posting list");
        VarLengthValueWriter dictionaryWriter = new VarLengthValueWriter(_dictionaryFile, _numPostingLists);
        BitmapInvertedIndexWriter invertedIndexWriter = new BitmapInvertedIndexWriter(_invertedIndexFile,
            _numPostingLists)) {
      byte[] bitmapBytesBuffer = new byte[_maxBitmapSize];
      long offset = 0;
      for (int i = 0; i < _numPostingLists; i++) {
        int valueLength = finalPostingListBuffer.getInt(offset);
        offset += Integer.BYTES;
        finalPostingListBuffer.copyTo(offset, valueBytesBuffer, 0, valueLength);
        offset += valueLength;
        dictionaryWriter.add(valueBytesBuffer, valueLength);

        int bitmapSize = finalPostingListBuffer.getInt(offset);
        offset += Integer.BYTES;
        finalPostingListBuffer.copyTo(offset, bitmapBytesBuffer, 0, bitmapSize);
        offset += bitmapSize;
        invertedIndexWriter.add(bitmapBytesBuffer, bitmapSize);
      }
    }

    generateIndexFile();
  }

  private File createFinalPostingListFile(byte[] valueBytesBuffer)
      throws IOException {
    File finalPostingListFile = new File(_tempDir, FINAL_POSTING_LIST_FILE_NAME);
    try (PinotDataBuffer postingListBuffer = PinotDataBuffer
        .mapFile(_postingListFile, true, 0, _postingListFile.length(), ByteOrder.BIG_ENDIAN,
            "Json index posting list")) {
      // Create chunk iterators from the posting list file
      int numChunks = _postingListChunkEndOffsets.size();
      List<ChunkIterator> chunkIterators = new ArrayList<>(numChunks);
      long chunkEndOffset = 0;
      for (int i = 0; i < numChunks; i++) {
        long chunkStartOffset = chunkEndOffset;
        chunkEndOffset = _postingListChunkEndOffsets.getLong(i);
        PinotDataBuffer chunkDataBuffer = postingListBuffer.view(chunkStartOffset, chunkEndOffset);
        chunkIterators.add(new ChunkIterator(chunkDataBuffer, i, valueBytesBuffer));
      }

      // Merge posting lists from the chunk iterators
      try (DataOutputStream finalPostingListOutputStream = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(finalPostingListFile)))) {
        PriorityQueue<PostingListEntry> priorityQueue = new PriorityQueue<>(numChunks);
        for (ChunkIterator chunkIterator : chunkIterators) {
          if (chunkIterator.hasNext()) {
            priorityQueue.offer(chunkIterator.next());
          }
        }
        String currentValue = null;
        MutableRoaringBitmap currentDocIds = null;
        while (!priorityQueue.isEmpty()) {
          PostingListEntry leastEntry = priorityQueue.poll();
          if (currentValue == null || !currentValue.equals(leastEntry._value)) {
            // Find a new value
            if (currentValue != null) {
              writeToFinalPostingList(finalPostingListOutputStream, currentValue, currentDocIds);
            }
            currentValue = leastEntry._value;
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
        if (currentValue != null) {
          writeToFinalPostingList(finalPostingListOutputStream, currentValue, currentDocIds);
        }
      }
    }
    return finalPostingListFile;
  }

  private void writeToFinalPostingList(DataOutputStream finalPostingListOutputStream, String value,
      MutableRoaringBitmap docIds)
      throws IOException {
    byte[] valueBytes = StringUtils.encodeUtf8(value);
    finalPostingListOutputStream.writeInt(valueBytes.length);
    finalPostingListOutputStream.write(valueBytes);

    int bitmapSize = docIds.serializedSizeInBytes();
    _maxBitmapSize = Integer.max(_maxBitmapSize, bitmapSize);
    finalPostingListOutputStream.writeInt(bitmapSize);
    docIds.serialize(finalPostingListOutputStream);

    _numPostingLists++;
  }

  private static class ChunkIterator implements Iterator<PostingListEntry> {
    final PinotDataBuffer _dataBuffer;
    final long _bufferSize;
    final int _chunkId;
    final byte[] _valueBytesBuffer;

    long _offset;

    ChunkIterator(PinotDataBuffer dataBuffer, int chunkId, byte[] valueBytesBuffer) {
      _dataBuffer = dataBuffer;
      _bufferSize = dataBuffer.size();
      _chunkId = chunkId;
      _valueBytesBuffer = valueBytesBuffer;
    }

    @Override
    public boolean hasNext() {
      return _offset < _bufferSize;
    }

    @Override
    public PostingListEntry next() {
      int valueLength = _dataBuffer.getInt(_offset);
      _offset += Integer.BYTES;
      _dataBuffer.copyTo(_offset, _valueBytesBuffer, 0, valueLength);
      String value = StringUtils.decodeUtf8(_valueBytesBuffer, 0, valueLength);
      _offset += valueLength;

      int bitmapSize = _dataBuffer.getInt(_offset);
      _offset += Integer.BYTES;
      ImmutableRoaringBitmap docIds = new ImmutableRoaringBitmap(_dataBuffer.toDirectByteBuffer(_offset, bitmapSize));
      _offset += bitmapSize;

      return new PostingListEntry(value, docIds, _chunkId);
    }
  }

  private static class PostingListEntry implements Comparable<PostingListEntry> {
    final String _value;
    final ImmutableRoaringBitmap _docIds;
    final int _chunkId;

    private PostingListEntry(String value, ImmutableRoaringBitmap docIds, int chunkId) {
      _value = value;
      _docIds = docIds;
      _chunkId = chunkId;
    }

    @Override
    public int compareTo(PostingListEntry entry) {
      return _value.compareTo(entry._value);
    }
  }
}
