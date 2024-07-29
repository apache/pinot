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
package org.apache.pinot.segment.local.io.writer.impl;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Bit-compressed dictionary-encoded forward index writer for multi-value columns. The values written are dictionary
 * ids.
 *
 * Storage Layout
 * ==============
 * There will be three sections HEADER section, BITMAP and RAW DATA
 * CHUNK OFFSET HEADER will contain one line per chunk, each line corresponding to the start offset
 * and length of the chunk
 * BITMAP This will contain sequence of bits. The number of bits will be equal to the
 * totalNumberOfValues.A bit is set to 1 if its start of a new docId. The number of bits set to 1
 * will be equal to the number of docs.
 * RAWDATA This simply has the actual multivalued data stored in sequence of int's. The number of
 * ints is equal to the totalNumberOfValues
 * We divide all the documents into groups referred to as CHUNK. Each CHUNK will
 * - Have the same number of documents.
 * - Started Offset of each CHUNK in the BITMAP will stored in the HEADER section. This is to speed
 * the look up.
 * Over all each look up will take log(NUM CHUNKS) for binary search + CHUNK to linear scan on the
 * bitmap to find the right offset in the raw data section
 */
public class FixedBitMVForwardIndexWriter implements Closeable {
  private static final int SIZE_OF_INT = 4;
  private static final int NUM_COLS_IN_HEADER = 1;
  private static final int PREFERRED_NUM_VALUES_PER_CHUNK = 2048;

  private PinotDataBuffer _indexDataBuffer;
  private PinotDataBuffer _chunkOffsetsBuffer;
  private PinotDataBuffer _bitsetBuffer;
  private PinotDataBuffer _rawDataBuffer;

  private FixedByteValueReaderWriter _chunkOffsetsWriter;
  private PinotDataBitSet _customBitSet;
  private FixedBitIntReaderWriter _rawDataWriter;
  private int _numChunks;
  private int _prevRowStartIndex = 0;
  private int _prevRowLength = 0;
  private int _chunkOffsetHeaderSize;
  private int _bitsetSize;
  private long _rawDataSize;
  private long _totalSize;
  private int _docsPerChunk;

  private int _nextDocId = 0;

  public FixedBitMVForwardIndexWriter(File file, int numDocs, int totalNumValues, int numBitsPerValue)
      throws IOException {
    float averageValuesPerDoc = totalNumValues / numDocs;
    _docsPerChunk = (int) (Math.ceil(PREFERRED_NUM_VALUES_PER_CHUNK / averageValuesPerDoc));
    _numChunks = (numDocs + _docsPerChunk - 1) / _docsPerChunk;
    _chunkOffsetHeaderSize = _numChunks * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    _bitsetSize = (totalNumValues + 7) / 8;
    _rawDataSize = ((long) totalNumValues * numBitsPerValue + 7) / 8;
    _totalSize = _chunkOffsetHeaderSize + _bitsetSize + _rawDataSize;
    Preconditions
        .checkState(_totalSize > 0 && _totalSize < Integer.MAX_VALUE, "Total size can not exceed 2GB for file: ",
            file.toString());
    // Backward-compatible: index file is always big-endian
    _indexDataBuffer =
        PinotDataBuffer.mapFile(file, false, 0, _totalSize, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());

    _chunkOffsetsBuffer = _indexDataBuffer.view(0, _chunkOffsetHeaderSize);
    int bitsetEndPos = _chunkOffsetHeaderSize + _bitsetSize;
    _bitsetBuffer = _indexDataBuffer.view(_chunkOffsetHeaderSize, bitsetEndPos);
    _rawDataBuffer = _indexDataBuffer.view(bitsetEndPos, bitsetEndPos + _rawDataSize);

    _chunkOffsetsWriter = new FixedByteValueReaderWriter(_chunkOffsetsBuffer);
    _customBitSet = new PinotDataBitSet(_bitsetBuffer);
    _rawDataWriter = new FixedBitIntReaderWriter(_rawDataBuffer, totalNumValues, numBitsPerValue);
  }

  public int getChunkOffsetHeaderSize() {
    return _chunkOffsetHeaderSize;
  }

  public int getBitsetSize() {
    return _bitsetSize;
  }

  public long getRawDataSize() {
    return _rawDataSize;
  }

  public long getTotalSize() {
    return _totalSize;
  }

  public int getNumChunks() {
    return _numChunks;
  }

  public int getRowsPerChunk() {
    return _docsPerChunk;
  }

  @Override
  public void close()
      throws IOException {
    _customBitSet.close();
    _chunkOffsetsWriter.close();
    _rawDataWriter.close();
    _indexDataBuffer.close();

    _chunkOffsetsBuffer = null;
    _bitsetBuffer = null;
    _rawDataBuffer = null;
    _customBitSet = null;
    _chunkOffsetsWriter = null;
    _rawDataWriter = null;
  }

  private int updateHeader(int length) {
    int newStartIndex = _prevRowStartIndex + _prevRowLength;
    int docId = _nextDocId++;
    if (docId % _docsPerChunk == 0) {
      int chunkId = docId / _docsPerChunk;
      _chunkOffsetsWriter.writeInt(chunkId, newStartIndex);
    }
    _customBitSet.setBit(newStartIndex);
    _prevRowStartIndex = newStartIndex;
    _prevRowLength = length;
    return newStartIndex;
  }

  public void putDictIds(int[] dictIds) {
    _rawDataWriter.writeInt(updateHeader(dictIds.length), dictIds.length, dictIds);
  }
}
