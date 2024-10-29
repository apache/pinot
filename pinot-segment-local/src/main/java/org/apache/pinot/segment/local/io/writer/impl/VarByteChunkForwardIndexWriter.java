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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.segment.local.utils.ArraySerDeUtils;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.utils.BigDecimalUtils;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Chunk-based raw (non-dictionary-encoded) forward index writer where each chunk contains fixed number of docs, and
 * the entries are variable length.
 *
 * <p>The layout of each chunk is as follows:
 * <ul>
 *   <li>
 *     Header Section: start offsets (stored as int) of the entry within the data section. For partial chunks, offset
 *     values are 0 for missing entries.
 *   </li>
 *   <li>Data Section</li>
 * </ul>
 */
@NotThreadSafe
public class VarByteChunkForwardIndexWriter extends BaseChunkForwardIndexWriter implements VarByteChunkWriter {
  public static final int CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE = Integer.BYTES;

  private final int _chunkHeaderSize;
  private int _chunkHeaderOffset;
  private int _chunkDataOffSet;

  /**
   * Constructor for the class.
   *
   * @param file File to write to.
   * @param compressionType Type of compression to use.
   * @param totalDocs Total number of docs to write.
   * @param numDocsPerChunk Number of documents per chunk.
   * @param lengthOfLongestEntry Length of longest entry (in bytes)
   * @param writerVersion writer format version
   * @throws FileNotFoundException Throws {@link FileNotFoundException} if the specified file is
   *     not found.
   */
  public VarByteChunkForwardIndexWriter(File file, ChunkCompressionType compressionType, int totalDocs,
      int numDocsPerChunk, int lengthOfLongestEntry, int writerVersion)
      throws IOException {
    super(file, compressionType, totalDocs, numDocsPerChunk,
        numDocsPerChunk * (CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE + (long) lengthOfLongestEntry),
        // chunkSize
        lengthOfLongestEntry, writerVersion, false);

    _chunkHeaderOffset = 0;
    _chunkHeaderSize = numDocsPerChunk * CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE;
    _chunkDataOffSet = _chunkHeaderSize;
  }

  @Override
  public void putBigDecimal(BigDecimal value) {
    putBytes(BigDecimalUtils.serialize(value));
  }

  @Override
  public void putString(String value) {
    putBytes(value.getBytes(UTF_8));
  }

  @Override
  public void putBytes(byte[] value) {
    _chunkBuffer.putInt(_chunkHeaderOffset, _chunkDataOffSet);
    _chunkHeaderOffset += CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE;

    _chunkBuffer.position(_chunkDataOffSet);
    _chunkBuffer.put(value);
    _chunkDataOffSet += value.length;

    writeChunkIfNecessary();
  }

  @Override
  public void putIntMV(int[] values) {
    putBytes(ArraySerDeUtils.serializeIntArrayWithLength(values));
  }

  @Override
  public void putLongMV(long[] values) {
    putBytes(ArraySerDeUtils.serializeLongArrayWithLength(values));
  }

  @Override
  public void putFloatMV(float[] values) {
    putBytes(ArraySerDeUtils.serializeFloatArrayWithLength(values));
  }

  @Override
  public void putDoubleMV(double[] values) {
    putBytes(ArraySerDeUtils.serializeDoubleArrayWithLength(values));
  }

  @Override
  public void putStringMV(String[] values) {
    putBytes(ArraySerDeUtils.serializeStringArray(values));
  }

  @Override
  public void putBytesMV(byte[][] values) {
    putBytes(ArraySerDeUtils.serializeBytesArray(values));
  }

  private void writeChunkIfNecessary() {
    // If buffer filled, then compress and write to file.
    if (_chunkHeaderOffset == _chunkHeaderSize) {
      writeChunk();
    }
  }

  /**
   * Helper method to compress and write the current chunk.
   * <ul>
   *   <li> Chunk header is of fixed size, so fills out any remaining offsets for partially filled chunks. </li>
   *   <li> Compresses and writes the chunk to the data file. </li>
   *   <li> Updates the header with the current chunks offset. </li>
   *   <li> Clears up the buffers, so that they can be reused. </li>
   * </ul>
   */
  protected void writeChunk() {
    // For partially filled chunks, we still need to clear the offsets for remaining rows, as we reuse this buffer.
    for (int i = _chunkHeaderOffset; i < _chunkHeaderSize; i += Integer.BYTES) {
      _chunkBuffer.putInt(i, 0);
    }

    super.writeChunk();

    // Reset the chunk offsets.
    _chunkHeaderOffset = 0;
    _chunkDataOffSet = _chunkHeaderSize;
  }
}
