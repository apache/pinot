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
package org.apache.pinot.core.segment.index.readers.forward;

import java.nio.ByteBuffer;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.io.writer.impl.VarByteChunkSVForwardIndexWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Chunk-based single-value raw (non-dictionary-encoded) forward index reader for values of  of variable length data
 * type (STRING, BYTES).
 * <p>For data layout, please refer to the documentation for {@link VarByteChunkSVForwardIndexWriter}
 */
public final class VarByteChunkSVForwardIndexReader extends BaseChunkSVForwardIndexReader {
  private final int _maxChunkSize;

  // Thread local (reusable) byte[] to read bytes from data file.
  private final ThreadLocal<byte[]> _reusableBytes = ThreadLocal.withInitial(() -> new byte[_lengthOfLongestEntry]);

  public VarByteChunkSVForwardIndexReader(PinotDataBuffer dataBuffer, DataType valueType) {
    super(dataBuffer, valueType);
    _maxChunkSize = _numDocsPerChunk * (VarByteChunkSVForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE
        + _lengthOfLongestEntry);
  }

  @Override
  public ChunkReaderContext createContext() {
    return new ChunkReaderContext(_maxChunkSize);
  }

  @Override
  public String getString(int docId, ChunkReaderContext context) {
    int chunkRowId = docId % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkBuffer(docId, context);

    int rowOffset =
        chunkBuffer.getInt(chunkRowId * VarByteChunkSVForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE);
    int nextRowOffset = getNextRowOffset(chunkRowId, chunkBuffer);

    int length = nextRowOffset - rowOffset;
    byte[] bytes = _reusableBytes.get();

    chunkBuffer.position(rowOffset);
    chunkBuffer.get(bytes, 0, length);

    return StringUtil.decodeUtf8(bytes, 0, length);
  }

  @Override
  public byte[] getBytes(int docId, ChunkReaderContext context) {
    int chunkRowId = docId % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkBuffer(docId, context);

    int rowOffset =
        chunkBuffer.getInt(chunkRowId * VarByteChunkSVForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE);
    int nextRowOffset = getNextRowOffset(chunkRowId, chunkBuffer);

    int length = nextRowOffset - rowOffset;
    byte[] bytes = new byte[length];

    chunkBuffer.position(rowOffset);
    chunkBuffer.get(bytes, 0, length);
    return bytes;
  }

  /**
   * Helper method to compute the offset of next row in the chunk buffer.
   *
   * @param currentRowId Current row id within the chunk buffer.
   * @param chunkBuffer Chunk buffer containing the rows.
   *
   * @return Offset of next row within the chunk buffer. If current row is the last one,
   * chunkBuffer.limit() is returned.
   */
  private int getNextRowOffset(int currentRowId, ByteBuffer chunkBuffer) {
    int nextRowOffset;

    if (currentRowId == _numDocsPerChunk - 1) {
      // Last row in this trunk.
      nextRowOffset = chunkBuffer.limit();
    } else {
      nextRowOffset =
          chunkBuffer.getInt((currentRowId + 1) * VarByteChunkSVForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE);
      // For incomplete chunks, the next string's offset will be 0 as row offset for absent rows are 0.
      if (nextRowOffset == 0) {
        nextRowOffset = chunkBuffer.limit();
      }
    }
    return nextRowOffset;
  }
}
