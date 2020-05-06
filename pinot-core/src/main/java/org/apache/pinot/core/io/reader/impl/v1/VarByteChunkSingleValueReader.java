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
package org.apache.pinot.core.io.reader.impl.v1;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.io.reader.impl.ChunkReaderContext;
import org.apache.pinot.core.io.writer.impl.v1.VarByteChunkSingleValueWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


/**
 * Reader class for data written out by {@link VarByteChunkSingleValueWriter}.
 * For data layout, please refer to the documentation for {@link VarByteChunkSingleValueWriter}
 */
public class VarByteChunkSingleValueReader extends BaseChunkSingleValueReader {
  private final int _maxChunkSize;

  // Thread local (reusable) byte[] to read bytes from data file.
  private final ThreadLocal<byte[]> _reusableBytes = ThreadLocal.withInitial(() -> new byte[_lengthOfLongestEntry]);

  /**
   * Constructor for the class.
   *
   * @param pinotDataBuffer Data buffer to read from
   */
  public VarByteChunkSingleValueReader(PinotDataBuffer pinotDataBuffer) {
    super(pinotDataBuffer);
    _maxChunkSize = _numDocsPerChunk * (VarByteChunkSingleValueWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE + _lengthOfLongestEntry);
  }

  @Override
  public String getString(int row) {
    return getString(row, createContext());
  }

  @Override
  public String getString(int row, ChunkReaderContext context) {
    int chunkRowId = row % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(row, context);

    int rowOffset = chunkBuffer.getInt(chunkRowId * VarByteChunkSingleValueWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE);
    int nextRowOffset = getNextRowOffset(chunkRowId, chunkBuffer);

    int length = nextRowOffset - rowOffset;
    byte[] bytes = _reusableBytes.get();

    ((Buffer) chunkBuffer).position(rowOffset);
    chunkBuffer.get(bytes, 0, length);

    return StringUtil.decodeUtf8(bytes, 0, length);
  }

  @Override
  public byte[] getBytes(int row) {
    return getBytes(row, createContext());
  }

  @Override
  public byte[] getBytes(int row, ChunkReaderContext context) {
    int chunkRowId = row % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(row, context);

    int rowOffset = chunkBuffer.getInt(chunkRowId * VarByteChunkSingleValueWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE);
    int nextRowOffset = getNextRowOffset(chunkRowId, chunkBuffer);

    int length = nextRowOffset - rowOffset;
    byte[] bytes = new byte[length];

    ((Buffer) chunkBuffer).position(rowOffset);
    chunkBuffer.get(bytes, 0, length);
    return bytes;
  }

  @Override
  public ChunkReaderContext createContext() {
    return new ChunkReaderContext(_maxChunkSize);
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
      nextRowOffset = chunkBuffer.getInt((currentRowId + 1) * VarByteChunkSingleValueWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE);
      // For incomplete chunks, the next string's offset will be 0 as row offset for absent rows are 0.
      if (nextRowOffset == 0) {
        nextRowOffset = chunkBuffer.limit();
      }
    }
    return nextRowOffset;
  }
}
