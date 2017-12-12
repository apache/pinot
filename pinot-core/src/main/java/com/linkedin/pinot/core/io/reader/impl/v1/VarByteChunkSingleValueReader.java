/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.io.reader.impl.v1;

import com.linkedin.pinot.core.io.reader.impl.ChunkReaderContext;
import com.linkedin.pinot.core.io.writer.impl.v1.VarByteChunkSingleValueWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;


/**
 * Reader class for data written out by {@link VarByteChunkSingleValueWriter}.
 * For data layout, please refer to the documentation for {@link VarByteChunkSingleValueWriter}
 */
public class VarByteChunkSingleValueReader extends BaseChunkSingleValueReader {
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private final int _maxChunkSize;

  // Thread local (reusable) byte[] to read bytes from data file.
  private final ThreadLocal<byte[]> _reusableBytes = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[_lengthOfLongestEntry];
    }
  };

  /**
   * Constructor for the class.
   *
   * @param pinotDataBuffer Data buffer to read from
   * @throws IOException
   */
  public VarByteChunkSingleValueReader(PinotDataBuffer pinotDataBuffer)
      throws IOException {
    super(pinotDataBuffer);

    int chunkHeaderSize = _numDocsPerChunk * INT_SIZE;
    _maxChunkSize = chunkHeaderSize + (_lengthOfLongestEntry * _numDocsPerChunk);
  }

  @Override
  public String getString(int row, ChunkReaderContext context) {
    int chunkRowId = row % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(row, context);

    int rowOffset = chunkBuffer.getInt(chunkRowId * INT_SIZE);
    int nextRowOffset;

    if (chunkRowId == _numDocsPerChunk - 1) {
      // Last row in this trunk.
      nextRowOffset = chunkBuffer.limit();
    } else {
      nextRowOffset = chunkBuffer.getInt((chunkRowId + 1) * INT_SIZE);
      // For incomplete chunks, the next string's offset will be 0 as row offset for absent rows are 0.
      if (nextRowOffset == 0) {
        nextRowOffset = chunkBuffer.limit();
      }
    }

    int length = nextRowOffset - rowOffset;
    chunkBuffer.position(rowOffset);

    byte[] bytes = _reusableBytes.get();
    chunkBuffer.get(bytes, 0, length);
    return new String(bytes, 0, length, UTF_8);
  }

  @Override
  public ChunkReaderContext createContext() {
    return new ChunkReaderContext(_maxChunkSize);
  }
}
