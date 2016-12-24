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

import com.linkedin.pinot.core.io.compression.ChunkDecompressor;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.VarByteReaderContext;
import com.linkedin.pinot.core.io.writer.SingleColumnSingleValueWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;


/**
 * Reader class for data written out by {@link SingleColumnSingleValueWriter}.
 * For data layout, please refer to the documentation for {@link SingleColumnSingleValueWriter}
 */
public class VarByteSingleValueReader extends BaseSingleColumnSingleValueReader<VarByteReaderContext> {
  private static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private final PinotDataBuffer _dataBuffer;
  final PinotDataBuffer _header;
  private final ChunkDecompressor _chunkDecompressor;
  private final int _maxChunkSize;

  private final int _numDocsPerChunk;
  private final int _numChunks;
  private final int _lengthOfLongestEntry;

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
   * @param uncompressor Chunk uncompressor
   * @throws IOException
   */
  public VarByteSingleValueReader(PinotDataBuffer pinotDataBuffer, ChunkDecompressor uncompressor)
      throws IOException {
    _chunkDecompressor = uncompressor;
    _dataBuffer = pinotDataBuffer;

    int headerOffset = INT_SIZE; // First entry is the version, which is unused currently.
    _numChunks = _dataBuffer.getInt(headerOffset);
    headerOffset += INT_SIZE;

    _numDocsPerChunk = _dataBuffer.getInt(headerOffset);
    headerOffset += INT_SIZE;

    _lengthOfLongestEntry = _dataBuffer.getInt(headerOffset);
    headerOffset += INT_SIZE;

    int chunkHeaderSize = _numDocsPerChunk * INT_SIZE;
    _maxChunkSize = chunkHeaderSize + (_lengthOfLongestEntry * _numDocsPerChunk);

    // Slice out the header from the data buffer.
    int headerLength = _numChunks * INT_SIZE;
    _header = _dataBuffer.view(headerOffset, headerOffset + headerLength);
  }

  @Override
  public String getString(int row, VarByteReaderContext context)
      throws IOException {
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
    ByteBuffer byteBuffer = chunkBuffer.duplicate();
    byteBuffer.position(rowOffset);

    byte[] bytes = _reusableBytes.get();
    byteBuffer.get(bytes, 0, length);
    return new String(bytes, 0, length, UTF_8);
  }

  @Override
  public void close() {
    // Nothing to close here.
  }

  @Override
  public VarByteReaderContext createContext() {
    return new VarByteReaderContext(_maxChunkSize);
  }

  /**
   * Helper method to get the chunk for a given row.
   * <ul>
   *   <li> If the chunk already exists in the reader context, returns the same. </li>
   *   <li> Otherwise, loads the chunk for the row, and sets it in the reader context. </li>
   * </ul>
   * @param row Row for which to get the chunk
   * @param context Reader context
   * @return Chunk for the row
   */
  private ByteBuffer getChunkForRow(int row, VarByteReaderContext context)
      throws IOException {
    int chunkId = row / _numDocsPerChunk;
    if (context.getChunkId() == chunkId) {
      return context.getChunkBuffer();
    }

    int chunkSize;
    int chunkPosition = getChunkPosition(chunkId);

    // Size of chunk can be determined using next chunks offset, or end of data buffer for last chunk.
    if (chunkId == (_numChunks - 1)) { // Last chunk.
      chunkSize = (int) (_dataBuffer.size() - chunkPosition);
    } else {
      int nextChunkOffset = getChunkPosition(chunkId + 1);
      chunkSize = nextChunkOffset - chunkPosition;
    }

    ByteBuffer uncompressedBuffer = context.getChunkBuffer();
    uncompressedBuffer.clear();

    _chunkDecompressor.decompress(_dataBuffer.toDirectByteBuffer(chunkPosition, chunkSize), uncompressedBuffer);
    context.setChunkId(chunkId);
    return uncompressedBuffer;
  }

  /**
   * Helper method to get the offset of the chunk in the data.
   *
   * @param chunkId Id of the chunk for which to return the position.
   * @return Position (offset) of the chunk in the data.
   */
  private int getChunkPosition(int chunkId) {
    return _header.getInt(chunkId * INT_SIZE);
  }
}
