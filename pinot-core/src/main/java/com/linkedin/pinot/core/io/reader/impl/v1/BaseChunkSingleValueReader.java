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
import com.linkedin.pinot.core.io.reader.impl.ChunkReaderContext;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class implementation for {@link BaseSingleColumnSingleValueReader}.
 * Base class for the fixed and variable byte reader implementations.
 *
 */
public abstract class BaseChunkSingleValueReader extends BaseSingleColumnSingleValueReader<ChunkReaderContext> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseChunkSingleValueReader.class);

  protected static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
  protected static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
  protected static final int FLOAT_SIZE = Float.SIZE / Byte.SIZE;
  protected static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;

  protected final PinotDataBuffer _dataBuffer;
  protected final PinotDataBuffer _header;
  protected final ChunkDecompressor _chunkDecompressor;
  protected final int _chunkSize;

  protected final int _numDocsPerChunk;
  protected final int _numChunks;
  protected final int _lengthOfLongestEntry;

  /**
   * Constructor for the class.
   *
   * @param pinotDataBuffer Data buffer
   * @param decompressor Data decompressor
   */
  public BaseChunkSingleValueReader(PinotDataBuffer pinotDataBuffer, ChunkDecompressor decompressor) {
    _chunkDecompressor = decompressor;
    _dataBuffer = pinotDataBuffer;

    int headerOffset = INT_SIZE; // First entry is the version, which is unused currently.
    _numChunks = _dataBuffer.getInt(headerOffset);
    headerOffset += INT_SIZE;

    _numDocsPerChunk = _dataBuffer.getInt(headerOffset);
    headerOffset += INT_SIZE;

    _lengthOfLongestEntry = _dataBuffer.getInt(headerOffset);
    headerOffset += INT_SIZE;
    _chunkSize = (_lengthOfLongestEntry * _numDocsPerChunk);

    // Slice out the header from the data buffer.
    int headerLength = _numChunks * INT_SIZE;
    _header = _dataBuffer.view(headerOffset, headerOffset + headerLength);
  }

  @Override
  public void close() {
    // Nothing to close here.
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
  protected ByteBuffer getChunkForRow(int row, ChunkReaderContext context) {
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

    try {
      _chunkDecompressor.decompress(_dataBuffer.toDirectByteBuffer(chunkPosition, chunkSize), uncompressedBuffer);
    } catch (IOException e) {
      LOGGER.error("Exception caught while decompressing data chunk", e);
      throw new RuntimeException(e);
    }
    context.setChunkId(chunkId);
    return uncompressedBuffer;
  }

  /**
   * Helper method to get the offset of the chunk in the data.
   *
   * @param chunkId Id of the chunk for which to return the position.
   * @return Position (offset) of the chunk in the data.
   */
  protected int getChunkPosition(int chunkId) {
    return _header.getInt(chunkId * INT_SIZE);
  }
}
