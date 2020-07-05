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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.core.io.compression.ChunkCompressorFactory;
import org.apache.pinot.core.io.compression.ChunkDecompressor;
import org.apache.pinot.core.io.writer.impl.BaseChunkSVForwardIndexWriter;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReaderContext;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.util.CleanerUtil;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation for chunk-based single-value raw (non-dictionary-encoded) forward index reader.
 */
public abstract class BaseChunkSVForwardIndexReader implements ForwardIndexReader<BaseChunkSVForwardIndexReader.ChunkReaderContext> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseChunkSVForwardIndexReader.class);

  protected final int _chunkSize;
  protected final int _numDocsPerChunk;
  protected final int _numChunks;
  protected final int _lengthOfLongestEntry;
  protected final boolean _isCompressed;
  protected final PinotDataBuffer _rawData;

  private final PinotDataBuffer _dataBuffer;
  private final DataType _valueType;
  private final PinotDataBuffer _dataHeader;
  private final ChunkDecompressor _chunkDecompressor;
  private final int _headerEntryChunkOffsetSize;

  public BaseChunkSVForwardIndexReader(PinotDataBuffer dataBuffer, DataType valueType) {
    _dataBuffer = dataBuffer;
    _valueType = valueType;

    int headerOffset = 0;
    int version = _dataBuffer.getInt(headerOffset);
    headerOffset += Integer.BYTES;

    _numChunks = _dataBuffer.getInt(headerOffset);
    headerOffset += Integer.BYTES;

    _numDocsPerChunk = _dataBuffer.getInt(headerOffset);
    headerOffset += Integer.BYTES;

    _lengthOfLongestEntry = _dataBuffer.getInt(headerOffset);
    if (valueType.isFixedWidth()) {
      Preconditions.checkState(_lengthOfLongestEntry == valueType.size());
    }
    headerOffset += Integer.BYTES;

    int dataHeaderStart = headerOffset;
    if (version > 1) {
      _dataBuffer.getInt(headerOffset); // Total docs
      headerOffset += Integer.BYTES;

      ChunkCompressorFactory.CompressionType compressionType =
          ChunkCompressorFactory.CompressionType.values()[_dataBuffer.getInt(headerOffset)];
      _chunkDecompressor = ChunkCompressorFactory.getDecompressor(compressionType);
      _isCompressed = !compressionType.equals(ChunkCompressorFactory.CompressionType.PASS_THROUGH);

      headerOffset += Integer.BYTES;
      dataHeaderStart = _dataBuffer.getInt(headerOffset);
    } else {
      _isCompressed = true;
      _chunkDecompressor = ChunkCompressorFactory.getDecompressor(ChunkCompressorFactory.CompressionType.SNAPPY);
    }

    _chunkSize = (_lengthOfLongestEntry * _numDocsPerChunk);
    _headerEntryChunkOffsetSize = BaseChunkSVForwardIndexWriter.getHeaderEntryChunkOffsetSize(version);

    // Slice out the header from the data buffer.
    int dataHeaderLength = _numChunks * _headerEntryChunkOffsetSize;
    int rawDataStart = dataHeaderStart + dataHeaderLength;
    _dataHeader = _dataBuffer.view(dataHeaderStart, rawDataStart);

    // Useful for uncompressed data.
    _rawData = _dataBuffer.view(rawDataStart, _dataBuffer.size());
  }

  /**
   * Helper method to return the chunk buffer that contains the value at the given document id.
   * <ul>
   *   <li> If the chunk already exists in the reader context, returns the same. </li>
   *   <li> Otherwise, loads the chunk for the row, and sets it in the reader context. </li>
   * </ul>
   * @param docId Document id
   * @param context Reader context
   * @return Chunk for the row
   */
  protected ByteBuffer getChunkBuffer(int docId, ChunkReaderContext context) {
    int chunkId = docId / _numDocsPerChunk;
    if (context.getChunkId() == chunkId) {
      return context.getChunkBuffer();
    }

    int chunkSize;
    long chunkPosition = getChunkPosition(chunkId);

    // Size of chunk can be determined using next chunks offset, or end of data buffer for last chunk.
    if (chunkId == (_numChunks - 1)) { // Last chunk.
      chunkSize = (int) (_dataBuffer.size() - chunkPosition);
    } else {
      long nextChunkOffset = getChunkPosition(chunkId + 1);
      chunkSize = (int) (nextChunkOffset - chunkPosition);
    }

    ByteBuffer decompressedBuffer = context.getChunkBuffer();
    decompressedBuffer.clear();

    try {
      _chunkDecompressor.decompress(_dataBuffer.toDirectByteBuffer(chunkPosition, chunkSize), decompressedBuffer);
    } catch (IOException e) {
      LOGGER.error("Exception caught while decompressing data chunk", e);
      throw new RuntimeException(e);
    }
    context.setChunkId(chunkId);
    return decompressedBuffer;
  }

  /**
   * Helper method to get the offset of the chunk in the data.
   * @param chunkId Id of the chunk for which to return the position.
   * @return Position (offset) of the chunk in the data.
   */
  private long getChunkPosition(int chunkId) {
    if (_headerEntryChunkOffsetSize == Integer.BYTES) {
      return _dataHeader.getInt(chunkId * _headerEntryChunkOffsetSize);
    } else {
      return _dataHeader.getLong(chunkId * _headerEntryChunkOffsetSize);
    }
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getValueType() {
    return _valueType;
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }

  /**
   * Context for the chunk-based forward index readers.
   * <p>Information saved in the context can be used by subsequent reads as cache:
   * <ul>
   *   <li>
   *     Chunk Buffer from the previous read. Useful if the subsequent read is from the same buffer, as it avoids extra
   *     chunk decompression.
   *   </li>
   *   <li>Id for the chunk</li>
   * </ul>
   */
  public static class ChunkReaderContext implements ForwardIndexReaderContext {
    private final ByteBuffer _chunkBuffer;
    private int _chunkId;

    public ChunkReaderContext(int maxChunkSize) {
      _chunkBuffer = ByteBuffer.allocateDirect(maxChunkSize);
      _chunkId = -1;
    }

    public ByteBuffer getChunkBuffer() {
      return _chunkBuffer;
    }

    public int getChunkId() {
      return _chunkId;
    }

    public void setChunkId(int chunkId) {
      _chunkId = chunkId;
    }

    @Override
    public void close()
        throws IOException {
      if (CleanerUtil.UNMAP_SUPPORTED) {
        CleanerUtil.getCleaner().freeBuffer(_chunkBuffer);
      }
    }
  }
}
