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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.nio.ByteBuffer;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Chunk-based single-value raw (non-dictionary-encoded) forward index reader for values of fixed length data type (INT,
 * LONG, FLOAT, DOUBLE).
 * <p>For data layout, please refer to the documentation for {@link FixedByteChunkForwardIndexWriter}
 */
public final class FixedByteChunkSVForwardIndexReader extends BaseChunkForwardIndexReader {
  private final int _chunkSize;

  public FixedByteChunkSVForwardIndexReader(PinotDataBuffer dataBuffer, DataType valueType) {
    super(dataBuffer, valueType, true);
    _chunkSize = _numDocsPerChunk * _lengthOfLongestEntry;
  }

  @Nullable
  @Override
  public ChunkReaderContext createContext() {
    if (_isCompressed) {
      return new ChunkReaderContext(_chunkSize);
    } else {
      return null;
    }
  }

  @Override
  public int getInt(int docId, ChunkReaderContext context) {
    if (_isCompressed) {
      int chunkRowId = docId % _numDocsPerChunk;
      ByteBuffer chunkBuffer = getChunkBuffer(docId, context);
      return chunkBuffer.getInt(chunkRowId * Integer.BYTES);
    } else {
      return _rawData.getInt(docId * Integer.BYTES);
    }
  }

  @Override
  public long getLong(int docId, ChunkReaderContext context) {
    if (_isCompressed) {
      int chunkRowId = docId % _numDocsPerChunk;
      ByteBuffer chunkBuffer = getChunkBuffer(docId, context);
      return chunkBuffer.getLong(chunkRowId * Long.BYTES);
    } else {
      return _rawData.getLong(docId * Long.BYTES);
    }
  }

  @Override
  public float getFloat(int docId, ChunkReaderContext context) {
    if (_isCompressed) {
      int chunkRowId = docId % _numDocsPerChunk;
      ByteBuffer chunkBuffer = getChunkBuffer(docId, context);
      return chunkBuffer.getFloat(chunkRowId * Float.BYTES);
    } else {
      return _rawData.getFloat(docId * Float.BYTES);
    }
  }

  @Override
  public double getDouble(int docId, ChunkReaderContext context) {
    if (_isCompressed) {
      int chunkRowId = docId % _numDocsPerChunk;
      ByteBuffer chunkBuffer = getChunkBuffer(docId, context);
      return chunkBuffer.getDouble(chunkRowId * Double.BYTES);
    } else {
      return _rawData.getDouble(docId * Double.BYTES);
    }
  }

  @Override
  public boolean isBufferByteRangeInfoSupported() {
    return true;
  }

  @Override
  public void recordDocIdByteRanges(int docId, ChunkReaderContext context, @Nullable List<ByteRange> ranges) {
    if (!_isCompressed) {
      // If uncompressed, should use fixed offset
      throw new UnsupportedOperationException("Forward index is fixed length type");
    }
    recordDocIdRanges(docId, context, ranges);
  }

  @Override
  public boolean isFixedOffsetMappingType() {
    return !_isCompressed;
  }

  @Override
  public long getRawDataStartOffset() {
    if (isFixedOffsetMappingType()) {
      return _rawDataStart;
    }
    throw new UnsupportedOperationException("Forward index is not fixed length type");
  }

  @Override
  public int getDocLength() {
    if (isFixedOffsetMappingType()) {
      return _storedType.size();
    }
    throw new UnsupportedOperationException("Forward index is not fixed length type");
  }
}
