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
package org.apache.pinot.core.io.reader.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.core.io.writer.impl.FixedByteChunkSVForwardIndexWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Reader class for data written out by {@link FixedByteChunkSVForwardIndexWriter}.
 * For data layout, please refer to the documentation for {@link FixedByteChunkSVForwardIndexWriter}
 *
 */
public class FixedByteChunkSVForwardIndexReader extends BaseChunkSVForwardIndexReader {
  private final DataType _valueType;

  /**
   * Constructor for the class.
   *
   * @param pinotDataBuffer Data buffer to read from
   * @throws IOException
   */
  public FixedByteChunkSVForwardIndexReader(PinotDataBuffer pinotDataBuffer, DataType valueType) {
    super(pinotDataBuffer);
    _valueType = valueType;
  }

  @Override
  public DataType getValueType() {
    return _valueType;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public int getInt(int docId) {
    if (!isCompressed()) {
      return getRawData().getInt(docId * Integer.BYTES);
    } else {
      throw new UnsupportedOperationException("Read without context not supported for compressed data.");
    }
  }

  @Override
  public int getInt(int docId, ChunkReaderContext context) {
    assert _lengthOfLongestEntry == Integer.BYTES;
    int chunkRowId = docId % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(docId, context);
    return chunkBuffer.getInt(chunkRowId * Integer.BYTES);
  }

  @Override
  public long getLong(int docId) {
    if (!isCompressed()) {
      return getRawData().getLong(docId * Long.BYTES);
    } else {
      throw new UnsupportedOperationException("Read without context not supported for compressed data.");
    }
  }

  @Override
  public long getLong(int docId, ChunkReaderContext context) {
    assert _lengthOfLongestEntry == Long.BYTES;
    int chunkRowId = docId % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(docId, context);
    return chunkBuffer.getLong(chunkRowId * Long.BYTES);
  }

  @Override
  public float getFloat(int docId) {
    if (!isCompressed()) {
      return getRawData().getFloat(docId * Float.BYTES);
    } else {
      throw new UnsupportedOperationException("Read without context not supported for compressed data.");
    }
  }

  @Override
  public float getFloat(int docId, ChunkReaderContext context) {
    assert _lengthOfLongestEntry == Float.BYTES;
    int chunkRowId = docId % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(docId, context);
    return chunkBuffer.getFloat(chunkRowId * Float.BYTES);
  }

  @Override
  public double getDouble(int docId) {
    if (!isCompressed()) {
      return getRawData().getDouble(docId * Double.BYTES);
    } else {
      throw new UnsupportedOperationException("Read without context not supported for compressed data.");
    }
  }

  @Override
  public double getDouble(int docId, ChunkReaderContext context) {
    assert _lengthOfLongestEntry == Double.BYTES;
    int chunkRowId = docId % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(docId, context);
    return chunkBuffer.getDouble(chunkRowId * Double.BYTES);
  }

  @Override
  public void readValues(int[] docIds, int length, int[] values, ChunkReaderContext context) {
    for (int i = 0; i < length; i++) {
      values[i] = getInt(docIds[i], context);
    }
  }

  @Override
  public ChunkReaderContext createContext() {
    return new ChunkReaderContext(_chunkSize);
  }
}
