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
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkSVForwardIndexWriter;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Chunk-based single-value raw (non-dictionary-encoded) forward index reader for values of fixed length data type (INT,
 * LONG, FLOAT, DOUBLE).
 * <p>For data layout, please refer to the documentation for {@link FixedByteChunkSVForwardIndexWriter}
 */
public final class FixedByteChunkSVForwardIndexReader extends BaseChunkSVForwardIndexReader {
  private final int _chunkSize;

  public FixedByteChunkSVForwardIndexReader(PinotDataBuffer dataBuffer, DataType valueType) {
    super(dataBuffer, valueType);
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
  public void readValuesSV(int[] docIds, int length, int[] values, ChunkReaderContext context) {
    if (getValueType().getStoredType().isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (getValueType().getStoredType()) {
        case INT: {
          int minOffset = docIds[0] * Integer.BYTES;
          IntBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Integer.BYTES).asIntBuffer();
          buffer.get(values, 0, length);
        }
        break;
        case LONG: {
          int minOffset = docIds[0] * Long.BYTES;
          LongBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Long.BYTES).asLongBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (int) buffer.get(i);
          }
        }
        break;
        case FLOAT: {
          int minOffset = docIds[0] * Float.BYTES;
          FloatBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Float.BYTES).asFloatBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (int) buffer.get(i);
          }
        }
        break;
        case DOUBLE: {
          int minOffset = docIds[0] * Double.BYTES;
          DoubleBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Double.BYTES).asDoubleBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (int) buffer.get(i);
          }
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      super.readValuesSV(docIds, length, values, context);
    }
  }

  @Override
  public void readValuesSV(int[] docIds, int length, long[] values, ChunkReaderContext context) {
    if (getValueType().getStoredType().isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (getValueType().getStoredType()) {
        case INT: {
          int minOffset = docIds[0] * Integer.BYTES;
          IntBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Integer.BYTES).asIntBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case LONG: {
          int minOffset = docIds[0] * Long.BYTES;
          LongBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Long.BYTES).asLongBuffer();
          buffer.get(values, 0, length);
        }
        break;
        case FLOAT: {
          int minOffset = docIds[0] * Float.BYTES;
          FloatBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Float.BYTES).asFloatBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (long) buffer.get(i);
          }
        }
        break;
        case DOUBLE: {
          int minOffset = docIds[0] * Double.BYTES;
          DoubleBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Double.BYTES).asDoubleBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (long) buffer.get(i);
          }
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      super.readValuesSV(docIds, length, values, context);
    }
  }

  @Override
  public void readValuesSV(int[] docIds, int length, float[] values, ChunkReaderContext context) {
    if (getValueType().getStoredType().isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (getValueType().getStoredType()) {
        case INT: {
          int minOffset = docIds[0] * Integer.BYTES;
          IntBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Integer.BYTES).asIntBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case LONG: {
          int minOffset = docIds[0] * Long.BYTES;
          LongBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Long.BYTES).asLongBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case FLOAT: {
          int minOffset = docIds[0] * Float.BYTES;
          FloatBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Float.BYTES).asFloatBuffer();
          buffer.get(values, 0, length);
        }
        break;
        case DOUBLE: {
          int minOffset = docIds[0] * Double.BYTES;
          DoubleBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Double.BYTES).asDoubleBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (float) buffer.get(i);
          }
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      super.readValuesSV(docIds, length, values, context);
    }
  }

  @Override
  public void readValuesSV(int[] docIds, int length, double[] values, ChunkReaderContext context) {
    if (getValueType().getStoredType().isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (getValueType().getStoredType()) {
        case INT: {
          int minOffset = docIds[0] * Integer.BYTES;
          IntBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Integer.BYTES).asIntBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case LONG: {
          int minOffset = docIds[0] * Long.BYTES;
          getLong(0, context);
          LongBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Long.BYTES).asLongBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case FLOAT: {
          int minOffset = docIds[0] * Float.BYTES;
          FloatBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Float.BYTES).asFloatBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case DOUBLE: {
          int minOffset = docIds[0] * Double.BYTES;
          DoubleBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Double.BYTES).asDoubleBuffer();
          buffer.get(values, 0, length);
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      super.readValuesSV(docIds, length, values, context);
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

  private boolean isContiguousRange(int[] docIds, int length) {
    return docIds[length - 1] - docIds[0] == length - 1;
  }
}
