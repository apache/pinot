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
package org.apache.pinot.common.datablock;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * A {@link DataBlock} backed by an Apache Arrow {@link VectorSchemaRoot}.
 *
 * <p>Primitive types use their native Arrow vector types (e.g. {@code IntVector}, {@code BigIntVector}).
 * String columns are stored as plain {@code VarCharVector}.
 *
 * <p><b>Lifetime:</b> the block's off-heap buffers are owned by the {@code BufferAllocator} used to
 * build the underlying {@link VectorSchemaRoot}. Ownership is allocator-scoped — when that allocator
 * closes, every root (and block) it produced is freed atomically. {@link #close()} is provided for
 * explicit early disposal (e.g. in tests, or before the owning allocator closes); it is not
 * reference-counted and must be called at most once per block.
 *
 * <p>{@link #getStringDictionary()}, {@link #getFixedData()}, and {@link #getVarSizeData()} throw
 * {@link UnsupportedOperationException} — they're part of {@link DataBlock}'s legacy abstraction that assumes
 * a flat-byte-buffer layout and have no equivalent in Arrow's columnar vector layout.
 */
public class ArrowDataBlock implements DataBlock, AutoCloseable {
  private final VectorSchemaRoot _root;
  private final Map<Integer, String> _errCodeToExceptionMap;
  private final DataSchema _dataSchema;

  public ArrowDataBlock(VectorSchemaRoot root, DataSchema schema) {
    _root = root;
    _dataSchema = schema;
    _errCodeToExceptionMap = new HashMap<>();
  }

  public Schema getSchema() {
    return _root.getSchema();
  }

  public VectorSchemaRoot getRoot() {
    return _root;
  }

  // ----- DataBlock interface -----

  @Override
  public Map<String, String> getMetadata() {
    return Collections.emptyMap();
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  @Override
  public int getNumberOfRows() {
    return _root.getRowCount();
  }

  @Override
  public int getNumberOfColumns() {
    return _root.getSchema().getFields().size();
  }

  @Override
  public void addException(int errCode, String errMsg) {
    _errCodeToExceptionMap.put(errCode, errMsg);
  }

  @Override
  public Map<Integer, String> getExceptions() {
    return _errCodeToExceptionMap;
  }

  @Override
  public List<ByteBuffer> serialize()
      throws IOException {
    throw new UnsupportedOperationException("ArrowDataBlock does not support legacy serialization");
  }

  @Override
  public int getInt(int rowId, int colId) {
    FieldVector vector = _root.getVector(colId);
    if (vector instanceof IntVector) {
      return ((IntVector) vector).get(rowId);
    }
    if (vector instanceof BitVector) {
      return ((BitVector) vector).get(rowId);
    }
    throw new UnsupportedOperationException("Cannot read int from vector type: " + vector.getClass().getSimpleName());
  }

  @Override
  public long getLong(int rowId, int colId) {
    return ((BigIntVector) _root.getVector(colId)).get(rowId);
  }

  @Override
  public float getFloat(int rowId, int colId) {
    return ((Float4Vector) _root.getVector(colId)).get(rowId);
  }

  @Override
  public double getDouble(int rowId, int colId) {
    return ((Float8Vector) _root.getVector(colId)).get(rowId);
  }

  @Override
  public BigDecimal getBigDecimal(int rowId, int colId) {
    Object value = ((VarCharVector) _root.getVector(colId)).getObject(rowId);
    return value == null ? null : new BigDecimal(value.toString());
  }

  @Override
  public String getString(int rowId, int colId) {
    Object value = ((VarCharVector) _root.getVector(colId)).getObject(rowId);
    return value == null ? null : value.toString();
  }

  @Override
  public ByteArray getBytes(int rowId, int colId) {
    byte[] bytes = ((VarBinaryVector) _root.getVector(colId)).get(rowId);
    return bytes == null ? null : new ByteArray(bytes);
  }

  @Override
  public int[] getIntArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public long[] getLongArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public float[] getFloatArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public double[] getDoubleArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public String[] getStringArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public Map<String, Object> getMap(int rowId, int colId) {
    throw new UnsupportedOperationException("Map columns are not supported on ArrowDataBlock yet");
  }

  @Nullable
  @Override
  public CustomObject getCustomObject(int rowId, int colId) {
    throw new UnsupportedOperationException("CustomObject columns are not supported on ArrowDataBlock yet");
  }

  @Nullable
  @Override
  public RoaringBitmap getNullRowIds(int colId) {
    FieldVector vector = _root.getVector(colId);
    if (vector.getNullCount() == 0) {
      return null;
    }
    RoaringBitmap nullIds = new RoaringBitmap();
    int numRows = vector.getValueCount();
    for (int row = 0; row < numRows; row++) {
      if (vector.isNull(row)) {
        nullIds.add(row);
      }
    }
    return nullIds;
  }

  @Override
  public Type getDataBlockType() {
    return Type.ARROW;
  }

  @Nullable
  @Override
  public String[] getStringDictionary() {
    throw new UnsupportedOperationException("getStringDictionary is not supported on ArrowDataBlock");
  }

  @Nullable
  @Override
  public DataBuffer getFixedData() {
    throw new UnsupportedOperationException("getFixedData is not supported on ArrowDataBlock");
  }

  @Nullable
  @Override
  public DataBuffer getVarSizeData() {
    throw new UnsupportedOperationException("getVarSizeData is not supported on ArrowDataBlock");
  }

  @Nullable
  @Override
  public List<DataBuffer> getStatsByStage() {
    return List.of();
  }

  @Override
  public String toString() {
    return "{\"type\": \"arrow\", \"numRows\": " + getNumberOfRows() + ", \"numCols\": " + getNumberOfColumns() + "}";
  }

  @Override
  public void close() {
    _root.close();
  }
}
