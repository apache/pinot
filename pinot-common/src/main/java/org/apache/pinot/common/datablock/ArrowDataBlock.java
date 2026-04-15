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
import java.util.Set;
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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.arrow.ArrowSchemaConverter;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.memory.ArrowBuffers;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.MapUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * A {@link DataBlock} backed by an Apache Arrow {@link VectorSchemaRoot}.
 *
 * <p>All primitive types are stored in their native Arrow vector types (e.g. {@code IntVector}, {@code BigIntVector}).
 * String and JSON columns are stored as dictionary-encoded {@code IntVector} indices referencing a shared
 * {@code VarCharVector} dictionary held in the accompanying {@link DictionaryProvider}.
 *
 * <p>This class is {@link AutoCloseable}; callers must close it to release off-heap memory.
 */
public class ArrowDataBlock implements DataBlock, AutoCloseable {
  private final VectorSchemaRoot _root;
  private final Map<Integer, String> _errCodeToExceptionMap;
  @Nullable
  private DictionaryProvider _dictionaryProvider;
  @Nullable
  private DataSchema _dataSchema;

  public ArrowDataBlock(VectorSchemaRoot root) {
    _root = root;
    _errCodeToExceptionMap = new HashMap<>();
  }

  public ArrowDataBlock(VectorSchemaRoot root, @Nullable DictionaryProvider dictionaryProvider) {
    _root = root;
    _dictionaryProvider = dictionaryProvider;
    _errCodeToExceptionMap = new HashMap<>();
  }

  public Schema getSchema() {
    return _root.getSchema();
  }

  public VectorSchemaRoot getRoot() {
    return _root;
  }

  @Nullable
  public DictionaryProvider getDictionaryProvider() {
    return _dictionaryProvider;
  }

  /**
   * Copies a {@link DictionaryProvider} by transferring all dictionary vectors to new off-heap buffers.
   * This ensures the dictionaries remain valid after the source block is closed.
   */
  public static DictionaryProvider copyDictionaryProvider(@Nullable DictionaryProvider source) {
    DictionaryProvider.MapDictionaryProvider copy = new DictionaryProvider.MapDictionaryProvider();
    if (source == null) {
      return copy;
    }
    Set<Long> ids = source.getDictionaryIds();
    for (long id : ids) {
      Dictionary sourceDictionary = source.lookup(id);
      FieldVector sourceVector = sourceDictionary.getVector();
      TransferPair transferPair = sourceVector.getTransferPair(ArrowBuffers.getLocalAllocator());
      transferPair.splitAndTransfer(0, sourceVector.getValueCount());
      copy.put(new Dictionary((FieldVector) transferPair.getTo(), sourceDictionary.getEncoding()));
    }
    return copy;
  }

  // ----- DataBlock interface -----

  @Override
  public Map<String, String> getMetadata() {
    return Collections.emptyMap();
  }

  @Nullable
  @Override
  public DataSchema getDataSchema() {
    if (_dataSchema == null) {
      _dataSchema = ArrowSchemaConverter.toDataSchema(_root.getSchema());
    }
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
    return new BigDecimal(((VarCharVector) _root.getVector(colId)).getObject(rowId).toString());
  }

  @Override
  public String getString(int rowId, int colId) {
    FieldVector vector = _root.getVector(colId);
    DictionaryEncoding encoding = vector.getField().getFieldType().getDictionary();
    if (encoding != null && _dictionaryProvider != null) {
      VarCharVector dictVector = (VarCharVector) _dictionaryProvider.lookup(encoding.getId()).getVector();
      int dictIdx = ((IntVector) vector).get(rowId);
      return dictVector.getObject(dictIdx).toString();
    }
    return ((VarCharVector) vector).getObject(rowId).toString();
  }

  @Override
  public ByteArray getBytes(int rowId, int colId) {
    return new ByteArray(((VarBinaryVector) _root.getVector(colId)).get(rowId));
  }

  @Override
  public int[] getIntArray(int rowId, int colId) {
    ListVector listVector = (ListVector) _root.getVector(colId);
    int start = listVector.getOffsetBuffer().getInt(rowId * 4L);
    int end = listVector.getOffsetBuffer().getInt((rowId + 1) * 4L);
    int[] result = new int[end - start];
    IntVector child = (IntVector) listVector.getDataVector();
    for (int i = 0; i < result.length; i++) {
      result[i] = child.get(start + i);
    }
    return result;
  }

  @Override
  public long[] getLongArray(int rowId, int colId) {
    ListVector listVector = (ListVector) _root.getVector(colId);
    int start = listVector.getOffsetBuffer().getInt(rowId * 4L);
    int end = listVector.getOffsetBuffer().getInt((rowId + 1) * 4L);
    long[] result = new long[end - start];
    BigIntVector child = (BigIntVector) listVector.getDataVector();
    for (int i = 0; i < result.length; i++) {
      result[i] = child.get(start + i);
    }
    return result;
  }

  @Override
  public float[] getFloatArray(int rowId, int colId) {
    ListVector listVector = (ListVector) _root.getVector(colId);
    int start = listVector.getOffsetBuffer().getInt(rowId * 4L);
    int end = listVector.getOffsetBuffer().getInt((rowId + 1) * 4L);
    float[] result = new float[end - start];
    Float4Vector child = (Float4Vector) listVector.getDataVector();
    for (int i = 0; i < result.length; i++) {
      result[i] = child.get(start + i);
    }
    return result;
  }

  @Override
  public double[] getDoubleArray(int rowId, int colId) {
    ListVector listVector = (ListVector) _root.getVector(colId);
    int start = listVector.getOffsetBuffer().getInt(rowId * 4L);
    int end = listVector.getOffsetBuffer().getInt((rowId + 1) * 4L);
    double[] result = new double[end - start];
    Float8Vector child = (Float8Vector) listVector.getDataVector();
    for (int i = 0; i < result.length; i++) {
      result[i] = child.get(start + i);
    }
    return result;
  }

  @Override
  public String[] getStringArray(int rowId, int colId) {
    ListVector listVector = (ListVector) _root.getVector(colId);
    int start = listVector.getOffsetBuffer().getInt(rowId * 4L);
    int end = listVector.getOffsetBuffer().getInt((rowId + 1) * 4L);
    String[] result = new String[end - start];
    FieldVector child = listVector.getDataVector();
    DictionaryEncoding encoding = child.getField().getFieldType().getDictionary();
    if (encoding != null && _dictionaryProvider != null) {
      VarCharVector dictVector = (VarCharVector) _dictionaryProvider.lookup(encoding.getId()).getVector();
      IntVector childIdx = (IntVector) child;
      for (int i = 0; i < result.length; i++) {
        result[i] = dictVector.getObject(childIdx.get(start + i)).toString();
      }
    } else {
      VarCharVector childVarChar = (VarCharVector) child;
      for (int i = 0; i < result.length; i++) {
        result[i] = childVarChar.getObject(start + i).toString();
      }
    }
    return result;
  }

  @Override
  public Map<String, Object> getMap(int rowId, int colId) {
    byte[] bytes = ((VarBinaryVector) _root.getVector(colId)).get(rowId);
    return MapUtils.deserializeMap(bytes);
  }

  @Nullable
  @Override
  public CustomObject getCustomObject(int rowId, int colId) {
    byte[] bytes = ((VarBinaryVector) _root.getVector(colId)).get(rowId);
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    int type = ByteBuffer.wrap(bytes, 0, 4).getInt();
    if (bytes.length == 4) {
      // Only type, no payload — represents a null value
      assert type == CustomObject.NULL_TYPE_VALUE;
      return null;
    }
    return new CustomObject(type, ByteBuffer.wrap(bytes, 4, bytes.length - 4));
  }

  @Nullable
  @Override
  public RoaringBitmap getNullRowIds(int colId) {
    // Arrow vectors carry per-element null validity bits; null tracking is per-access
    return null;
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
    if (_dictionaryProvider instanceof DictionaryProvider.MapDictionaryProvider) {
      ((DictionaryProvider.MapDictionaryProvider) _dictionaryProvider).close();
    }
    _root.close();
  }
}
