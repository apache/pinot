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
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
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
   *
   * <p>Prefer {@link #shareDictionaryProvider} when the source outlives the consumer.
   */
  public static DictionaryProvider copyDictionaryProvider(@Nullable DictionaryProvider source,
      BufferAllocator allocator) {
    DictionaryProvider.MapDictionaryProvider copy = new DictionaryProvider.MapDictionaryProvider();
    if (source == null) {
      return copy;
    }
    Set<Long> ids = source.getDictionaryIds();
    for (long id : ids) {
      Dictionary sourceDictionary = source.lookup(id);
      FieldVector sourceVector = sourceDictionary.getVector();
      TransferPair transferPair = sourceVector.getTransferPair(allocator);
      transferPair.splitAndTransfer(0, sourceVector.getValueCount());
      copy.put(new Dictionary((FieldVector) transferPair.getTo(), sourceDictionary.getEncoding()));
    }
    return copy;
  }

  /**
   * Returns a reference-counted wrapper around the given {@link DictionaryProvider}. Multiple result blocks can
   * share the same underlying dictionary without copying. The dictionary vectors are freed only when the last
   * reference is released via {@link SharedDictionaryProvider#release()}.
   *
   * <p>Returns {@code null} if the source is null or has no dictionaries.
   */
  @Nullable
  public static SharedDictionaryProvider shareDictionaryProvider(@Nullable DictionaryProvider source) {
    if (source == null || source.getDictionaryIds().isEmpty()) {
      return null;
    }
    return new SharedDictionaryProvider(source);
  }

  /**
   * A reference-counted wrapper around a {@link DictionaryProvider}. Call {@link #retain()} before sharing
   * and {@link #release()} when done. The underlying dictionary vectors are closed when the count reaches zero.
   */
  public static class SharedDictionaryProvider implements DictionaryProvider {
    private final DictionaryProvider _delegate;
    private final AtomicInteger _refCount = new AtomicInteger(1);

    SharedDictionaryProvider(DictionaryProvider delegate) {
      _delegate = delegate;
    }

    @Override
    public Dictionary lookup(long id) {
      return _delegate.lookup(id);
    }

    @Override
    public Set<Long> getDictionaryIds() {
      return _delegate.getDictionaryIds();
    }

    /** Increments the reference count. Must be paired with a subsequent {@link #release()}. */
    public void retain() {
      while (true) {
        int count = _refCount.get();
        if (count <= 0) {
          throw new IllegalStateException("SharedDictionaryProvider already released");
        }
        if (_refCount.compareAndSet(count, count + 1)) {
          return;
        }
      }
    }

    /** Decrements the reference count, closing the underlying dictionaries when it reaches zero. */
    public void release() {
      if (_refCount.decrementAndGet() == 0) {
        if (_delegate instanceof DictionaryProvider.MapDictionaryProvider) {
          ((DictionaryProvider.MapDictionaryProvider) _delegate).close();
        }
      }
    }
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
      List<Field> fields = _root.getSchema().getFields();
      String[] names = new String[fields.size()];
      ColumnDataType[] types = new ColumnDataType[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        names[i] = fields.get(i).getName();
        types[i] = arrowTypeToPinot(fields.get(i).getType());
      }
      _dataSchema = new DataSchema(names, types);
    }
    return _dataSchema;
  }

  private static ColumnDataType arrowTypeToPinot(ArrowType type) {
    if (type instanceof ArrowType.Bool) {
      return ColumnDataType.BOOLEAN;
    } else if (type instanceof ArrowType.Int) {
      return ((ArrowType.Int) type).getBitWidth() == 32 ? ColumnDataType.INT : ColumnDataType.LONG;
    } else if (type instanceof ArrowType.FloatingPoint) {
      return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.SINGLE
          ? ColumnDataType.FLOAT : ColumnDataType.DOUBLE;
    } else if (type instanceof ArrowType.Utf8) {
      return ColumnDataType.STRING;
    } else if (type instanceof ArrowType.Binary) {
      return ColumnDataType.BYTES;
    } else if (type instanceof ArrowType.Null) {
      return ColumnDataType.UNKNOWN;
    }
    return ColumnDataType.STRING;
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
    ListVector lv = (ListVector) _root.getVector(colId);
    int start = lv.getOffsetBuffer().getInt(rowId * 4L);
    int len = lv.getOffsetBuffer().getInt((rowId + 1) * 4L) - start;
    int[] result = new int[len];
    IntVector child = (IntVector) lv.getDataVector();
    for (int i = 0; i < len; i++) {
      result[i] = child.get(start + i);
    }
    return result;
  }

  @Override
  public long[] getLongArray(int rowId, int colId) {
    ListVector lv = (ListVector) _root.getVector(colId);
    int start = lv.getOffsetBuffer().getInt(rowId * 4L);
    int len = lv.getOffsetBuffer().getInt((rowId + 1) * 4L) - start;
    long[] result = new long[len];
    BigIntVector child = (BigIntVector) lv.getDataVector();
    for (int i = 0; i < len; i++) {
      result[i] = child.get(start + i);
    }
    return result;
  }

  @Override
  public float[] getFloatArray(int rowId, int colId) {
    ListVector lv = (ListVector) _root.getVector(colId);
    int start = lv.getOffsetBuffer().getInt(rowId * 4L);
    int len = lv.getOffsetBuffer().getInt((rowId + 1) * 4L) - start;
    float[] result = new float[len];
    Float4Vector child = (Float4Vector) lv.getDataVector();
    for (int i = 0; i < len; i++) {
      result[i] = child.get(start + i);
    }
    return result;
  }

  @Override
  public double[] getDoubleArray(int rowId, int colId) {
    ListVector lv = (ListVector) _root.getVector(colId);
    int start = lv.getOffsetBuffer().getInt(rowId * 4L);
    int len = lv.getOffsetBuffer().getInt((rowId + 1) * 4L) - start;
    double[] result = new double[len];
    Float8Vector child = (Float8Vector) lv.getDataVector();
    for (int i = 0; i < len; i++) {
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
