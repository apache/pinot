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
package org.apache.pinot.segment.local.segment.index.openstruct;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.segment.index.datasource.BaseDataSource;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.PinotDataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Virtual per-key DataSource for a sparse OPEN_STRUCT key. Parses the blob per doc via
/// the shared [OpenStructSparseBlobReader], coerces to the resolved stored type. No dictionary.
/// Null vector built lazily (one blob scan, memoized).
///
/// Single- or multi-value follows `resolvedChildSpec`, which is what the key reads as on the dense side too:
/// a declared child spec, or -- for an undeclared key -- the shape the segment recorded for it.
/// `maxNumValuesPerMVEntry` is the longest value the key holds, the number callers size their multi-value
/// buffers from. A single-value key reports 0. Pass -1 for a multi-value key without recorded lengths to compute
/// its maximum on the first call to [DataSourceMetadata#getMaxNumValuesPerMVEntry()].
///
/// A numeric, BOOLEAN or TIMESTAMP value uses the dense tier's build-time conversion
/// ([PinotDataType] of the stored type in `OpenStructColumnSplitter`) on the raw JSON scalar.
/// A value that conversion rejects -- text that is not a number, an object in a numeric key -- is dropped by the dense
/// tier, so here it reads as absent (null, or the default with null handling off) and the null vector agrees. A
/// multi-value key is absent when any element is rejected, as the dense tier drops the whole value. STRING, JSON and
/// BYTES keys accept any non-null element.
public class SparseKeyDataSource extends BaseDataSource {
  static final int UNKNOWN_MAX_NUM_VALUES = -1;
  private final FieldSpec _fieldSpec;

  public SparseKeyDataSource(FieldSpec resolvedChildSpec, OpenStructSparseBlobReader blobReader,
      int maxNumValuesPerMVEntry) {
    this(resolvedChildSpec, blobReader, maxNumValuesPerMVEntry,
        new SparseKeyForwardIndexReader(resolvedChildSpec, blobReader));
  }

  private SparseKeyDataSource(FieldSpec fieldSpec, OpenStructSparseBlobReader blobReader, int maxNumValuesPerMVEntry,
      SparseKeyForwardIndexReader forwardReader) {
    super(new SparseKeyMetadata(fieldSpec, blobReader.getNumDocs(), maxNumValuesPerMVEntry, forwardReader),
        new ColumnIndexContainer.FromMap(Map.of(StandardIndexes.forward(), forwardReader,
            StandardIndexes.nullValueVector(), new LazyPresenceNullVector(fieldSpec, blobReader))));
    _fieldSpec = fieldSpec;
  }

  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  static class SparseKeyForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
    private static final NumberWriter<int[]> INT_WRITER = (buffer, i, value) -> buffer[i] = value.intValue();
    private static final NumberWriter<long[]> LONG_WRITER = (buffer, i, value) -> buffer[i] = value.longValue();
    private static final NumberWriter<float[]> FLOAT_WRITER = (buffer, i, value) -> buffer[i] = value.floatValue();
    private static final NumberWriter<double[]> DOUBLE_WRITER = (buffer, i, value) -> buffer[i] = value.doubleValue();
    private static final NumberWriter<BigDecimal[]> BIG_DECIMAL_WRITER =
        (buffer, i, value) -> buffer[i] = (BigDecimal) value;

    private final String _key;
    private final DataType _storedType;
    private final boolean _singleValue;
    private final OpenStructSparseBlobReader _blob;
    /// The declared default for this key, read once. A document without the key reads as this, and
    /// [org.apache.pinot.core.operator.filter.MapFilterOperator] already refuses the JSON-index fast path when a
    /// predicate names it -- so the two must agree, or a NOT_IN over a declared default can take the fast path and
    /// miss the documents that lack the key.
    private final Object _declaredDefault;
    /// The dense tier's conversion for this key; null for a STRING, JSON or BYTES key, which reads any value.
    @Nullable
    private final PinotDataType _converter;

    SparseKeyForwardIndexReader(FieldSpec fieldSpec, OpenStructSparseBlobReader blob) {
      this(fieldSpec.getName(), fieldSpec.getDataType(), fieldSpec.isSingleValueField(), blob,
          fieldSpec.getDefaultNullValue());
    }

    SparseKeyForwardIndexReader(String key, DataType dataType, OpenStructSparseBlobReader blob) {
      this(key, dataType, true, blob, null);
    }

    private SparseKeyForwardIndexReader(String key, DataType dataType, boolean singleValue,
        OpenStructSparseBlobReader blob, @Nullable Object declaredDefault) {
      _declaredDefault = declaredDefault;
      _key = key;
      _converter = converterFor(dataType);
      _storedType = dataType.getStoredType();
      _singleValue = singleValue;
      _blob = blob;
    }

    @Override
    public boolean isDictionaryEncoded() {
      return false;
    }

    @Override
    public boolean isSingleValue() {
      return _singleValue;
    }

    @Override
    public DataType getStoredType() {
      return _storedType;
    }

    @Override
    public ForwardIndexReaderContext createContext() {
      return _blob.createBlobContext();
    }

    @Nullable
    private JsonNode valueNode(int docId, ForwardIndexReaderContext context) {
      JsonNode node = _blob.getValue(docId, _key, context, _converter);
      return node == null || node.isNull() ? null : node;
    }

    /// The single value converted to the key's stored type, or null when the document lacks the key or the dense
    /// tier's conversion rejects the value. Callers read it through the getter of the key's stored type, as for any
    /// forward index, so the numeric getters only see a converted number.
    @Nullable
    private Object converted(int docId, ForwardIndexReaderContext context) {
      JsonNode node = valueNode(docId, context);
      return node == null ? null : convert(_converter, node);
    }

    private <T> T orDefault(int docId, ForwardIndexReaderContext context, Function<JsonNode, T> map, T defaultValue) {
      JsonNode node = valueNode(docId, context);
      return node == null ? defaultValue : map.apply(node);
    }

    /// The declared default when the key has one, else the standard type default. This is what a key absent from the
    /// segment entirely already reads as, through `OpenStructDataSource.getValueFieldSpec`, so a key absent from one
    /// document should not read as something else.
    private <T> T declaredOr(Class<T> type, T typeDefault) {
      return type.isInstance(_declaredDefault) ? type.cast(_declaredDefault) : typeDefault;
    }

    @Override
    public int getInt(int docId, ForwardIndexReaderContext context) {
      Object value = converted(docId, context);
      return value != null
          ? ((Number) value).intValue()
          : declaredOr(Integer.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    }

    @Override
    public long getLong(int docId, ForwardIndexReaderContext context) {
      Object value = converted(docId, context);
      return value != null
          ? ((Number) value).longValue()
          : declaredOr(Long.class, (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    }

    @Override
    public float getFloat(int docId, ForwardIndexReaderContext context) {
      Object value = converted(docId, context);
      return value != null
          ? ((Number) value).floatValue()
          : declaredOr(Float.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
    }

    @Override
    public double getDouble(int docId, ForwardIndexReaderContext context) {
      Object value = converted(docId, context);
      return value != null
          ? ((Number) value).doubleValue()
          : declaredOr(Double.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
    }

    @Override
    public BigDecimal getBigDecimal(int docId, ForwardIndexReaderContext context) {
      Object value = converted(docId, context);
      return value != null
          ? (BigDecimal) value
          : declaredOr(BigDecimal.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL);
    }

    @Override
    public String getString(int docId, ForwardIndexReaderContext context) {
      // asText() is defined as the empty string for an object or array node, so a key whose value is a nested
      // document read back as "" and was indistinguishable from a missing key. Serialize container nodes instead,
      // which is what a caller asking a JSON blob for a value expects and what the JSON functions can consume.
      return orDefault(docId, context,
          node -> node.isContainerNode() ? node.toString() : node.asText(),
          declaredOr(String.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING));
    }

    @Override
    public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
      // Non-binary nodes yield null, and TextNode base64-decoding throws on malformed input; both fold to the
      // type default, matching the other getters.
      byte[] absent = declaredOr(byte[].class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
      return orDefault(docId, context, node -> {
        try {
          byte[] bytes = node.binaryValue();
          return bytes == null ? absent : bytes;
        } catch (IOException e) {
          return absent;
        }
      }, absent);
    }

    @FunctionalInterface
    private interface NumberWriter<T> {
      void set(T buffer, int index, Number value);
    }

    /// Convert directly into the caller's array. An absent key, an empty array, or one rejected element
    /// makes the whole row a singleton containing the declared default.
    private <T> int fillNumbers(@Nullable JsonNode node, T buffer, int capacity, NumberWriter<T> writer,
        Number defaultValue) {
      int count = elementCount(node);
      if (count > capacity && !keeps(_converter, false, node)) {
        // The buffer need only fit the logical value, which can be a singleton even for a long malformed array.
        count = 0;
      }
      for (int i = 0; i < count; i++) {
        Number value = (Number) convert(_converter, elementAt(node, i));
        if (value == null) {
          writer.set(buffer, 0, defaultValue);
          return 1;
        }
        writer.set(buffer, i, value);
      }
      if (count == 0) {
        writer.set(buffer, 0, defaultValue);
        return 1;
      }
      return count;
    }

    private <T> T readNumbers(@Nullable JsonNode node, IntFunction<T> allocate, NumberWriter<T> writer,
        Number defaultValue) {
      int capacity = Math.max(1, elementCount(node));
      T values = allocate.apply(capacity);
      if (fillNumbers(node, values, capacity, writer, defaultValue) == capacity) {
        return values;
      }
      T singleton = allocate.apply(1);
      writer.set(singleton, 0, defaultValue);
      return singleton;
    }

    @Override
    public int getNumValuesMV(int docId, ForwardIndexReaderContext context) {
      JsonNode node = valueNode(docId, context);
      return node != null && keeps(_converter, false, node) ? elementCount(node) : 1;
    }

    @Override
    public int[] getIntMV(int docId, ForwardIndexReaderContext context) {
      return readNumbers(valueNode(docId, context), int[]::new, INT_WRITER,
          declaredOr(Integer.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT));
    }

    @Override
    public int getIntMV(int docId, int[] valueBuffer, ForwardIndexReaderContext context) {
      return fillNumbers(valueNode(docId, context), valueBuffer, valueBuffer.length, INT_WRITER,
          declaredOr(Integer.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT));
    }

    @Override
    public long[] getLongMV(int docId, ForwardIndexReaderContext context) {
      return readNumbers(valueNode(docId, context), long[]::new, LONG_WRITER,
          declaredOr(Long.class, (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG));
    }

    @Override
    public int getLongMV(int docId, long[] valueBuffer, ForwardIndexReaderContext context) {
      return fillNumbers(valueNode(docId, context), valueBuffer, valueBuffer.length, LONG_WRITER,
          declaredOr(Long.class, (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG));
    }

    @Override
    public float[] getFloatMV(int docId, ForwardIndexReaderContext context) {
      return readNumbers(valueNode(docId, context), float[]::new, FLOAT_WRITER,
          declaredOr(Float.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT));
    }

    @Override
    public int getFloatMV(int docId, float[] valueBuffer, ForwardIndexReaderContext context) {
      return fillNumbers(valueNode(docId, context), valueBuffer, valueBuffer.length, FLOAT_WRITER,
          declaredOr(Float.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT));
    }

    @Override
    public double[] getDoubleMV(int docId, ForwardIndexReaderContext context) {
      return readNumbers(valueNode(docId, context), double[]::new, DOUBLE_WRITER,
          declaredOr(Double.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE));
    }

    @Override
    public int getDoubleMV(int docId, double[] valueBuffer, ForwardIndexReaderContext context) {
      return fillNumbers(valueNode(docId, context), valueBuffer, valueBuffer.length, DOUBLE_WRITER,
          declaredOr(Double.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE));
    }

    @Override
    public BigDecimal[] getBigDecimalMV(int docId, ForwardIndexReaderContext context) {
      return readNumbers(valueNode(docId, context), BigDecimal[]::new, BIG_DECIMAL_WRITER,
          declaredOr(BigDecimal.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL));
    }

    @Override
    public int getBigDecimalMV(int docId, BigDecimal[] valueBuffer, ForwardIndexReaderContext context) {
      return fillNumbers(valueNode(docId, context), valueBuffer, valueBuffer.length, BIG_DECIMAL_WRITER,
          declaredOr(BigDecimal.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL));
    }

    @Override
    public String[] getStringMV(int docId, ForwardIndexReaderContext context) {
      JsonNode node = valueNode(docId, context);
      if (node == null || !keeps(null, false, node)) {
        return new String[]{declaredOr(String.class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING)};
      }
      int count = elementCount(node);
      String[] values = new String[count];
      for (int i = 0; i < count; i++) {
        JsonNode element = elementAt(node, i);
        // Serialized for the same reason as the single-value getter: asText() is the empty string for a nested
        // element, which would be indistinguishable from an element that is genuinely "".
        values[i] = element.isContainerNode() ? element.toString() : element.asText();
      }
      return values;
    }

    @Override
    public int getStringMV(int docId, String[] valueBuffer, ForwardIndexReaderContext context) {
      String[] values = getStringMV(docId, context);
      System.arraycopy(values, 0, valueBuffer, 0, values.length);
      return values.length;
    }

    @Override
    public byte[][] getBytesMV(int docId, ForwardIndexReaderContext context) {
      JsonNode node = valueNode(docId, context);
      if (node == null || !keeps(null, false, node)) {
        return new byte[][]{declaredOr(byte[].class, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES)};
      }
      int count = elementCount(node);
      byte[][] values = new byte[count][];
      for (int i = 0; i < count; i++) {
        values[i] = binaryValue(elementAt(node, i));
      }
      return values;
    }

    @Override
    public int getBytesMV(int docId, byte[][] valueBuffer, ForwardIndexReaderContext context) {
      byte[][] values = getBytesMV(docId, context);
      System.arraycopy(values, 0, valueBuffer, 0, values.length);
      return values.length;
    }

    /// Bytes of one node, folding a non-binary or malformed value to the type default the same way the
    /// single-value getter does.
    private static byte[] binaryValue(JsonNode node) {
      try {
        byte[] bytes = node.binaryValue();
        return bytes == null ? FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES : bytes;
      } catch (IOException e) {
        return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES;
      }
    }

    @Override
    public void close() {
    }
  }

  /// The dense tier's conversion for a key of `dataType`: [PinotDataType] of its stored type, as
  /// `OpenStructColumnSplitter` applies at build. Null for STRING, JSON and BYTES, which read any value as is.
  @Nullable
  static PinotDataType converterFor(DataType dataType) {
    switch (dataType.getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
        return ColumnDataType.fromDataTypeSV(dataType.getStoredType()).toPinotDataType();
      default:
        return null;
    }
  }

  /// `node` converted by `converter`, or null when the conversion rejects it. With no converter the node itself is
  /// returned. Numeric keys cannot represent containers or nulls.
  @Nullable
  static Object convert(@Nullable PinotDataType converter, JsonNode node) {
    if (converter == null) {
      return node;
    }
    Object raw;
    if (node.isNumber()) {
      // BigInteger is classified as OBJECT by PinotDataType and would round through double.
      if (converter == PinotDataType.BIG_DECIMAL) {
        // The numeric parser keeps a DoubleNode only when this token exceeded BigDecimal's scale range.
        return node.isDouble() ? null : node.decimalValue();
      }
      raw = node.numberValue();
    } else if (node.isBoolean()) {
      raw = node.booleanValue();
    } else if (node.isTextual()) {
      raw = node.textValue();
    } else {
      return null;
    }
    try {
      return converter.convert(raw, PinotDataType.getSingleValueType(raw));
    } catch (RuntimeException e) {
      return null;
    }
  }

  /// A scalar in a multi-value key is stored as a singleton. An empty array or absent key contributes no elements.
  private static int elementCount(@Nullable JsonNode node) {
    return node == null ? 0 : node.isArray() ? node.size() : 1;
  }

  private static JsonNode elementAt(JsonNode node, int index) {
    return node.isArray() ? node.get(index) : node;
  }

  /// Whether the dense tier would keep this value for a key read through `converter`.
  static boolean keeps(@Nullable PinotDataType converter, boolean singleValue, JsonNode node) {
    if (singleValue || !node.isArray()) {
      return converter == null || convert(converter, node) != null;
    }
    if (node.isEmpty()) {
      return false;
    }
    for (JsonNode element : node) {
      if (element.isNull() || (converter != null && convert(converter, element) == null)) {
        return false;
      }
    }
    return true;
  }

  static class LazyPresenceNullVector implements NullValueVectorReader {
    private final String _key;
    @Nullable
    private final PinotDataType _converter;
    private final boolean _singleValue;
    private final OpenStructSparseBlobReader _blob;
    private volatile ImmutableRoaringBitmap _nullBitmap;

    LazyPresenceNullVector(FieldSpec fieldSpec, OpenStructSparseBlobReader blob) {
      _key = fieldSpec.getName();
      _converter = converterFor(fieldSpec.getDataType());
      _singleValue = fieldSpec.isSingleValueField();
      _blob = blob;
    }

    @Override
    public ImmutableRoaringBitmap getNullBitmap() {
      ImmutableRoaringBitmap bm = _nullBitmap;
      if (bm == null) {
        ImmutableRoaringBitmap presence = _blob.computePresence(_key, _converter,
            node -> keeps(_converter, _singleValue, node));
        MutableRoaringBitmap nulls = new MutableRoaringBitmap();
        nulls.add(0L, _blob.getNumDocs());
        nulls.andNot(presence);
        bm = nulls.toImmutableRoaringBitmap();
        _nullBitmap = bm;
      }
      return bm;
    }

    @Override
    public boolean isNull(int docId) {
      return getNullBitmap().contains(docId);
    }
  }

  private static class SparseKeyMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;
    private final SparseKeyForwardIndexReader _forwardReader;
    private volatile int _maxNumValuesPerMVEntry;

    SparseKeyMetadata(FieldSpec fieldSpec, int numDocs, int maxNumValuesPerMVEntry,
        SparseKeyForwardIndexReader forwardReader) {
      _fieldSpec = fieldSpec;
      _numDocs = numDocs;
      _forwardReader = forwardReader;
      _maxNumValuesPerMVEntry = fieldSpec.isSingleValueField() ? 0 : maxNumValuesPerMVEntry;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Override
    public int getNumValues() {
      return _numDocs;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      int max = _maxNumValuesPerMVEntry;
      return max >= 0 ? max : computeMaxNumValuesPerMVEntry();
    }

    /// Scan missing length metadata only when a caller needs it to size a buffer.
    private synchronized int computeMaxNumValuesPerMVEntry() {
      if (_maxNumValuesPerMVEntry < 0) {
        int max = 1;
        try (ForwardIndexReaderContext context = _forwardReader.createContext()) {
          for (int docId = 0; docId < _numDocs; docId++) {
            max = Math.max(max, _forwardReader.getNumValuesMV(docId, context));
          }
        }
        _maxNumValuesPerMVEntry = max;
      }
      return _maxNumValuesPerMVEntry;
    }

    @Override
    public int getCardinality() {
      return Constants.UNKNOWN_CARDINALITY;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMaxValue() {
      return null;
    }

    @Nullable
    @Override
    public PartitionFunction getPartitionFunction() {
      return null;
    }

    @Nullable
    @Override
    public Set<Integer> getPartitions() {
      return null;
    }
  }
}
