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
package org.apache.pinot.plugin.inputformat.arrow;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import javax.annotation.Nullable;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.MultiValueResult;
import org.apache.pinot.spi.utils.PinotDataType;


/// Column reader for an Apache Arrow [FieldVector].
///
/// Wraps a single Arrow [FieldVector] and exposes random-access reads conforming to the [ColumnReader] contract. The
/// vector is owned by the enclosing [ArrowColumnReaderFactory], which is responsible for its lifecycle; closing this
/// reader is a no-op for the underlying vector.
///
/// [#getValueType()] maps the backing vector's `MinorType` to the value type, naming a type only when a type-specific
/// accessor reads that vector directly:
/// - `INT` → `INT`, `BIGINT` → `LONG`, `FLOAT4` → `FLOAT`, `FLOAT8` → `DOUBLE`
/// - `DECIMAL` (128-bit) → `BIG_DECIMAL`, `VARCHAR` → `STRING`, `VARBINARY` → `BYTES`
/// - `List` of any of the above → the matching `_ARRAY` variant
/// - every other vector — `BIT` (boolean), all `TIMESTAMP` / date / time, unsigned or narrow ints, half floats,
///   `DECIMAL256`, `LARGEVARCHAR` / `LARGEVARBINARY` / fixed-size binary, and complex types (Map, Struct, Union) —
///   reports `null` and is read through the generic [#getValue(int)] accessor
///
/// [#getValue(int)] delegates to [ArrowToPinotTypeConverter] and returns the same canonical JDK types as the row-major
/// path ([ArrowRecordExtractor]) — e.g. `Boolean` for `Bool`, `Map<String, Object>` for Struct / Map, `Object[]` for
/// List variants, and `LocalDate` / `LocalTime` / `Timestamp` (or the raw epoch value under `extractRawTimeValues`)
/// for temporal types.
///
/// This class is not thread-safe.
public class ArrowColumnReader implements ColumnReader {
  private final String _columnName;
  private final FieldVector _vector;
  private final int _totalDocs;
  private final boolean _isSingleValue;
  private final boolean _extractRawTimeValues;

  /**
   * Construct an ArrowColumnReader for the given vector, surfacing converted (non-raw) temporal
   * values — equivalent to {@link #ArrowColumnReader(String, FieldVector, boolean)} with
   * {@code extractRawTimeValues = false}.
   *
   * @param columnName Pinot column name
   * @param vector Arrow field vector backing this column
   */
  public ArrowColumnReader(String columnName, FieldVector vector) {
    this(columnName, vector, false);
  }

  /**
   * Construct an ArrowColumnReader for the given vector.
   *
   * @param columnName Pinot column name
   * @param vector Arrow field vector backing this column
   * @param extractRawTimeValues when {@code true}, temporal columns surface their raw epoch values
   *        via the generic {@link #getValue(int)} path rather than canonical JDK temporal types,
   *        mirroring {@code ArrowRecordExtractorConfig.EXTRACT_RAW_TIME_VALUES} on the row-major path
   */
  public ArrowColumnReader(String columnName, FieldVector vector, boolean extractRawTimeValues) {
    _columnName = columnName;
    _vector = vector;
    _totalDocs = vector.getValueCount();
    _isSingleValue = !(vector instanceof ListVector);
    _extractRawTimeValues = extractRawTimeValues;
  }

  @Override
  public String getColumnName() {
    return _columnName;
  }

  @Override
  @Nullable
  public PinotDataType getValueType() {
    // The element vector's type maps directly to the value type; _isSingleValue selects scalar vs the _ARRAY variant.
    FieldVector elementVector = _isSingleValue ? _vector : ((ListVector) _vector).getDataVector();
    return ArrowToPinotTypeConverter.toValueType(elementVector.getMinorType(), _isSingleValue);
  }

  @Override
  public int getTotalDocs() {
    return _totalDocs;
  }

  @Override
  public boolean isNull(int docId) {
    checkBounds(docId);
    return _vector.isNull(docId);
  }

  @Override
  @Nullable
  public Object getValue(int docId)
      throws IOException {
    checkBounds(docId);
    Object value = _vector.getObject(docId);
    if (value == null) {
      return null;
    }
    // Delegate Arrow → Pinot type conversion to the shared utility extracted from
    // ArrowRecordExtractor. Returns canonical JDK types: Boolean for Bool, String for Utf8 / LargeUtf8 (unwrapped
    // from Arrow's Text), Object[] for List variants (with recursive element conversion),
    // LocalDate / LocalTime / Timestamp for temporal types, etc.
    return ArrowToPinotTypeConverter.toPinotValue(_vector.getField(), value, _extractRawTimeValues);
  }

  @Override
  public int getInt(int docId)
      throws IOException {
    checkBounds(docId);
    if (_vector instanceof IntVector) {
      return ((IntVector) _vector).get(docId);
    }
    throw typeMismatch("INT");
  }

  @Override
  public long getLong(int docId)
      throws IOException {
    checkBounds(docId);
    if (_vector instanceof BigIntVector) {
      return ((BigIntVector) _vector).get(docId);
    }
    throw typeMismatch("LONG");
  }

  @Override
  public float getFloat(int docId)
      throws IOException {
    checkBounds(docId);
    if (_vector instanceof Float4Vector) {
      return ((Float4Vector) _vector).get(docId);
    }
    throw typeMismatch("FLOAT");
  }

  @Override
  public double getDouble(int docId)
      throws IOException {
    checkBounds(docId);
    if (_vector instanceof Float8Vector) {
      return ((Float8Vector) _vector).get(docId);
    }
    throw typeMismatch("DOUBLE");
  }

  @Override
  public BigDecimal getBigDecimal(int docId)
      throws IOException {
    checkBounds(docId);
    if (_vector instanceof DecimalVector) {
      return ((DecimalVector) _vector).getObject(docId);
    }
    throw typeMismatch("BIG_DECIMAL");
  }

  @Override
  public String getString(int docId)
      throws IOException {
    checkBounds(docId);
    if (_vector instanceof VarCharVector) {
      byte[] bytes = ((VarCharVector) _vector).get(docId);
      return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
    throw typeMismatch("STRING");
  }

  @Override
  public byte[] getBytes(int docId)
      throws IOException {
    checkBounds(docId);
    if (_vector instanceof VarBinaryVector) {
      return ((VarBinaryVector) _vector).get(docId);
    }
    throw typeMismatch("BYTES");
  }

  @Override
  public MultiValueResult<int[]> getIntMV(int docId)
      throws IOException {
    checkBounds(docId);
    requireListVector();
    ListVector list = (ListVector) _vector;
    FieldVector elements = list.getDataVector();
    int start = list.getElementStartIndex(docId);
    int length = list.getElementEndIndex(docId) - start;
    BitSet nulls = elementNulls(elements, start, length);
    int[] values = new int[length];
    if (elements instanceof IntVector) {
      IntVector iv = (IntVector) elements;
      for (int i = 0; i < length; i++) {
        if (notNull(nulls, i)) {
          values[i] = iv.get(start + i);
        }
      }
    } else {
      throw typeMismatch("INT_MV");
    }
    return MultiValueResult.of(values, nulls);
  }

  @Override
  public MultiValueResult<long[]> getLongMV(int docId)
      throws IOException {
    checkBounds(docId);
    requireListVector();
    ListVector list = (ListVector) _vector;
    FieldVector elements = list.getDataVector();
    if (!(elements instanceof BigIntVector)) {
      throw typeMismatch("LONG_MV");
    }
    BigIntVector lv = (BigIntVector) elements;
    int start = list.getElementStartIndex(docId);
    int length = list.getElementEndIndex(docId) - start;
    BitSet nulls = elementNulls(elements, start, length);
    long[] values = new long[length];
    for (int i = 0; i < length; i++) {
      if (notNull(nulls, i)) {
        values[i] = lv.get(start + i);
      }
    }
    return MultiValueResult.of(values, nulls);
  }

  @Override
  public MultiValueResult<float[]> getFloatMV(int docId)
      throws IOException {
    checkBounds(docId);
    requireListVector();
    ListVector list = (ListVector) _vector;
    FieldVector elements = list.getDataVector();
    if (!(elements instanceof Float4Vector)) {
      throw typeMismatch("FLOAT_MV");
    }
    Float4Vector fv = (Float4Vector) elements;
    int start = list.getElementStartIndex(docId);
    int length = list.getElementEndIndex(docId) - start;
    BitSet nulls = elementNulls(elements, start, length);
    float[] values = new float[length];
    for (int i = 0; i < length; i++) {
      if (notNull(nulls, i)) {
        values[i] = fv.get(start + i);
      }
    }
    return MultiValueResult.of(values, nulls);
  }

  @Override
  public MultiValueResult<double[]> getDoubleMV(int docId)
      throws IOException {
    checkBounds(docId);
    requireListVector();
    ListVector list = (ListVector) _vector;
    FieldVector elements = list.getDataVector();
    if (!(elements instanceof Float8Vector)) {
      throw typeMismatch("DOUBLE_MV");
    }
    Float8Vector dv = (Float8Vector) elements;
    int start = list.getElementStartIndex(docId);
    int length = list.getElementEndIndex(docId) - start;
    BitSet nulls = elementNulls(elements, start, length);
    double[] values = new double[length];
    for (int i = 0; i < length; i++) {
      if (notNull(nulls, i)) {
        values[i] = dv.get(start + i);
      }
    }
    return MultiValueResult.of(values, nulls);
  }

  @Override
  public BigDecimal[] getBigDecimalMV(int docId)
      throws IOException {
    checkBounds(docId);
    requireListVector();
    ListVector list = (ListVector) _vector;
    FieldVector dataVector = list.getDataVector();
    if (!(dataVector instanceof DecimalVector)) {
      throw typeMismatch("BIG_DECIMAL_MV");
    }
    int start = list.getElementStartIndex(docId);
    int end = list.getElementEndIndex(docId);
    int length = end - start;
    BigDecimal[] out = new BigDecimal[length];
    DecimalVector elements = (DecimalVector) dataVector;
    for (int i = 0; i < length; i++) {
      out[i] = elements.isNull(start + i) ? null : elements.getObject(start + i);
    }
    return out;
  }

  @Override
  public String[] getStringMV(int docId)
      throws IOException {
    checkBounds(docId);
    requireListVector();
    ListVector list = (ListVector) _vector;
    FieldVector dataVector = list.getDataVector();
    if (!(dataVector instanceof VarCharVector)) {
      throw typeMismatch("STRING_MV");
    }
    int start = list.getElementStartIndex(docId);
    int end = list.getElementEndIndex(docId);
    int length = end - start;
    String[] out = new String[length];
    VarCharVector elements = (VarCharVector) dataVector;
    for (int i = 0; i < length; i++) {
      if (elements.isNull(start + i)) {
        out[i] = null;
      } else {
        byte[] bytes = elements.get(start + i);
        out[i] = new String(bytes, StandardCharsets.UTF_8);
      }
    }
    return out;
  }

  @Override
  public byte[][] getBytesMV(int docId)
      throws IOException {
    checkBounds(docId);
    requireListVector();
    ListVector list = (ListVector) _vector;
    FieldVector dataVector = list.getDataVector();
    if (!(dataVector instanceof VarBinaryVector)) {
      throw typeMismatch("BYTES_MV");
    }
    int start = list.getElementStartIndex(docId);
    int end = list.getElementEndIndex(docId);
    int length = end - start;
    byte[][] out = new byte[length][];
    VarBinaryVector elements = (VarBinaryVector) dataVector;
    for (int i = 0; i < length; i++) {
      out[i] = elements.isNull(start + i) ? null : elements.get(start + i);
    }
    return out;
  }

  /**
   * Scan element-level validity for the list slice {@code [start, start + length)} of
   * {@code elements}, returning a BitSet whose set bits mark null elements, or {@code null} when no
   * element is null (the common case, so callers skip the per-element check). The returned index
   * space is element-local: bit {@code i} corresponds to element {@code start + i}.
   */
  @Nullable
  private static BitSet elementNulls(FieldVector elements, int start, int length) {
    BitSet nulls = null;
    for (int i = 0; i < length; i++) {
      if (elements.isNull(start + i)) {
        if (nulls == null) {
          nulls = new BitSet(length);
        }
        nulls.set(i);
      }
    }
    return nulls;
  }

  /** True when element {@code i} is not null, given the {@link #elementNulls} BitSet (possibly null). */
  private static boolean notNull(@Nullable BitSet nulls, int i) {
    return nulls == null || !nulls.get(i);
  }

  private void requireListVector()
      throws IOException {
    if (!(_vector instanceof ListVector)) {
      throw new IOException("Column " + _columnName + " is not a ListVector; cannot read multi-value");
    }
  }

  private void checkBounds(int docId) {
    if (docId < 0 || docId >= _totalDocs) {
      throw new IndexOutOfBoundsException(
          "docId " + docId + " is out of range [0, " + _totalDocs + ") for column " + _columnName);
    }
  }

  private IOException typeMismatch(String expectedType) {
    return new IOException(
        "Column " + _columnName + " (Arrow type " + _vector.getField().getType() + ") cannot be read as "
            + expectedType);
  }

  /**
   * The underlying vector is owned by {@link ArrowColumnReaderFactory}; closing this reader does
   * not release the vector's memory.
   */
  @Override
  public void close() {
    // No-op: factory owns the vector lifecycle.
  }
}
