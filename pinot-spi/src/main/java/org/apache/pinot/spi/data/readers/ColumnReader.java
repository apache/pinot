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
package org.apache.pinot.spi.data.readers;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.PinotDataType;


/// Reads the values of a single column by document ID, for columnar segment building. Where a `RecordReader` exposes a
/// data source row-by-row, a `ColumnReader` exposes one column at a time: a caller reads every value of a column before
/// moving to the next one. Building a segment one fully-materialized column at a time is cheaper than transposing row
/// records when the source is already columnar (e.g. Arrow, Parquet).
///
/// Documents are addressed by a 0-based id, from 0 (inclusive) to [#getTotalDocs()] (exclusive). Two access patterns
/// are supported:
/// - **Random access** — [#getValue(int)] and the typed accessors may be called for any document id, in any order.
/// - **Repeated sequential traversal** — the full range may be traversed by ascending document id more than once. The
///   reader starts positioned at document 0; call [#rewind()] to reset it before each subsequent traversal. The
///   segment build uses this: a statistics pass, then an index-writing pass over the same reader.
///
/// To read a column:
/// 1. Call [#getValueType()] once. A non-`null` result names the type that can be read directly through the matching
///    type-specific accessor; `null` means the column must be read through the boxed [#getValue(int)].
/// 2. For each document id, call [#isNull(int)] first — type-specific accessors cannot represent null and must not be
///    called for a null value.
/// 3. Read each non-null value with the accessor chosen in step 1, e.g. [#getInt(int)] / [#getIntMV(int)] when
///    [#getValueType()] is [PinotDataType#INT] / [PinotDataType#INT_ARRAY], or [#getValue(int)] otherwise.
///
/// Implementations perform any conversion between the source encoding and the returned Pinot type, are not required to
/// be thread-safe, and must be released with [#close()] when no longer needed.
///
/// ```
/// PinotDataType valueType = columnReader.getValueType();
/// for (int docId = 0; docId < columnReader.getTotalDocs(); docId++) {
///   if (columnReader.isNull(docId)) {
///     continue;
///   }
///   if (valueType == PinotDataType.INT) {
///     consumeInt(columnReader.getInt(docId));
///   } else {
///     consumeObject(columnReader.getValue(docId));
///   }
/// }
/// ```
public interface ColumnReader extends Closeable, Serializable {

  /// Returns the name of the column read by this reader.
  String getColumnName();

  /// Returns the [PinotDataType] that [#getValue(int)] produces for this column, when that type has a matching typed
  /// accessor; otherwise `null`.
  ///
  /// A non-`null` result `T` therefore names an accessor that returns exactly what [#getValue(int)] returns, only
  /// unboxed: for every document id where [#isNull(int)] is `false`, the accessor matching `T` — e.g. [#getInt(int)]
  /// for [PinotDataType#INT], [#getIntMV(int)] for [PinotDataType#INT_ARRAY] — returns the same value as
  /// [#getValue(int)]. `null` means [#getValue(int)]'s type has no typed accessor; read the column through
  /// [#getValue(int)].
  ///
  /// The result is a constant property of the column (independent of the document id) and, when non-`null`, is one of
  /// the accessor-backed types — [PinotDataType#INT], [PinotDataType#LONG], [PinotDataType#FLOAT],
  /// [PinotDataType#DOUBLE], [PinotDataType#BIG_DECIMAL], [PinotDataType#STRING], [PinotDataType#BYTES] — or their
  /// `_ARRAY` variants for a multi-value column.
  ///
  /// Because [#getValue(int)] returns the column's logical type, a logical type whose object is not itself an
  /// accessor-backed type returns `null`: `BOOLEAN` (a `Boolean`), `TIMESTAMP` (a `Timestamp`), and the complex types.
  /// A `JSON` column reports [PinotDataType#STRING], since it is stored and read as its text `String`. Implementations
  /// keyed on the stored [DataType] can use [#toValueType(DataType, boolean)].
  @Nullable
  PinotDataType getValueType();

  /// Maps a stored [DataType] and cardinality to the [PinotDataType] a [#getValueType()] implementation should report,
  /// or `null` when the type has no dedicated accessor (e.g. BOOLEAN, TIMESTAMP, or a complex type). A convenience for
  /// implementations that already hold the column's [DataType]. `JSON` maps to [PinotDataType#STRING], since it is
  /// stored and read as its text `String`.
  @Nullable
  static PinotDataType toValueType(DataType dataType, boolean singleValue) {
    switch (dataType) {
      case INT:
        return singleValue ? PinotDataType.INT : PinotDataType.INT_ARRAY;
      case LONG:
        return singleValue ? PinotDataType.LONG : PinotDataType.LONG_ARRAY;
      case FLOAT:
        return singleValue ? PinotDataType.FLOAT : PinotDataType.FLOAT_ARRAY;
      case DOUBLE:
        return singleValue ? PinotDataType.DOUBLE : PinotDataType.DOUBLE_ARRAY;
      case BIG_DECIMAL:
        return singleValue ? PinotDataType.BIG_DECIMAL : PinotDataType.BIG_DECIMAL_ARRAY;
      case STRING:
        return singleValue ? PinotDataType.STRING : PinotDataType.STRING_ARRAY;
      case JSON:
        return PinotDataType.STRING;
      case BYTES:
        return singleValue ? PinotDataType.BYTES : PinotDataType.BYTES_ARRAY;
      default:
        return null;
    }
  }

  /// Converts a value read from physical storage into the logical object [#getValue(int)] must return. Pinot stores
  /// `BOOLEAN` as an `int` (`0`/`1`) and `TIMESTAMP` as a `long` (epoch millis), so those surface as `Boolean` /
  /// `Timestamp`; every other type is stored as its logical type already. Handles the single-value form and the
  /// multi-value `Object[]` form, and returns `null` unchanged. A convenience for segment-backed implementations that
  /// read stored values keyed on the stored [DataType].
  @Nullable
  static Object toLogicalValue(@Nullable Object storedValue, DataType dataType) {
    if (storedValue == null) {
      return null;
    }
    switch (dataType) {
      case BOOLEAN:
        if (storedValue instanceof Object[]) {
          Object[] stored = (Object[]) storedValue;
          Boolean[] logical = new Boolean[stored.length];
          for (int i = 0; i < stored.length; i++) {
            logical[i] = BooleanUtils.fromNonNullInternalValue(stored[i]);
          }
          return logical;
        }
        return BooleanUtils.fromNonNullInternalValue(storedValue);
      case TIMESTAMP:
        if (storedValue instanceof Object[]) {
          Object[] stored = (Object[]) storedValue;
          Timestamp[] logical = new Timestamp[stored.length];
          for (int i = 0; i < stored.length; i++) {
            logical[i] = new Timestamp(((Number) stored[i]).longValue());
          }
          return logical;
        }
        return new Timestamp(((Number) storedValue).longValue());
      default:
        return storedValue;
    }
  }

  /// Returns the number of documents in this column. Document ids run from 0 (inclusive) to this value (exclusive).
  int getTotalDocs();

  /// Resets this reader to the beginning so the column can be traversed again from document 0. The segment build
  /// traverses each column twice — a statistics pass, then an index-writing pass — and calls this before the second
  /// traversal. A random-access reader may keep the default no-op; a streaming or batched reader overrides it to reset
  /// its position (e.g. rewind the source or drop the current batch).
  ///
  /// @throws IOException if an I/O error occurs while resetting
  default void rewind()
      throws IOException {
  }

  /// Returns whether the value at the given document id is null. Call this before any type-specific accessor, which
  /// cannot represent null.
  ///
  /// @param docId 0-based document id, from 0 (inclusive) to [#getTotalDocs()] (exclusive)
  /// @throws IndexOutOfBoundsException if `docId` is out of range
  boolean isNull(int docId)
      throws IOException;

  /// Returns the value at the given document id as a boxed `Object`, for both single-value and multi-value columns (a
  /// multi-value column returns an `Object[]`). This is the general-purpose path, used when [#getValueType()] returns
  /// `null`, or when the caller does not need the primitive form — e.g. it would box the value anyway, or it converts
  /// the value itself.
  ///
  /// @param docId 0-based document id, from 0 (inclusive) to [#getTotalDocs()] (exclusive)
  /// @throws IndexOutOfBoundsException if `docId` is out of range
  /// @throws IOException if an I/O error occurs while reading
  @Nullable
  Object getValue(int docId)
      throws IOException;

  // Single-value accessors

  /// Returns the single-value int / long / float / double / BigDecimal / String / byte[] value at the given document
  /// id. Call the accessor matching [#getValueType()], and only for a non-null value (see [#isNull(int)]).
  ///
  /// @param docId 0-based document id, from 0 (inclusive) to [#getTotalDocs()] (exclusive)
  /// @throws IndexOutOfBoundsException if `docId` is out of range
  /// @throws IOException if an I/O error occurs while reading
  int getInt(int docId)
      throws IOException;

  long getLong(int docId)
      throws IOException;

  float getFloat(int docId)
      throws IOException;

  double getDouble(int docId)
      throws IOException;

  BigDecimal getBigDecimal(int docId)
      throws IOException;

  String getString(int docId)
      throws IOException;

  byte[] getBytes(int docId)
      throws IOException;

  // Multi-value accessors

  /// Returns the multi-value int[] / long[] / float[] / double[] / BigDecimal[] / String[] / byte[][] values at the
  /// given document id. Call the accessor matching [#getValueType()], and only for a non-null value (see
  /// [#isNull(int)]).
  ///
  /// The primitive-array accessors ([#getIntMV(int)], [#getLongMV(int)], [#getFloatMV(int)], [#getDoubleMV(int)])
  /// return a [MultiValueResult] that also tracks element-level nullity; use [MultiValueResult#hasNulls()] and
  /// [MultiValueResult#isNull(int)] to detect null elements within the array.
  ///
  /// @param docId 0-based document id, from 0 (inclusive) to [#getTotalDocs()] (exclusive)
  /// @throws IndexOutOfBoundsException if `docId` is out of range
  /// @throws IOException if an I/O error occurs while reading
  MultiValueResult<int[]> getIntMV(int docId)
      throws IOException;

  MultiValueResult<long[]> getLongMV(int docId)
      throws IOException;

  MultiValueResult<float[]> getFloatMV(int docId)
      throws IOException;

  MultiValueResult<double[]> getDoubleMV(int docId)
      throws IOException;

  BigDecimal[] getBigDecimalMV(int docId)
      throws IOException;

  String[] getStringMV(int docId)
      throws IOException;

  byte[][] getBytesMV(int docId)
      throws IOException;
}
