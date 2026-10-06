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
package org.apache.pinot.core.function.scalar;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;


/// Scalar form of the `IN_ID_SET` transform function, for queries that cannot run the transform function:
/// - In the multi-stage engine, wherever the predicate is not pushed into a leaf stage, e.g. a filter over a join,
///   `HAVING` or a join condition. Leaf stages keep using the `InIdSetTransformFunction`, because transform functions
///   take precedence over scalar functions.
/// - In the single-stage engine, after aggregation, e.g. in `HAVING`. Aggregations such as `MAX` return DOUBLE there,
///   so cast them to the type the IdSet was built from.
///
/// The method is not static, so each call site gets its own instance (see `FunctionInvoker`), which caches the last
/// deserialized IdSet:
/// - A literal IdSet reaches every row as the same String instance, so it is deserialized once per call site.
/// - An IdSet read from rows (e.g. a joined `IDSET` subquery result) arrives as a new but equal String instance with
///   every data block, which costs one String comparison per block.
/// - A call site whose IdSet changes from row to row (e.g. a correlated `IDSET` subquery, with one IdSet per group)
///   deserializes the IdSet on every change, so it only suits small IdSets.
///
/// Not thread-safe: an instance serves one call site, which one thread evaluates at a time.
public class IdSetFunctions {
  private String _serializedIdSet;
  private IdSet _idSet;
  // The IdSet and the value class whose combination passed the last value type validation
  private IdSet _validatedIdSet;
  private Class<?> _validatedValueClass;

  /// Returns whether the IdSet contains the value. As with an `IN` subquery, the result is `false` for an empty or
  /// `null` IdSet, even when the value is `null`, and `null` for a `null` value otherwise. `IDSET` returns a `null`
  /// IdSet over zero rows when null handling is enabled.
  ///
  /// Values arrive as their external Java types. BOOLEAN, TIMESTAMP and UUID values are looked up through their
  /// stored INT, LONG and BYTES values, as the transform function does.
  ///
  /// `isDeterministic = false` keeps all-literal calls out of compile-time evaluation, which passes literals as Java
  /// types the lookup cannot map to a value type (e.g. BigDecimal for an INT literal). The runtime evaluates them
  /// instead, as before this function existed. The flag also marks the function VOLATILE, so ingestion transforms
  /// cannot use it.
  @Nullable
  @ScalarFunction(nullableParameters = true, isDeterministic = false)
  public Boolean inIdSet(@Nullable Object value, @Nullable String serializedIdSet) {
    if (serializedIdSet == null) {
      return false;
    }
    IdSet idSet = getIdSet(serializedIdSet);
    if (value == null) {
      return idSet.getType() == IdSet.Type.EMPTY ? false : null;
    }
    if (idSet != _validatedIdSet || value.getClass() != _validatedValueClass) {
      IdSets.validateValueType(idSet, getStoredType(value));
      _validatedIdSet = idSet;
      _validatedValueClass = value.getClass();
    }
    if (value instanceof Integer) {
      return idSet.contains(((Integer) value).intValue());
    }
    if (value instanceof Long) {
      return idSet.contains(((Long) value).longValue());
    }
    if (value instanceof Float) {
      return idSet.contains(((Float) value).floatValue());
    }
    if (value instanceof Double) {
      return idSet.contains(((Double) value).doubleValue());
    }
    if (value instanceof String) {
      return idSet.contains((String) value);
    }
    if (value instanceof byte[]) {
      return idSet.contains((byte[]) value);
    }
    if (value instanceof Boolean) {
      return idSet.contains((Boolean) value ? 1 : 0);
    }
    if (value instanceof Timestamp) {
      return idSet.contains(((Timestamp) value).getTime());
    }
    if (value instanceof UUID) {
      return idSet.contains(UuidUtils.toBytes((UUID) value));
    }
    if (value instanceof ByteArray) {
      return idSet.contains(((ByteArray) value).getBytes());
    }
    throw new IllegalStateException("Unsupported value class: " + value.getClass().getName());
  }

  @VisibleForTesting
  IdSet getIdSet(String serializedIdSet) {
    if (serializedIdSet != _serializedIdSet) {
      if (!serializedIdSet.equals(_serializedIdSet)) {
        _idSet = deserialize(serializedIdSet);
      }
      // Keep the latest instance: the following rows of a data block share it, so they match by reference
      _serializedIdSet = serializedIdSet;
    }
    return _idSet;
  }

  private static IdSet deserialize(String serializedIdSet) {
    try {
      return IdSets.fromBase64String(serializedIdSet);
    } catch (IOException | RuntimeException e) {
      throw new IllegalArgumentException("Caught exception while deserializing IdSet: " + e, e);
    }
  }

  private static DataType getStoredType(Object value) {
    if (value instanceof Integer || value instanceof Boolean) {
      return DataType.INT;
    }
    if (value instanceof Long || value instanceof Timestamp) {
      return DataType.LONG;
    }
    if (value instanceof Float) {
      return DataType.FLOAT;
    }
    if (value instanceof Double) {
      return DataType.DOUBLE;
    }
    if (value instanceof String) {
      return DataType.STRING;
    }
    if (value instanceof byte[] || value instanceof UUID || value instanceof ByteArray) {
      return DataType.BYTES;
    }
    if (value instanceof BigDecimal) {
      return DataType.BIG_DECIMAL;
    }
    throw new IllegalArgumentException(
        "Cannot look up values of class " + value.getClass().getName() + " in an IdSet");
  }
}
