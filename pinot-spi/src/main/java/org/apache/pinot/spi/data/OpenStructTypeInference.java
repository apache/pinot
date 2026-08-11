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
package org.apache.pinot.spi.data;

import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.PinotDataType;


/// Infers the [FieldSpec.DataType] for an OPEN_STRUCT key from raw ingested values when the key has
/// no declared child [FieldSpec]. This is OPEN_STRUCT-specific policy (it keeps TIMESTAMP, folds
/// DATE/TIME/UUID to STRING, widens BYTE/CHARACTER/SHORT to INT, and returns `null` for values that
/// cannot be represented as a stored column type), distinct from the JSON-node-based inference in
/// `JsonUtils.valueOf`.
public final class OpenStructTypeInference {
  private OpenStructTypeInference() {
  }

  /// Infers the [FieldSpec.DataType] from a raw ingested value. Returns `null` when the value
  /// cannot be represented as a stored column type; callers decide whether to drop the entry or fall back
  /// to a default (e.g. STRING).
  @Nullable
  public static FieldSpec.DataType inferDataType(Object rawValue) {
    switch (PinotDataType.getSingleValueType(rawValue)) {
      case BYTE:
      case CHARACTER:
      case SHORT:
      case INT:
        return FieldSpec.DataType.INT;
      case LONG:
        return FieldSpec.DataType.LONG;
      case FLOAT:
        return FieldSpec.DataType.FLOAT;
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
      case BIG_DECIMAL:
        return FieldSpec.DataType.BIG_DECIMAL;
      case BOOLEAN:
        return FieldSpec.DataType.BOOLEAN;
      case TIMESTAMP:
        return FieldSpec.DataType.TIMESTAMP;
      case STRING:
      case DATE:
      case TIME:
      case UUID:
        return FieldSpec.DataType.STRING;
      case BYTES:
        return FieldSpec.DataType.BYTES;
      default:
        return null;
    }
  }

  /// The stored type to use for one value of an undeclared OPEN_STRUCT key, and whether that value
  /// took the STRING fallback. A plain class rather than a record because this module still targets
  /// Java 11 bytecode.
  public static final class Resolution {
    private final FieldSpec.DataType _storedType;
    private final boolean _stringFallback;

    private Resolution(FieldSpec.DataType storedType, boolean stringFallback) {
      _storedType = storedType;
      _stringFallback = stringFallback;
    }

    /// The type the value must be coerced to.
    public FieldSpec.DataType getStoredType() {
      return _storedType;
    }

    /// True when this value's own Java type maps to no [FieldSpec.DataType] and it is therefore
    /// being stored as its serialized string form. This is what the type-inference-failure meter
    /// counts, and it is deliberately false when the key already has a non-STRING type: such a
    /// value is dropped by coercion and counted there instead, so counting it here too would
    /// record one dropped value against two different meters.
    public boolean isStringFallback() {
      return _stringFallback;
    }
  }

  /// Resolves the stored type for one value of an OPEN_STRUCT key that has no declared child spec.
  ///
  /// Shared by the consuming and the sealed build paths so the two cannot drift: a value that maps
  /// to no [FieldSpec.DataType] is stored as its serialized string form rather than dropped, and if
  /// only one path did that, the same row would read differently either side of the seal boundary
  /// (and the REALTIME and OFFLINE halves of a hybrid table would disagree).
  ///
  /// @param rawValue        the non-null value being indexed
  /// @param establishedType the type already resolved for this key, or `null` on first sighting
  public static Resolution resolve(Object rawValue, @Nullable FieldSpec.DataType establishedType) {
    if (establishedType != null && establishedType != FieldSpec.DataType.STRING) {
      // Only a STRING-typed key can absorb an unmappable value as its serialized form, so for any
      // other established type the answer is fixed. Returning early also keeps the per-value
      // inference cost off the typed fast path, which runs once per key per doc during a build.
      return new Resolution(establishedType, false);
    }
    FieldSpec.DataType inferred = inferDataType(rawValue);
    if (inferred == null) {
      return new Resolution(FieldSpec.DataType.STRING, true);
    }
    return new Resolution(establishedType != null ? establishedType : inferred, false);
  }
}
