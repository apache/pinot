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
package org.apache.pinot.query.planner.spi.stats;

import java.math.BigDecimal;
import javax.annotation.Nullable;


/// How a column's min/max values, which are stored as text, must be ordered and deserialized.
///
/// The type has to be recorded with the values because it cannot be recovered from them. Guessing
/// "numeric if it parses" is wrong in both directions: a STRING column holding `"9"` and `"10"`
/// orders lexically in Pinot but would compare numerically, and a LONG beyond 2^53 loses precision
/// as a `double` — in the direction that narrows the range, which would exclude rows that exist.
///
/// This enum deliberately mirrors only the ordering classes the statistics layer needs rather than
/// Pinot's full `DataType`: this module does not depend on `pinot-spi` at compile scope, and the
/// statistics layer only ever needs to know how to order a value, not how it is encoded. Producers
/// map their column type onto it.
public enum ColumnValueType {
  /// Exact integral ordering; parsed as [Long] so values beyond 2^53 keep every digit.
  LONG,
  /// Floating-point ordering.
  DOUBLE,
  /// Arbitrary-precision numeric ordering.
  BIG_DECIMAL,
  /// Lexical ordering, matching how Pinot orders string columns.
  STRING;

  /// Resolves a persisted type name, returning `null` for any name this build does not know.
  ///
  /// Deliberately not [#valueOf(String)]: the name comes from a store that can outlive the process
  /// that wrote it, so a broker reading a file written by a newer build -- an aborted rolling
  /// upgrade, say -- would otherwise throw [IllegalArgumentException] out of the query-planning
  /// path. `null` is already the documented "ordering unknown" value, which degrades to untrusted
  /// bounds, so an unrecognized name costs precision rather than the query.
  @Nullable
  public static ColumnValueType fromName(@Nullable String name) {
    if (name == null) {
      return null;
    }
    for (ColumnValueType type : values()) {
      if (type.name().equals(name)) {
        return type;
      }
    }
    return null;
  }

  /// Orders two stored values of this type. Values that cannot be parsed as this type fall back to
  /// lexical order, so a malformed row degrades rather than throwing on the planning path.
  public int compare(String a, String b) {
    try {
      switch (this) {
        case LONG:
          return Long.compare(Long.parseLong(a), Long.parseLong(b));
        case DOUBLE:
          return Double.compare(Double.parseDouble(a), Double.parseDouble(b));
        case BIG_DECIMAL:
          return new BigDecimal(a).compareTo(new BigDecimal(b));
        default:
          return a.compareTo(b);
      }
    } catch (NumberFormatException e) {
      return a.compareTo(b);
    }
  }

  /// Deserializes a stored value into the [Comparable] a consumer expects for this type, or the
  /// raw [String] when it cannot be parsed.
  @Nullable
  public Comparable<?> toComparable(@Nullable String value) {
    if (value == null) {
      return null;
    }
    try {
      switch (this) {
        case LONG:
          return Long.parseLong(value);
        case DOUBLE:
          return Double.parseDouble(value);
        case BIG_DECIMAL:
          return new BigDecimal(value);
        default:
          return value;
      }
    } catch (NumberFormatException e) {
      return value;
    }
  }
}
