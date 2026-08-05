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
package org.apache.pinot.sql.ddl.reverse;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.BytesUtils;


/// Emits a list of canonical column-declaration strings from a Pinot [Schema]. Each
/// declaration is formatted to match the grammar consumed by `SqlPinotColumnDeclaration`.
///
/// Column ordering follows the schema's natural ordering: dimensions first (insertion order),
/// then metrics, then date-time columns. This mirrors what users see in JSON config and is stable
/// across emit/parse round-trips.
final class SchemaEmitter {

  private SchemaEmitter() {
  }

  /// Returns canonical column declarations for all fields in `schema`.
  ///
  /// Fails fast with [IllegalArgumentException] for MAP/LIST/STRUCT columns because those
  /// types have no DDL representation yet; emitting incomplete DDL for them would produce a
  /// statement that silently drops part of the schema on replay.
  static List<String> emitColumns(Schema schema) {
    List<String> out = new ArrayList<>();
    for (DimensionFieldSpec dim : schema.getDimensionFieldSpecs()) {
      validateEmittable(dim);
      out.add(emitColumn(dim));
    }
    for (MetricFieldSpec metric : schema.getMetricFieldSpecs()) {
      validateEmittable(metric);
      out.add(emitColumn(metric));
    }
    Set<String> dateTimeNames = new HashSet<>();
    for (DateTimeFieldSpec dt : schema.getDateTimeFieldSpecs()) {
      dateTimeNames.add(dt.getName());
      out.add(emitColumn(dt));
    }
    // Reject COMPLEX columns explicitly: the DDL grammar does not yet have a syntax for nested
    // STRUCT/LIST/MAP fields. Without this guard, ComplexFieldSpec columns would silently fall
    // through emitColumns and the canonical DDL would re-parse into a schema missing them.
    for (FieldSpec complexSpec : schema.getComplexFieldSpecs()) {
      throw new IllegalArgumentException(
          "SHOW CREATE TABLE cannot represent complex column '" + complexSpec.getName()
              + "' in DDL; replay of the emitted DDL would silently drop this column. "
              + "The DDL grammar does not yet support COMPLEX field type.");
    }
    // Legacy time field: emit as a DATETIME column so the column is not silently dropped — but
    // skip when a DateTimeFieldSpec already exists with the same column name. A schema mid-
    // migration may carry both the legacy TimeFieldSpec and the modern DateTimeFieldSpec for
    // the same logical column, and emitting both would yield a duplicate column declaration
    // that fails to re-parse.
    TimeFieldSpec timeFieldSpec = schema.getTimeFieldSpec();
    if (timeFieldSpec != null && !dateTimeNames.contains(timeFieldSpec.getName())) {
      out.add(emitTimeColumn(timeFieldSpec));
    }
    return out;
  }

  private static void validateEmittable(FieldSpec spec) {
    DataType dt = spec.getDataType();
    if (dt == DataType.MAP || dt == DataType.LIST || dt == DataType.STRUCT
        || dt == DataType.UNKNOWN) {
      throw new IllegalArgumentException(
          "SHOW CREATE TABLE cannot represent column '" + spec.getName() + "' of type " + dt
              + " in DDL; replay of the emitted DDL would silently drop this column. "
              + "The DDL grammar does not yet support complex types.");
    }
  }

  private static String emitTimeColumn(TimeFieldSpec spec) {
    // Emit the legacy time column as a DATETIME column so it survives a round-trip.
    // Use the outgoing granularity spec to derive format and granularity strings matching
    // the DateTimeFieldSpec convention ({size}:{unit}:{format}).
    TimeGranularitySpec tgs = spec.getOutgoingGranularitySpec();
    String format = tgs.getTimeUnitSize() + ":" + tgs.getTimeType().name() + ":"
        + tgs.getTimeFormat();
    String granularity = tgs.getTimeUnitSize() + ":" + tgs.getTimeType().name();
    StringBuilder sb = new StringBuilder();
    sb.append(SqlIdentifiers.quote(spec.getName()));
    sb.append(' ').append(emitDataType(spec.getDataType()));
    sb.append(" DATETIME FORMAT ").append(SqlIdentifiers.quoteString(format));
    sb.append(" GRANULARITY ").append(SqlIdentifiers.quoteString(granularity));
    return sb.toString();
  }

  private static String emitColumn(FieldSpec spec) {
    StringBuilder sb = new StringBuilder();
    sb.append(SqlIdentifiers.quote(spec.getName()));
    sb.append(' ').append(emitDataType(spec.getDataType()));
    if (spec.isNotNull()) {
      sb.append(" NOT NULL");
    }
    // Only emit DEFAULT when the user-supplied value differs from the data-type's natural
    // default; this matches Pinot's own JSON serialization rule and keeps canonical output
    // free of redundant defaults. For BYTES, use Arrays.equals via DataType.equals (byte[]
    // reference equality would falsely flag every BYTES default as a mismatch). For
    // BIG_DECIMAL, use compareTo so different scales of the same numeric value compare equal
    // (BigDecimal.equals is scale-sensitive: new BigDecimal("0.0").equals(BigDecimal.ZERO)
    // is false, which would make us emit a redundant DEFAULT 0.0).
    Object defaultValue = spec.getDefaultNullValue();
    Object naturalDefault =
        FieldSpec.getDefaultNullValue(spec.getFieldType(), spec.getDataType(), null);
    boolean atNaturalDefault;
    if (defaultValue == null || naturalDefault == null) {
      atNaturalDefault = (defaultValue == null && naturalDefault == null);
    } else if (spec.getDataType() == DataType.BIG_DECIMAL && defaultValue instanceof BigDecimal
        && naturalDefault instanceof BigDecimal) {
      atNaturalDefault = ((BigDecimal) defaultValue).compareTo((BigDecimal) naturalDefault) == 0;
    } else {
      atNaturalDefault = spec.getDataType().equals(defaultValue, naturalDefault);
    }
    if (defaultValue != null && !atNaturalDefault) {
      sb.append(" DEFAULT ").append(emitDefault(defaultValue, spec.getDataType()));
    }
    if (spec instanceof DateTimeFieldSpec) {
      DateTimeFieldSpec dt = (DateTimeFieldSpec) spec;
      sb.append(" DATETIME FORMAT ").append(SqlIdentifiers.quoteString(dt.getFormat()));
      sb.append(" GRANULARITY ").append(SqlIdentifiers.quoteString(dt.getGranularity()));
    } else if (spec instanceof MetricFieldSpec) {
      sb.append(" METRIC");
    } else {
      // DimensionFieldSpec — emit DIMENSION (with ARRAY for multi-value) so the round-trip
      // preserves the MV shape. Single-value is the default and needs no suffix.
      sb.append(" DIMENSION");
      if (!spec.isSingleValueField()) {
        sb.append(" ARRAY");
      }
    }
    return sb.toString();
  }

  /// Maps a Pinot [DataType] to the canonical SQL keyword we emit. We pick the
  /// Pinot-native names (LONG, BIG_DECIMAL, STRING, BYTES) where they exist so the round-trip
  /// matches what a human would write.
  private static String emitDataType(DataType dt) {
    switch (dt) {
      case INT:
        return "INT";
      case LONG:
        return "LONG";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case BIG_DECIMAL:
        return "BIG_DECIMAL";
      case BOOLEAN:
        return "BOOLEAN";
      case TIMESTAMP:
        return "TIMESTAMP";
      case STRING:
        return "STRING";
      case JSON:
        return "JSON";
      case BYTES:
        return "BYTES";
      default:
        // Fall back to the enum name for types we don't have a canonical short name for
        // (LIST, MAP, STRUCT, UNKNOWN). These are not yet expressible in DDL but emitting the
        // name lets a future grammar accept them.
        return dt.name();
    }
  }

  private static String emitDefault(Object value, DataType dt) {
    if (value == null) {
      return "NULL";
    }
    switch (dt) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return value.toString();
      case BOOLEAN:
        // Pinot stores BOOLEAN values internally as Integer 0/1; emit the SQL literal form
        // (TRUE/FALSE) so canonical DDL is grammar-standard rather than exposing the internal
        // encoding.
        if (value instanceof Number) {
          return ((Number) value).intValue() == 1 ? "TRUE" : "FALSE";
        }
        if (value instanceof Boolean) {
          return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        return value.toString();
      case TIMESTAMP:
        // Pinot stores TIMESTAMP defaults as Long millis. Emit a quoted UTC ISO-8601 string
        // (Instant.toString) — NOT java.sql.Timestamp.toString, which formats in the JVM's
        // default time zone and would make canonical DDL emit different strings on different
        // controllers for the same input. ISO-8601 round-trips through TimestampUtils.
        if (value instanceof Number) {
          return SqlIdentifiers.quoteString(
              Instant.ofEpochMilli(((Number) value).longValue()).toString());
        }
        return SqlIdentifiers.quoteString(value.toString());
      case BIG_DECIMAL:
        // BigDecimal.toString() can emit scientific notation (e.g. "1E+10") for large or
        // small magnitudes, which Calcite's Literal() rule does not accept. toPlainString()
        // always produces the decimal form so the round-trip stays grammar-legal.
        return value instanceof BigDecimal ? ((BigDecimal) value).toPlainString() : value.toString();
      case BYTES:
        // Pinot stores BYTES defaults internally as byte[]. byte[].toString() returns the JVM
        // identity-hash form (e.g. "[B@1f32e575") which would emit structurally invalid DDL.
        // Convert to hex matching FieldSpec.getDefaultNullValueString and toJsonObject so the
        // canonical DDL re-parses back to the same byte content via FieldSpec.setDefaultNullValue.
        if (value instanceof byte[]) {
          return SqlIdentifiers.quoteString(BytesUtils.toHexString((byte[]) value));
        }
        return SqlIdentifiers.quoteString(value.toString());
      default:
        return SqlIdentifiers.quoteString(value.toString());
    }
  }
}
