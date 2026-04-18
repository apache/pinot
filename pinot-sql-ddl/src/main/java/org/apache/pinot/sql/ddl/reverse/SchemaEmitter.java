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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;


/**
 * Emits a list of canonical column-declaration strings from a Pinot {@link Schema}. Each
 * declaration is formatted to match the grammar consumed by {@code SqlPinotColumnDeclaration}.
 *
 * <p>Column ordering follows the schema's natural ordering: dimensions first (insertion order),
 * then metrics, then date-time columns. This mirrors what users see in JSON config and is stable
 * across emit/parse round-trips.
 */
final class SchemaEmitter {

  private SchemaEmitter() {
  }

  /**
   * Returns canonical column declarations for all fields in {@code schema}.
   *
   * <p>Fails fast with {@link IllegalArgumentException} for MAP/LIST/STRUCT columns because those
   * types have no DDL representation yet; emitting incomplete DDL for them would produce a
   * statement that silently drops part of the schema on replay.
   */
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
    for (DateTimeFieldSpec dt : schema.getDateTimeFieldSpecs()) {
      out.add(emitColumn(dt));
    }
    // Legacy time field: emit as a DATETIME column so the column is not silently dropped.
    TimeFieldSpec timeFieldSpec = schema.getTimeFieldSpec();
    if (timeFieldSpec != null) {
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
    org.apache.pinot.spi.data.TimeGranularitySpec tgs = spec.getOutgoingGranularitySpec();
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
    // free of redundant defaults.
    Object defaultValue = spec.getDefaultNullValue();
    Object naturalDefault =
        FieldSpec.getDefaultNullValue(spec.getFieldType(), spec.getDataType(), null);
    if (defaultValue != null && !defaultValue.equals(naturalDefault)) {
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

  /**
   * Maps a Pinot {@link DataType} to the canonical SQL keyword we emit. We pick the
   * Pinot-native names (LONG, BIG_DECIMAL, STRING, BYTES) where they exist so the round-trip
   * matches what a human would write.
   */
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
      case BIG_DECIMAL:
      case BOOLEAN:
        return value.toString();
      default:
        return SqlIdentifiers.quoteString(value.toString());
    }
  }
}
