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
package org.apache.pinot.plugin.inputformat.avro;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.UuidUtils;

/// Stateless helpers for mapping between Avro schema shapes and Pinot's [DataType] / Avro JSON schema representations.
public class AvroSchemaUtil {

  private AvroSchemaUtil() {
  }

  // Avro logical-type name for UUID (see org.apache.avro.LogicalTypes). Value-level logical-type conversion lives
  // in AvroRecordExtractor; this class only deals with schema-shape mapping.
  private static final String UUID = "uuid";

  /// Returns the Avro schema for a single value of the given Pinot [DataType]. This is the canonical Pinot-to-Avro
  /// type mapping; it is driven by the **original (logical)** data type rather than the stored type, so logical types
  /// stay self-describing in the generated Avro schema instead of collapsing to their physical storage type.
  ///
  /// | Pinot type    | Avro type                | Value handed to the Avro writer                          |
  /// |---------------|--------------------------|----------------------------------------------------------|
  /// | INT           | `int`                    | `Integer`                                                 |
  /// | LONG          | `long`                   | `Long`                                                    |
  /// | FLOAT         | `float`                  | `Float`                                                   |
  /// | DOUBLE        | `double`                 | `Double`                                                  |
  /// | BOOLEAN       | `boolean`                | `Boolean` (Pinot stores `int` 0/1 — see note below)       |
  /// | TIMESTAMP     | `long{timestamp-millis}` | `Long` millis since epoch (the logical type's base type)   |
  /// | BIG_DECIMAL   | `bytes{big-decimal}`     | [java.math.BigDecimal] via Avro's `BigDecimalConversion`   |
  /// | STRING / JSON | `string`                 | `String`                                                  |
  /// | BYTES         | `bytes`                  | [java.nio.ByteBuffer]                                     |
  /// | UUID          | `string{uuid}`           | 16-byte `byte[]` via a `uuid` `Conversion`                |
  ///
  /// `timestamp-millis` needs no write-side conversion because `Long` *is* that logical type's base representation.
  /// `big-decimal` is used for BIG_DECIMAL rather than `decimal(precision, scale)` because Pinot does not pin a
  /// per-column precision/scale, and `big-decimal` encodes the scale with every value. BOOLEAN is the one type with
  /// no Avro logical type to hang a `Conversion` on, so writers must coerce Pinot's stored `int` 0/1 to `Boolean`
  /// themselves (see `SegmentProcessorAvroUtils#convertGenericRowToAvroRecord` and `AvroWriter`).
  ///
  /// This mapping is intentionally one-way: [#valueOf(Schema)] does **not** map `timestamp-millis` back to TIMESTAMP
  /// or `big-decimal` back to BIG_DECIMAL, so that Pinot schema inference from existing Avro data keeps its
  /// long-standing behavior.
  ///
  /// Throws [UnsupportedOperationException] for types with no Avro representation (STRUCT / MAP / OPEN_STRUCT /
  /// LIST / UNKNOWN).
  public static Schema toAvroSchema(DataType dataType) {
    switch (dataType) {
      case INT:
        return Schema.create(Schema.Type.INT);
      case LONG:
        return Schema.create(Schema.Type.LONG);
      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);
      case TIMESTAMP:
        return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
      case BIG_DECIMAL:
        return LogicalTypes.bigDecimal().addToSchema(Schema.create(Schema.Type.BYTES));
      case STRING:
      case JSON:
        return Schema.create(Schema.Type.STRING);
      case BYTES:
        return Schema.create(Schema.Type.BYTES);
      case UUID:
        return LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  /// Returns the Avro schema for a whole Pinot column: [#toAvroSchema(DataType)] for a single-value field, or an
  /// array of it for a multi-value field.
  public static Schema toAvroSchema(FieldSpec fieldSpec) {
    Schema valueSchema = toAvroSchema(fieldSpec.getDataType());
    return fieldSpec.isSingleValueField() ? valueSchema : Schema.createArray(valueSchema);
  }

  /// Returns the Pinot data type for a bare Avro type. This does not honor logical types (e.g. a `string` or `fixed`
  /// carrying `logicalType:uuid` maps to STRING/BYTES, not UUID); prefer [#valueOf(Schema)] when a full [Schema] is
  /// available.
  public static DataType valueOf(Schema.Type avroType) {
    switch (avroType) {
      case INT:
        return DataType.INT;
      case LONG:
        return DataType.LONG;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case BOOLEAN:
        return DataType.BOOLEAN;
      case STRING:
      case ENUM:
        return DataType.STRING;
      case BYTES:
      case FIXED:
        return DataType.BYTES;
      case MAP:
      case ARRAY:
      case RECORD:
      case UNION:
        return DataType.JSON;
      default:
        throw new IllegalStateException("Unsupported Avro type: " + avroType);
    }
  }

  /// Returns the Pinot data type associated with the given Avro schema, including logical types.
  ///
  /// Recognizes the UUID logical type on both STRING-backed schemas (Avro spec §logical-types.uuid) and FIXED(16)
  /// schemas (used by some producers including Confluent's fixed-uuid mode). Both forms arrive at
  /// [AvroRecordExtractor] as either a [java.util.UUID] (for STRING-backed logical UUIDs) or a 16-byte `byte[]`
  /// (for FIXED-backed ones), and both are accepted by [UuidUtils#toBytes].
  public static DataType valueOf(Schema schema) {
    LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
    if (logicalType != null && UUID.equals(logicalType.getName())) {
      if (schema.getType() == Schema.Type.STRING) {
        return DataType.UUID;
      }
      if (schema.getType() == Schema.Type.FIXED && schema.getFixedSize() == UuidUtils.UUID_NUM_BYTES) {
        return DataType.UUID;
      }
    }
    return valueOf(schema.getType());
  }

  /// Returns whether the given Avro type is a primitive type.
  public static boolean isPrimitiveType(Schema.Type avroType) {
    switch (avroType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case STRING:
      case ENUM:
        return true;
      default:
        return false;
    }
  }

  /// Builds the Avro schema JSON for a single Pinot field, as a nullable union `["null", <type>]`. Used to generate
  /// sample Avro data from a Pinot schema (see `AvroWriter`).
  ///
  /// The `<type>` branch is [#toAvroSchema(DataType)] rendered as JSON, so this shares the single logical-type
  /// mapping documented there. The field is always emitted as the single-value type even for a multi-value
  /// [FieldSpec] — the data generator backing `AvroWriter` has never produced Avro arrays.
  public static ObjectNode toAvroSchemaJsonObject(FieldSpec fieldSpec) {
    ObjectNode jsonSchema = JsonUtils.newObjectNode();
    jsonSchema.put("name", fieldSpec.getName());
    ArrayNode nullableUnion = JsonUtils.newArrayNode();
    nullableUnion.add("null");
    // Schema.toString() emits valid JSON: a bare name for primitives ("int"), an object for logical types
    // ({"type":"long","logicalType":"timestamp-millis"}).
    try {
      nullableUnion.add(JsonUtils.stringToJsonNode(toAvroSchema(fieldSpec.getDataType()).toString()));
    } catch (IOException e) {
      throw new IllegalStateException("Caught exception while parsing the Avro schema generated for field: "
          + fieldSpec.getName(), e);
    }
    jsonSchema.set("type", nullableUnion);
    return jsonSchema;
  }
}
