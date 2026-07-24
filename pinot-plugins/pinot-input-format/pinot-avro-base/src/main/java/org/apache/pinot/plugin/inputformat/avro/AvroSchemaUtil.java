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

  /// Builds the Avro schema JSON for a single Pinot field. Used to generate sample Avro data from a Pinot schema
  /// (see `AvroWriter`). Each field is emitted as a nullable union `["null", <type>]`.
  ///
  /// The switch is driven by the original (logical) [DataType] rather than the stored type, so logical types are
  /// represented faithfully instead of collapsing to their physical storage type: BOOLEAN maps to Avro `boolean`,
  /// TIMESTAMP to a `timestamp-millis` long (not a plain `int`/`long`), and UUID to a `uuid`-logical-type string
  /// (not raw `bytes`).
  ///
  /// This intentionally differs from the segment-processing converters `AvroUtils.getAvroSchemaFromPinotSchema` and
  /// `SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema`, which switch on the stored type because they
  /// serialize Pinot's physically-stored values (e.g. an int for BOOLEAN) directly.
  public static ObjectNode toAvroSchemaJsonObject(FieldSpec fieldSpec) {
    ObjectNode jsonSchema = JsonUtils.newObjectNode();
    jsonSchema.put("name", fieldSpec.getName());
    DataType dataType = fieldSpec.getDataType();
    switch (dataType) {
      case INT:
        jsonSchema.set("type", convertStringsToJsonArray("null", "int"));
        return jsonSchema;
      case LONG:
        jsonSchema.set("type", convertStringsToJsonArray("null", "long"));
        return jsonSchema;
      case FLOAT:
        jsonSchema.set("type", convertStringsToJsonArray("null", "float"));
        return jsonSchema;
      case DOUBLE:
        jsonSchema.set("type", convertStringsToJsonArray("null", "double"));
        return jsonSchema;
      case BOOLEAN:
        jsonSchema.set("type", convertStringsToJsonArray("null", "boolean"));
        return jsonSchema;
      case TIMESTAMP:
        // TIMESTAMP is stored as LONG millis-since-epoch; annotate the long branch with the timestamp-millis
        // logical type so the value stays a long but is self-describing as a timestamp.
        ObjectNode timestampType = JsonUtils.newObjectNode();
        timestampType.put("type", "long");
        timestampType.put("logicalType", "timestamp-millis");
        ArrayNode timestampUnion = JsonUtils.newArrayNode();
        timestampUnion.add("null");
        timestampUnion.add(timestampType);
        jsonSchema.set("type", timestampUnion);
        return jsonSchema;
      case STRING:
      case JSON:
        jsonSchema.set("type", convertStringsToJsonArray("null", "string"));
        return jsonSchema;
      case UUID:
        // UUID is a logical type; represent it faithfully as an Avro string annotated with logicalType "uuid" rather
        // than collapsing to raw bytes, so generated sample data round-trips as canonical UUID strings.
        ObjectNode uuidType = JsonUtils.newObjectNode();
        uuidType.put("type", "string");
        uuidType.put("logicalType", "uuid");
        ArrayNode uuidUnion = JsonUtils.newArrayNode();
        uuidUnion.add("null");
        uuidUnion.add(uuidType);
        jsonSchema.set("type", uuidUnion);
        return jsonSchema;
      case BYTES:
        jsonSchema.set("type", convertStringsToJsonArray("null", "bytes"));
        return jsonSchema;
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  private static ArrayNode convertStringsToJsonArray(String... strings) {
    ArrayNode jsonArray = JsonUtils.newArrayNode();
    for (String string : strings) {
      jsonArray.add(string);
    }
    return jsonArray;
  }
}
