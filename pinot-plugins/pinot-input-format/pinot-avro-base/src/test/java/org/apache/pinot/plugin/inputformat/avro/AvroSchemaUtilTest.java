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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class AvroSchemaUtilTest {

  /// The switch must be driven by the original (logical) data type, not the stored type. Otherwise BOOLEAN collapses
  /// to "int" and TIMESTAMP to a plain "long", misrepresenting the column in the generated Avro schema.
  @Test
  public void testToAvroSchemaJsonObjectUsesOriginalType() {
    assertPrimitiveType(DataType.INT, "int");
    assertPrimitiveType(DataType.LONG, "long");
    assertPrimitiveType(DataType.FLOAT, "float");
    assertPrimitiveType(DataType.DOUBLE, "double");
    assertPrimitiveType(DataType.STRING, "string");
    assertPrimitiveType(DataType.JSON, "string");
    assertPrimitiveType(DataType.BYTES, "bytes");
    // Logical types must not collapse to their stored INT/LONG type.
    assertPrimitiveType(DataType.BOOLEAN, "boolean");

    JsonNode type = typeOf(DataType.TIMESTAMP);
    assertEquals(type.get(0).asText(), "null");
    JsonNode timestampBranch = type.get(1);
    assertEquals(timestampBranch.get("type").asText(), "long");
    assertEquals(timestampBranch.get("logicalType").asText(), "timestamp-millis");
  }

  /// Types with no Avro mapping (e.g. BIG_DECIMAL) must be rejected rather than silently mishandled.
  @Test
  public void testToAvroSchemaJsonObjectRejectsUnsupportedType() {
    assertThrows(UnsupportedOperationException.class,
        () -> AvroSchemaUtil.toAvroSchemaJsonObject(new DimensionFieldSpec("col", DataType.BIG_DECIMAL, true)));
  }

  /// UUID is a logical type; a single-value column maps to an Avro string carrying the "uuid" logical type.
  @Test
  public void testToAvroSchemaJsonObjectForUuid() {
    JsonNode type = typeOf(DataType.UUID);
    assertEquals(type.get(0).asText(), "null");
    assertEquals(type.get(1).get("type").asText(), "string");
    assertEquals(type.get(1).get("logicalType").asText(), "uuid");
  }

  /// A multi-value UUID column maps to an Avro array of "uuid"-logical-type strings.
  @Test
  public void testToAvroSchemaJsonObjectForUuidArray() {
    ObjectNode jsonSchema =
        AvroSchemaUtil.toAvroSchemaJsonObject(new DimensionFieldSpec("col", DataType.UUID, false));
    JsonNode type = jsonSchema.get("type");
    assertEquals(type.get(0).asText(), "null");
    assertEquals(type.get(1).get("type").asText(), "array");
    assertEquals(type.get(1).get("items").get("type").asText(), "string");
    assertEquals(type.get(1).get("items").get("logicalType").asText(), "uuid");
  }

  @Test
  public void testValueOfUuidStringLogicalType() {
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\","
        + "\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}]}");
    assertEquals(AvroSchemaUtil.valueOf(schema.getField("id").schema()), DataType.UUID,
        "STRING logicalType:uuid should map to UUID");
  }

  @Test
  public void testValueOfUuidFixed16LogicalType() {
    // FIXED(16) + logicalType:uuid — produced by Confluent fixed-uuid mode and Parquet uuid
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\","
        + "\"type\":{\"type\":\"fixed\",\"name\":\"uuid_fixed\",\"size\":16,\"logicalType\":\"uuid\"}}]}");
    assertEquals(AvroSchemaUtil.valueOf(schema.getField("id").schema()), DataType.UUID,
        "FIXED(16) logicalType:uuid should map to UUID");
  }

  @Test
  public void testValueOfFixed16WithoutLogicalTypeIsBytes() {
    // FIXED(16) without logicalType should stay as BYTES
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"raw\","
        + "\"type\":{\"type\":\"fixed\",\"name\":\"raw16\",\"size\":16}}]}");
    assertEquals(AvroSchemaUtil.valueOf(schema.getField("raw").schema()), DataType.BYTES,
        "FIXED(16) without logicalType:uuid should stay BYTES");
  }

  @Test
  public void testValueOfFixedWrongSizeWithUuidLogicalTypeIsBytes() {
    // FIXED of non-16 size with logicalType:uuid should not map to UUID
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\","
        + "\"type\":{\"type\":\"fixed\",\"name\":\"uuid32\",\"size\":32,\"logicalType\":\"uuid\"}}]}");
    assertEquals(AvroSchemaUtil.valueOf(schema.getField("id").schema()), DataType.BYTES,
        "FIXED(32) with logicalType:uuid should stay BYTES");
  }

  private static void assertPrimitiveType(DataType dataType, String expectedAvroType) {
    JsonNode type = typeOf(dataType);
    assertEquals(type.get(0).asText(), "null");
    assertEquals(type.get(1).asText(), expectedAvroType);
  }

  private static JsonNode typeOf(DataType dataType) {
    ObjectNode jsonSchema = AvroSchemaUtil.toAvroSchemaJsonObject(new DimensionFieldSpec("col", dataType, true));
    assertEquals(jsonSchema.get("name").asText(), "col");
    return jsonSchema.get("type");
  }
}
