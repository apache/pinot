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

  /// A UUID column is represented faithfully as an Avro string annotated with logicalType "uuid".
  /// `DataGenerator#buildSpec` always marks the recommender schema FieldSpec single-value, so this emits a scalar
  /// union like every sibling type.
  @Test
  public void testToAvroSchemaJsonObjectForUuid() {
    JsonNode type = typeOf(DataType.UUID);
    assertEquals(type.get(0).asText(), "null");
    JsonNode uuidBranch = type.get(1);
    assertEquals(uuidBranch.get("type").asText(), "string");
    assertEquals(uuidBranch.get("logicalType").asText(), "uuid");
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
