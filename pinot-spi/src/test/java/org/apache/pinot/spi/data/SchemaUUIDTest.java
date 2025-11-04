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

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class SchemaUUIDTest {

  @Test
  public void testUUIDSchemaCreationAndSerialization()
      throws Exception {
    // Create a schema with UUID fields
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("uuidTestSchema")
        .addSingleValueDimension("userId", FieldSpec.DataType.UUID)
        .addSingleValueDimension("sessionId", FieldSpec.DataType.UUID)
        .addSingleValueDimension("eventType", FieldSpec.DataType.STRING)
        .addMetric("eventCount", FieldSpec.DataType.INT)
        .addDateTime("timestamp", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    // Verify schema was created successfully
    assertNotNull(schema);
    assertEquals(schema.getSchemaName(), "uuidTestSchema");

    // Verify dimension fields
    DimensionFieldSpec userIdSpec = schema.getDimensionSpec("userId");
    assertNotNull(userIdSpec);
    assertEquals(userIdSpec.getDataType(), FieldSpec.DataType.UUID);
    assertTrue(userIdSpec.isSingleValueField());

    DimensionFieldSpec sessionIdSpec = schema.getDimensionSpec("sessionId");
    assertNotNull(sessionIdSpec);
    assertEquals(sessionIdSpec.getDataType(), FieldSpec.DataType.UUID);

    // Test schema serialization to JSON
    String schemaJson = schema.toSingleLineJsonString();
    assertNotNull(schemaJson);
    assertTrue(schemaJson.contains("UUID"));
    assertTrue(schemaJson.contains("userId"));
    assertTrue(schemaJson.contains("sessionId"));

    // Test schema deserialization from JSON
    Schema deserializedSchema = Schema.fromString(schemaJson);
    assertNotNull(deserializedSchema);
    assertEquals(deserializedSchema.getSchemaName(), "uuidTestSchema");

    // Verify deserialized UUID fields
    DimensionFieldSpec deserializedUserIdSpec = deserializedSchema.getDimensionSpec("userId");
    assertNotNull(deserializedUserIdSpec);
    assertEquals(deserializedUserIdSpec.getDataType(), FieldSpec.DataType.UUID);
    assertTrue(deserializedUserIdSpec.isSingleValueField());

    // Test round-trip serialization
    String reserializedJson = deserializedSchema.toSingleLineJsonString();
    assertEquals(reserializedJson, schemaJson);
  }

  @Test
  public void testMultiValueUUIDField()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("mvUuidSchema")
        .addMultiValueDimension("userIds", FieldSpec.DataType.UUID)
        .addSingleValueDimension("eventType", FieldSpec.DataType.STRING)
        .build();

    assertNotNull(schema);

    DimensionFieldSpec userIdsSpec = schema.getDimensionSpec("userIds");
    assertNotNull(userIdsSpec);
    assertEquals(userIdsSpec.getDataType(), FieldSpec.DataType.UUID);
    assertTrue(!userIdsSpec.isSingleValueField());

    // Test serialization round-trip
    String schemaJson = schema.toSingleLineJsonString();
    Schema deserializedSchema = Schema.fromString(schemaJson);

    DimensionFieldSpec deserializedSpec = deserializedSchema.getDimensionSpec("userIds");
    assertNotNull(deserializedSpec);
    assertEquals(deserializedSpec.getDataType(), FieldSpec.DataType.UUID);
    assertTrue(!deserializedSpec.isSingleValueField());
  }

  @Test
  public void testUUIDFieldToJsonObject() {
    DimensionFieldSpec uuidField = new DimensionFieldSpec("testUuid", FieldSpec.DataType.UUID, true);

    JsonNode jsonObject = uuidField.toJsonObject();
    assertNotNull(jsonObject);
    assertEquals(jsonObject.get("name").asText(), "testUuid");
    assertEquals(jsonObject.get("dataType").asText(), "UUID");
  }

  @Test
  public void testUUIDValidation() {
    // UUID should be valid for DIMENSION fields
    Schema.validate(FieldSpec.FieldType.DIMENSION, FieldSpec.DataType.UUID);

    // UUID should be valid for TIME fields
    Schema.validate(FieldSpec.FieldType.TIME, FieldSpec.DataType.UUID);

    // UUID should be valid for DATE_TIME fields
    Schema.validate(FieldSpec.FieldType.DATE_TIME, FieldSpec.DataType.UUID);

    // UUID should be valid for METRIC fields
    Schema.validate(FieldSpec.FieldType.METRIC, FieldSpec.DataType.UUID);
  }

  @Test
  public void testComplexSchemaWithUUID()
      throws Exception {
    // Create a more complex schema with various field types including UUID
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("complexUuidSchema")
        .addSingleValueDimension("userId", FieldSpec.DataType.UUID)
        .addSingleValueDimension("userName", FieldSpec.DataType.STRING)
        .addSingleValueDimension("age", FieldSpec.DataType.INT)
        .addMultiValueDimension("deviceIds", FieldSpec.DataType.UUID)
        .addMetric("revenue", FieldSpec.DataType.DOUBLE)
        .addMetric("transactionId", FieldSpec.DataType.UUID)
        .addDateTime("timestamp", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(java.util.Arrays.asList("userId", "timestamp"))
        .build();

    assertNotNull(schema);

    // Test JSON serialization
    String schemaJson = schema.toSingleLineJsonString();
    assertNotNull(schemaJson);

    // Test deserialization
    Schema deserializedSchema = Schema.fromString(schemaJson);
    assertNotNull(deserializedSchema);

    // Verify all UUID fields
    assertEquals(deserializedSchema.getDimensionSpec("userId").getDataType(), FieldSpec.DataType.UUID);
    assertEquals(deserializedSchema.getDimensionSpec("deviceIds").getDataType(), FieldSpec.DataType.UUID);
    assertEquals(deserializedSchema.getMetricSpec("transactionId").getDataType(), FieldSpec.DataType.UUID);

    // Verify primary keys
    assertEquals(deserializedSchema.getPrimaryKeyColumns().size(), 2);
    assertTrue(deserializedSchema.getPrimaryKeyColumns().contains("userId"));
    assertTrue(deserializedSchema.getPrimaryKeyColumns().contains("timestamp"));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testUUIDNotSupportedForComplex() {
    // UUID should not be valid for COMPLEX fields
    Schema.validate(FieldSpec.FieldType.COMPLEX, FieldSpec.DataType.UUID);
  }

  @Test
  public void testUUIDFieldWithDefaultNullValue() {
    DimensionFieldSpec uuidField = new DimensionFieldSpec("testUuid", FieldSpec.DataType.UUID, true);

    // Get default null value
    Object defaultNullValue = uuidField.getDefaultNullValue();
    assertNotNull(defaultNullValue);
    assertTrue(defaultNullValue instanceof byte[]);
    assertEquals(((byte[]) defaultNullValue).length, 0);

    // Test JSON serialization with default null value
    JsonNode jsonObject = uuidField.toJsonObject();
    assertNotNull(jsonObject);
  }
}
