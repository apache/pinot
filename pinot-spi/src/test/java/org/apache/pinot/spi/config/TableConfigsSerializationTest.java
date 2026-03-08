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
package org.apache.pinot.spi.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for TableConfigs serialization with @JsonValue annotation.
 * These tests verify that Jackson serialization uses the toJsonObject() method
 * which produces a minimal, canonical JSON format.
 */
public class TableConfigsSerializationTest {

  private static final String TEST_TABLE_NAME = "testTable";

  private Schema createTestSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(TEST_TABLE_NAME)
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addMetric("metric1", FieldSpec.DataType.LONG)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  private TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TEST_TABLE_NAME)
        .setNumReplicas(1)
        .build();
  }

  private TableConfig createRealtimeTableConfig() {
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TEST_TABLE_NAME)
        .setNumReplicas(1)
        .setStreamConfigs(java.util.Map.of(
            "streamType", "kafka",
            "stream.kafka.topic.name", "testTopic",
            "stream.kafka.broker.list", "localhost:9092",
            "stream.kafka.decoder.class.name", "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder"
        ))
        .build();
  }

  /**
   * Tests that TableConfigs serialization uses toJsonObject() format via @JsonValue.
   */
  @Test
  public void testTableConfigsSerializationUsesToJsonObject()
      throws Exception {
    final Schema schema = createTestSchema();
    final TableConfig offlineConfig = createOfflineTableConfig();

    final TableConfigs tableConfigs = new TableConfigs(TEST_TABLE_NAME, schema, offlineConfig, null);

    // Serialize using Jackson
    final String jsonString = JsonUtils.objectToString(tableConfigs);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Verify structure
    Assert.assertTrue(jsonNode.has("tableName"));
    Assert.assertEquals(jsonNode.get("tableName").asText(), TEST_TABLE_NAME);

    Assert.assertTrue(jsonNode.has("schema"));
    Assert.assertTrue(jsonNode.has("offline"));
    Assert.assertFalse(jsonNode.has("realtime"), "realtime should not be present when null");
  }

  /**
   * Tests that the embedded Schema in TableConfigs uses toJsonObject() format.
   */
  @Test
  public void testTableConfigsSchemaSerializationFormat()
      throws Exception {
    final Schema schema = createTestSchema();
    final TableConfig offlineConfig = createOfflineTableConfig();

    final TableConfigs tableConfigs = new TableConfigs(TEST_TABLE_NAME, schema, offlineConfig, null);

    final String jsonString = JsonUtils.objectToString(tableConfigs);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Get the schema node
    final JsonNode schemaNode = jsonNode.get("schema");
    Assert.assertNotNull(schemaNode);

    // Verify schema uses toJsonObject() format (no defaultNullValueString)
    final String schemaString = schemaNode.toString();
    Assert.assertFalse(schemaString.contains("defaultNullValueString"),
        "Schema within TableConfigs should not contain defaultNullValueString");

    // Verify schemaName is present
    Assert.assertTrue(schemaNode.has("schemaName"));
    Assert.assertEquals(schemaNode.get("schemaName").asText(), TEST_TABLE_NAME);

    // Verify enableColumnBasedNullHandling is present
    Assert.assertTrue(schemaNode.has("enableColumnBasedNullHandling"));

    // Verify dimensionFieldSpecs uses minimal format
    final JsonNode dimSpecs = schemaNode.get("dimensionFieldSpecs");
    Assert.assertNotNull(dimSpecs);
    Assert.assertEquals(dimSpecs.size(), 1);

    final JsonNode dimSpec = dimSpecs.get(0);
    Assert.assertFalse(dimSpec.has("defaultNullValue"),
        "Default null value should be omitted for STRING dimension");
    Assert.assertFalse(dimSpec.has("notNull"),
        "notNull should be omitted when false (default)");
    Assert.assertFalse(dimSpec.has("singleValueField"),
        "singleValueField should be omitted when true (default)");
  }

  /**
   * Tests that TableConfigs with both offline and realtime configs serializes correctly.
   */
  @Test
  public void testTableConfigsWithBothTableTypes()
      throws Exception {
    final Schema schema = createTestSchema();
    final TableConfig offlineConfig = createOfflineTableConfig();
    final TableConfig realtimeConfig = createRealtimeTableConfig();

    final TableConfigs tableConfigs = new TableConfigs(TEST_TABLE_NAME, schema, offlineConfig, realtimeConfig);

    final String jsonString = JsonUtils.objectToString(tableConfigs);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    Assert.assertTrue(jsonNode.has("tableName"));
    Assert.assertTrue(jsonNode.has("schema"));
    Assert.assertTrue(jsonNode.has("offline"));
    Assert.assertTrue(jsonNode.has("realtime"));

    // Verify offline config
    final JsonNode offlineNode = jsonNode.get("offline");
    Assert.assertEquals(offlineNode.get("tableType").asText(), "OFFLINE");

    // Verify realtime config
    final JsonNode realtimeNode = jsonNode.get("realtime");
    Assert.assertEquals(realtimeNode.get("tableType").asText(), "REALTIME");
  }

  /**
   * Tests that Jackson serialization output matches toJsonObject() output.
   */
  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    final Schema schema = createTestSchema();
    final TableConfig offlineConfig = createOfflineTableConfig();

    final TableConfigs tableConfigs = new TableConfigs(TEST_TABLE_NAME, schema, offlineConfig, null);

    // Get JSON from Jackson serialization
    final String jacksonJson = JsonUtils.objectToString(tableConfigs);
    final JsonNode jacksonNode = JsonUtils.stringToJsonNode(jacksonJson);

    // Get JSON from toJsonObject()
    final JsonNode toJsonObjectNode = tableConfigs.toJsonObject();

    // They should be equal
    Assert.assertEquals(jacksonNode, toJsonObjectNode,
        "Jackson serialization should match toJsonObject() output");
  }

  /**
   * Tests round-trip serialization/deserialization of TableConfigs.
   */
  @Test
  public void testRoundTripSerialization()
      throws Exception {
    final Schema schema = createTestSchema();
    final TableConfig offlineConfig = createOfflineTableConfig();

    final TableConfigs original = new TableConfigs(TEST_TABLE_NAME, schema, offlineConfig, null);

    // Serialize
    final String jsonString = JsonUtils.objectToString(original);

    // Deserialize
    final TableConfigs deserialized = JsonUtils.stringToObject(jsonString, TableConfigs.class);

    // Verify
    Assert.assertEquals(deserialized.getTableName(), TEST_TABLE_NAME);
    Assert.assertNotNull(deserialized.getSchema());
    Assert.assertEquals(deserialized.getSchema().getSchemaName(), TEST_TABLE_NAME);
    Assert.assertNotNull(deserialized.getOffline());
    Assert.assertNull(deserialized.getRealtime());

    // Verify schema fields
    Assert.assertNotNull(deserialized.getSchema().getDimensionSpec("dim1"));
    Assert.assertNotNull(deserialized.getSchema().getMetricSpec("metric1"));
    Assert.assertNotNull(deserialized.getSchema().getDateTimeSpec("ts"));
  }

  /**
   * Tests that a fresh ObjectMapper produces the same serialization.
   * This verifies that @JsonValue works with any ObjectMapper.
   */
  @Test
  public void testJsonValueWorksWithFreshObjectMapper()
      throws Exception {
    final Schema schema = createTestSchema();
    final TableConfig offlineConfig = createOfflineTableConfig();

    final TableConfigs tableConfigs = new TableConfigs(TEST_TABLE_NAME, schema, offlineConfig, null);

    // Use a fresh ObjectMapper
    final ObjectMapper freshMapper = new ObjectMapper();
    final String freshMapperJson = freshMapper.writeValueAsString(tableConfigs);

    // Should produce toJsonObject() format
    Assert.assertFalse(freshMapperJson.contains("defaultNullValueString"),
        "Fresh ObjectMapper should use @JsonValue and not include defaultNullValueString");

    // Verify it's valid JSON and has expected structure
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(freshMapperJson);
    Assert.assertTrue(jsonNode.has("tableName"));
    Assert.assertTrue(jsonNode.has("schema"));
    Assert.assertTrue(jsonNode.has("offline"));
  }

  /**
   * Tests that toJsonString() and Jackson serialization produce the same result.
   */
  @Test
  public void testToJsonStringMatchesJacksonSerialization()
      throws Exception {
    final Schema schema = createTestSchema();
    final TableConfig offlineConfig = createOfflineTableConfig();

    final TableConfigs tableConfigs = new TableConfigs(TEST_TABLE_NAME, schema, offlineConfig, null);

    // Get JSON from toJsonString()
    final String toJsonStringResult = tableConfigs.toJsonString();

    // Get JSON from Jackson
    final String jacksonResult = JsonUtils.objectToString(tableConfigs);

    // Parse both and compare (to handle whitespace differences)
    final JsonNode toJsonStringNode = JsonUtils.stringToJsonNode(toJsonStringResult);
    final JsonNode jacksonNode = JsonUtils.stringToJsonNode(jacksonResult);

    Assert.assertEquals(jacksonNode, toJsonStringNode,
        "toJsonString() and Jackson serialization should produce equivalent JSON");
  }

  /**
   * Tests serialization with realtime-only TableConfigs.
   */
  @Test
  public void testRealtimeOnlyTableConfigs()
      throws Exception {
    final Schema schema = createTestSchema();
    final TableConfig realtimeConfig = createRealtimeTableConfig();

    final TableConfigs tableConfigs = new TableConfigs(TEST_TABLE_NAME, schema, null, realtimeConfig);

    final String jsonString = JsonUtils.objectToString(tableConfigs);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    Assert.assertTrue(jsonNode.has("tableName"));
    Assert.assertTrue(jsonNode.has("schema"));
    Assert.assertFalse(jsonNode.has("offline"), "offline should not be present when null");
    Assert.assertTrue(jsonNode.has("realtime"));
  }
}
