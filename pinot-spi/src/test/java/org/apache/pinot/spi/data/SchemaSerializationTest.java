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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for Schema serialization with @JsonValue annotation.
 * These tests verify that Jackson serialization uses the toJsonObject() method
 * which produces a minimal, canonical JSON format.
 */
public class SchemaSerializationTest {

  /**
   * Tests that Jackson serialization uses toJsonObject() format via @JsonValue annotation.
   * This ensures that defaultNullValueString is never included in serialized output.
   */
  @Test
  public void testJsonValueSerializationOmitsDefaultNullValueString()
      throws Exception {
    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dim1", DataType.STRING)
        .addMetric("metric1", DataType.LONG)
        .build();

    // Serialize using Jackson (which should use @JsonValue -> toJsonObject())
    final String jsonString = JsonUtils.objectToString(schema);

    // Verify that defaultNullValueString is NOT present in the output
    assertFalse(jsonString.contains("defaultNullValueString"),
        "defaultNullValueString should not be present in serialized output");

    // Verify it can be deserialized back
    final Schema deserializedSchema = Schema.fromString(jsonString);
    assertEquals(deserializedSchema.getSchemaName(), "testSchema");
    assertNotNull(deserializedSchema.getDimensionSpec("dim1"));
    assertNotNull(deserializedSchema.getMetricSpec("metric1"));
  }

  /**
   * Tests that Jackson serialization omits default values for fields.
   */
  @Test
  public void testJsonValueSerializationOmitsDefaultValues()
      throws Exception {
    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dim1", DataType.STRING) // default null value = "null"
        .addMetric("metric1", DataType.DOUBLE) // default null value = 0.0
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Check dimension field spec - should not have defaultNullValue since "null" is the default
    final JsonNode dimSpecs = jsonNode.get("dimensionFieldSpecs");
    assertNotNull(dimSpecs);
    assertEquals(dimSpecs.size(), 1);
    final JsonNode dimSpec = dimSpecs.get(0);
    assertFalse(dimSpec.has("defaultNullValue"),
        "defaultNullValue should not be present for STRING dimension with default value");
    assertFalse(dimSpec.has("notNull"),
        "notNull should not be present when false (default)");
    assertFalse(dimSpec.has("singleValueField"),
        "singleValueField should not be present when true (default)");
    assertFalse(dimSpec.has("allowTrailingZeros"),
        "allowTrailingZeros should not be present when false (default)");

    // Check metric field spec - should not have defaultNullValue since 0.0 is the default
    final JsonNode metricSpecs = jsonNode.get("metricFieldSpecs");
    assertNotNull(metricSpecs);
    assertEquals(metricSpecs.size(), 1);
    final JsonNode metricSpec = metricSpecs.get(0);
    assertFalse(metricSpec.has("defaultNullValue"),
        "defaultNullValue should not be present for DOUBLE metric with default value");
  }

  /**
   * Tests that Jackson serialization includes non-default values.
   */
  @Test
  public void testJsonValueSerializationIncludesNonDefaultValues()
      throws Exception {
    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dim1", DataType.STRING, "custom_default")
        .addMetric("metric1", DataType.DOUBLE, 99.9)
        .addMultiValueDimension("mvDim", DataType.INT)
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Check dimension with custom default
    final JsonNode dimSpecs = jsonNode.get("dimensionFieldSpecs");
    JsonNode dim1 = null;
    JsonNode mvDim = null;
    for (int i = 0; i < dimSpecs.size(); i++) {
      final JsonNode spec = dimSpecs.get(i);
      if ("dim1".equals(spec.get("name").asText())) {
        dim1 = spec;
      } else if ("mvDim".equals(spec.get("name").asText())) {
        mvDim = spec;
      }
    }

    assertNotNull(dim1);
    assertTrue(dim1.has("defaultNullValue"),
        "defaultNullValue should be present for non-default value");
    assertEquals(dim1.get("defaultNullValue").asText(), "custom_default");

    // Check multi-value dimension has singleValueField: false
    assertNotNull(mvDim);
    assertTrue(mvDim.has("singleValueField"),
        "singleValueField should be present when false (non-default)");
    assertFalse(mvDim.get("singleValueField").asBoolean());

    // Check metric with custom default
    final JsonNode metricSpecs = jsonNode.get("metricFieldSpecs");
    assertNotNull(metricSpecs);
    final JsonNode metric1 = metricSpecs.get(0);
    assertTrue(metric1.has("defaultNullValue"),
        "defaultNullValue should be present for non-default value");
    assertEquals(metric1.get("defaultNullValue").asDouble(), 99.9);
  }

  /**
   * Tests that empty field spec arrays are omitted from serialization.
   */
  @Test
  public void testJsonValueSerializationOmitsEmptyArrays()
      throws Exception {
    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dim1", DataType.STRING)
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Should have dimensionFieldSpecs
    assertTrue(jsonNode.has("dimensionFieldSpecs"));

    // Should NOT have empty metricFieldSpecs, dateTimeFieldSpecs, complexFieldSpecs
    assertFalse(jsonNode.has("metricFieldSpecs"),
        "Empty metricFieldSpecs should be omitted");
    assertFalse(jsonNode.has("dateTimeFieldSpecs"),
        "Empty dateTimeFieldSpecs should be omitted");
    assertFalse(jsonNode.has("complexFieldSpecs"),
        "Empty complexFieldSpecs should be omitted");
    assertFalse(jsonNode.has("timeFieldSpec"),
        "Null timeFieldSpec should be omitted");
    assertFalse(jsonNode.has("primaryKeyColumns"),
        "Empty primaryKeyColumns should be omitted");
  }

  /**
   * Tests that enableColumnBasedNullHandling is always included in serialization.
   */
  @Test
  public void testJsonValueSerializationAlwaysIncludesEnableColumnBasedNullHandling()
      throws Exception {
    // Test with default value (false)
    final Schema schemaWithDefault = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dim1", DataType.STRING)
        .build();

    final String jsonStringDefault = JsonUtils.objectToString(schemaWithDefault);
    final JsonNode jsonNodeDefault = JsonUtils.stringToJsonNode(jsonStringDefault);

    assertTrue(jsonNodeDefault.has("enableColumnBasedNullHandling"),
        "enableColumnBasedNullHandling should always be present");
    assertFalse(jsonNodeDefault.get("enableColumnBasedNullHandling").asBoolean());

    // Test with non-default value (true)
    final Schema schemaWithEnabled = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .setEnableColumnBasedNullHandling(true)
        .addSingleValueDimension("dim1", DataType.STRING)
        .build();

    final String jsonStringEnabled = JsonUtils.objectToString(schemaWithEnabled);
    final JsonNode jsonNodeEnabled = JsonUtils.stringToJsonNode(jsonStringEnabled);

    assertTrue(jsonNodeEnabled.has("enableColumnBasedNullHandling"));
    assertTrue(jsonNodeEnabled.get("enableColumnBasedNullHandling").asBoolean());
  }

  /**
   * Tests that ComplexFieldSpec with MAP type serializes correctly.
   */
  @Test
  public void testJsonValueSerializationWithComplexFieldSpecMap()
      throws Exception {
    final ComplexFieldSpec mapField = new ComplexFieldSpec("mapField", DataType.MAP, true, Map.of());

    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addField(mapField)
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Should have complexFieldSpecs
    assertTrue(jsonNode.has("complexFieldSpecs"));
    final JsonNode complexSpecs = jsonNode.get("complexFieldSpecs");
    assertEquals(complexSpecs.size(), 1);

    final JsonNode mapSpec = complexSpecs.get(0);
    assertEquals(mapSpec.get("name").asText(), "mapField");
    assertEquals(mapSpec.get("dataType").asText(), "MAP");
    assertEquals(mapSpec.get("fieldType").asText(), "COMPLEX");

    // defaultNullValue should be omitted since empty Map is the default
    assertFalse(mapSpec.has("defaultNullValue"),
        "Empty Map default should not be serialized");

    // Verify round-trip
    final Schema deserializedSchema = Schema.fromString(jsonString);
    assertNotNull(deserializedSchema.getFieldSpecFor("mapField"));
    assertEquals(deserializedSchema.getFieldSpecFor("mapField").getDataType(), DataType.MAP);
  }

  /**
   * Tests that ComplexFieldSpec with LIST type serializes correctly.
   */
  @Test
  public void testJsonValueSerializationWithComplexFieldSpecList()
      throws Exception {
    final ComplexFieldSpec listField = new ComplexFieldSpec("listField", DataType.LIST, true, Map.of());

    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addField(listField)
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Should have complexFieldSpecs
    assertTrue(jsonNode.has("complexFieldSpecs"));
    final JsonNode complexSpecs = jsonNode.get("complexFieldSpecs");
    assertEquals(complexSpecs.size(), 1);

    final JsonNode listSpec = complexSpecs.get(0);
    assertEquals(listSpec.get("name").asText(), "listField");
    assertEquals(listSpec.get("dataType").asText(), "LIST");

    // Verify round-trip
    final Schema deserializedSchema = Schema.fromString(jsonString);
    assertNotNull(deserializedSchema.getFieldSpecFor("listField"));
    assertEquals(deserializedSchema.getFieldSpecFor("listField").getDataType(), DataType.LIST);
  }

  /**
   * Tests that DateTimeFieldSpec serializes correctly with format and granularity.
   */
  @Test
  public void testJsonValueSerializationWithDateTimeFieldSpec()
      throws Exception {
    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addDateTime("timestamp", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    assertTrue(jsonNode.has("dateTimeFieldSpecs"));
    final JsonNode dateTimeSpecs = jsonNode.get("dateTimeFieldSpecs");
    assertEquals(dateTimeSpecs.size(), 1);

    final JsonNode dtSpec = dateTimeSpecs.get(0);
    assertEquals(dtSpec.get("name").asText(), "timestamp");
    assertEquals(dtSpec.get("dataType").asText(), "LONG");
    assertEquals(dtSpec.get("format").asText(), "1:MILLISECONDS:EPOCH");
    assertEquals(dtSpec.get("granularity").asText(), "1:MILLISECONDS");

    // defaultNullValue should be omitted since Long.MIN_VALUE is the default for DATE_TIME LONG
    assertFalse(dtSpec.has("defaultNullValue"),
        "Default null value should not be serialized for DATE_TIME LONG");
  }

  /**
   * Tests that Jackson serialization output matches toJsonObject() output.
   */
  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .setEnableColumnBasedNullHandling(true)
        .addSingleValueDimension("dim1", DataType.STRING)
        .addSingleValueDimension("dim2", DataType.INT, 42)
        .addMultiValueDimension("mvDim", DataType.DOUBLE)
        .addMetric("metric1", DataType.LONG)
        .addDateTime("ts", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(Lists.newArrayList("dim1"))
        .build();

    // Get JSON from Jackson serialization
    final String jacksonJson = JsonUtils.objectToString(schema);
    final JsonNode jacksonNode = JsonUtils.stringToJsonNode(jacksonJson);

    // Get JSON from toJsonObject()
    final JsonNode toJsonObjectNode = schema.toJsonObject();

    // They should be equal
    assertEquals(jacksonNode, toJsonObjectNode,
        "Jackson serialization should match toJsonObject() output");
  }

  /**
   * Tests round-trip serialization/deserialization with a complex schema.
   */
  @Test
  public void testRoundTripSerializationWithComplexSchema()
      throws Exception {
    final Schema originalSchema = new Schema.SchemaBuilder()
        .setSchemaName("complexTestSchema")
        .setEnableColumnBasedNullHandling(true)
        .addSingleValueDimension("stringDim", DataType.STRING)
        .addSingleValueDimension("intDimWithDefault", DataType.INT, 100)
        .addMultiValueDimension("mvStringDim", DataType.STRING, "default")
        .addMetric("longMetric", DataType.LONG)
        .addMetric("doubleMetricWithDefault", DataType.DOUBLE, 3.14)
        .addDateTime("eventTime", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addDateTime("dayTime", DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS")
        .setPrimaryKeyColumns(Lists.newArrayList("stringDim", "eventTime"))
        .build();

    // Serialize with Jackson
    final String jsonString = JsonUtils.objectToString(originalSchema);

    // Deserialize
    final Schema deserializedSchema = Schema.fromString(jsonString);

    // Verify all fields
    assertEquals(deserializedSchema.getSchemaName(), "complexTestSchema");
    assertTrue(deserializedSchema.isEnableColumnBasedNullHandling());

    // Verify dimensions
    assertNotNull(deserializedSchema.getDimensionSpec("stringDim"));
    assertEquals(deserializedSchema.getDimensionSpec("intDimWithDefault").getDefaultNullValue(), 100);
    assertFalse(deserializedSchema.getDimensionSpec("mvStringDim").isSingleValueField());

    // Verify metrics
    assertNotNull(deserializedSchema.getMetricSpec("longMetric"));
    assertEquals(deserializedSchema.getMetricSpec("doubleMetricWithDefault").getDefaultNullValue(), 3.14);

    // Verify date time
    assertNotNull(deserializedSchema.getDateTimeSpec("eventTime"));
    assertEquals(deserializedSchema.getDateTimeSpec("eventTime").getFormat(), "1:MILLISECONDS:EPOCH");

    // Verify primary keys
    assertEquals(deserializedSchema.getPrimaryKeyColumns(), Lists.newArrayList("stringDim", "eventTime"));
  }

  /**
   * Tests that a fresh ObjectMapper produces the same serialization as JsonUtils.
   * This verifies that @JsonValue annotation works with any ObjectMapper, not just JsonUtils.
   */
  @Test
  public void testJsonValueWorksWithFreshObjectMapper()
      throws Exception {
    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dim1", DataType.STRING)
        .addMetric("metric1", DataType.LONG)
        .build();

    // Use a fresh ObjectMapper (simulates different Jackson configurations)
    final ObjectMapper freshMapper = new ObjectMapper();
    final String freshMapperJson = freshMapper.writeValueAsString(schema);

    // Should still produce toJsonObject() format
    assertFalse(freshMapperJson.contains("defaultNullValueString"),
        "Fresh ObjectMapper should also use @JsonValue and omit defaultNullValueString");

    // Should be deserializable
    final Schema deserializedSchema = Schema.fromString(freshMapperJson);
    assertEquals(deserializedSchema.getSchemaName(), "testSchema");
  }

  @Test
  public void testComplexFieldDefaultNullValue()
      throws Exception {
    // Test LIST
    ComplexFieldSpec listFieldSpec = new ComplexFieldSpec("list", DataType.LIST, true, Map.of());

    // Test no defaultNullValue
    Object defaultNullValue = listFieldSpec.getDefaultNullValue();
    assertTrue(defaultNullValue instanceof List);
    assertEquals(defaultNullValue, List.of());
    ObjectNode jsonObject = listFieldSpec.toJsonObject();
    assertFalse(jsonObject.has("defaultNullValue"));
    ComplexFieldSpec deserialized = JsonUtils.jsonNodeToObject(jsonObject, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
    String serialized = jsonObject.toString();
    assertFalse(serialized.contains("defaultNullValue"));
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);

    // Test null defaultNullValue
    serialized = "{"
        + "\"name\":\"list\","
        + "\"dataType\":\"LIST\","
        + "\"fieldType\":\"COMPLEX\","
        + "\"defaultNullValue\":null,"
        + "\"childFieldSpecs\":{}"
        + "}";
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);

    // Test numeric
    listFieldSpec.setDefaultNullValue(List.of(1, 2, 3));
    defaultNullValue = listFieldSpec.getDefaultNullValue();
    assertTrue(defaultNullValue instanceof List);
    assertEquals(defaultNullValue, List.of(1, 2, 3));
    jsonObject = listFieldSpec.toJsonObject();
    JsonNode defaultNullValueNode = jsonObject.get("defaultNullValue");
    assertTrue(defaultNullValueNode.isArray());
    assertEquals(defaultNullValueNode.toString(), "[1,2,3]");
    deserialized = JsonUtils.jsonNodeToObject(jsonObject, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
    serialized = jsonObject.toString();
    assertTrue(serialized.contains("\"defaultNullValue\":[1,2,3]"));
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
    // Test compatibility with serialized JSON ARRAY
    serialized = "{"
        + "\"name\":\"list\","
        + "\"dataType\":\"LIST\","
        + "\"fieldType\":\"COMPLEX\","
        + "\"defaultNullValue\":\"[1,2,3]\","
        + "\"childFieldSpecs\":{}"
        + "}";
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);

    // Test text
    listFieldSpec.setDefaultNullValue(List.of("a", "b", "c"));
    defaultNullValue = listFieldSpec.getDefaultNullValue();
    assertTrue(defaultNullValue instanceof List);
    assertEquals(defaultNullValue, List.of("a", "b", "c"));
    jsonObject = listFieldSpec.toJsonObject();
    defaultNullValueNode = jsonObject.get("defaultNullValue");
    assertTrue(defaultNullValueNode.isArray());
    assertEquals(defaultNullValueNode.toString(), "[\"a\",\"b\",\"c\"]");
    deserialized = JsonUtils.jsonNodeToObject(jsonObject, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
    serialized = jsonObject.toString();
    assertTrue(serialized.contains("\"defaultNullValue\":[\"a\",\"b\",\"c\"]"));
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
    // Test compatibility with serialized JSON ARRAY
    serialized = "{"
        + "\"name\":\"list\","
        + "\"dataType\":\"LIST\","
        + "\"fieldType\":\"COMPLEX\","
        + "\"defaultNullValue\":\"[\\\"a\\\",\\\"b\\\",\\\"c\\\"]\","
        + "\"childFieldSpecs\":{}"
        + "}";
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);

    // Test MAP
    ComplexFieldSpec mapFieldSpec = new ComplexFieldSpec("map", DataType.MAP, true, Map.of());

    // Test no defaultNullValue
    defaultNullValue = mapFieldSpec.getDefaultNullValue();
    assertTrue(defaultNullValue instanceof Map);
    assertEquals(defaultNullValue, Map.of());
    jsonObject = mapFieldSpec.toJsonObject();
    assertFalse(jsonObject.has("defaultNullValue"));
    deserialized = JsonUtils.jsonNodeToObject(jsonObject, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
    serialized = jsonObject.toString();
    assertFalse(serialized.contains("defaultNullValue"));
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);

    // Test null defaultNullValue
    serialized = "{"
        + "\"name\":\"map\","
        + "\"dataType\":\"MAP\","
        + "\"fieldType\":\"COMPLEX\","
        + "\"defaultNullValue\":null,"
        + "\"childFieldSpecs\":{}"
        + "}";
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);

    // Test numeric
    mapFieldSpec.setDefaultNullValue(Map.of("a", 1, "b", 2));
    defaultNullValue = mapFieldSpec.getDefaultNullValue();
    assertTrue(defaultNullValue instanceof Map);
    assertEquals(defaultNullValue, Map.of("a", 1, "b", 2));
    jsonObject = mapFieldSpec.toJsonObject();
    defaultNullValueNode = jsonObject.get("defaultNullValue");
    assertTrue(defaultNullValueNode.isObject());
    deserialized = JsonUtils.jsonNodeToObject(jsonObject, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
    serialized = jsonObject.toString();
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
    // Test compatibility with serialized JSON OBJECT
    serialized = "{"
        + "\"name\":\"map\","
        + "\"dataType\":\"MAP\","
        + "\"fieldType\":\"COMPLEX\","
        + "\"defaultNullValue\":\"{\\\"a\\\":1,\\\"b\\\":2}\","
        + "\"childFieldSpecs\":{}"
        + "}";
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);

    // Test text
    mapFieldSpec.setDefaultNullValue(Map.of("key", "a", "value", "b"));
    defaultNullValue = mapFieldSpec.getDefaultNullValue();
    assertTrue(defaultNullValue instanceof Map);
    assertEquals(defaultNullValue, Map.of("key", "a", "value", "b"));
    jsonObject = mapFieldSpec.toJsonObject();
    defaultNullValueNode = jsonObject.get("defaultNullValue");
    assertTrue(defaultNullValueNode.isObject());
    deserialized = JsonUtils.jsonNodeToObject(jsonObject, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
    serialized = jsonObject.toString();
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
    // Test compatibility with serialized JSON OBJECT
    serialized = "{"
        + "\"name\":\"map\","
        + "\"dataType\":\"MAP\","
        + "\"fieldType\":\"COMPLEX\","
        + "\"defaultNullValue\":\"{\\\"key\\\":\\\"a\\\",\\\"value\\\":\\\"b\\\"}\","
        + "\"childFieldSpecs\":{}"
        + "}";
    deserialized = JsonUtils.stringToObject(serialized, ComplexFieldSpec.class);
    assertEquals(deserialized.getDefaultNullValue(), defaultNullValue);
  }
}
