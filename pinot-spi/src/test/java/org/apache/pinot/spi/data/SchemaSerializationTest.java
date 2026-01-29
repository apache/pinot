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
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


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
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addMetric("metric1", FieldSpec.DataType.LONG)
        .build();

    // Serialize using Jackson (which should use @JsonValue -> toJsonObject())
    final String jsonString = JsonUtils.objectToString(schema);

    // Verify that defaultNullValueString is NOT present in the output
    Assert.assertFalse(jsonString.contains("defaultNullValueString"),
        "defaultNullValueString should not be present in serialized output");

    // Verify it can be deserialized back
    final Schema deserializedSchema = Schema.fromString(jsonString);
    Assert.assertEquals(deserializedSchema.getSchemaName(), "testSchema");
    Assert.assertNotNull(deserializedSchema.getDimensionSpec("dim1"));
    Assert.assertNotNull(deserializedSchema.getMetricSpec("metric1"));
  }

  /**
   * Tests that Jackson serialization omits default values for fields.
   */
  @Test
  public void testJsonValueSerializationOmitsDefaultValues()
      throws Exception {
    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING) // default null value = "null"
        .addMetric("metric1", FieldSpec.DataType.DOUBLE) // default null value = 0.0
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Check dimension field spec - should not have defaultNullValue since "null" is the default
    final JsonNode dimSpecs = jsonNode.get("dimensionFieldSpecs");
    Assert.assertNotNull(dimSpecs);
    Assert.assertEquals(dimSpecs.size(), 1);
    final JsonNode dimSpec = dimSpecs.get(0);
    Assert.assertFalse(dimSpec.has("defaultNullValue"),
        "defaultNullValue should not be present for STRING dimension with default value");
    Assert.assertFalse(dimSpec.has("notNull"),
        "notNull should not be present when false (default)");
    Assert.assertFalse(dimSpec.has("singleValueField"),
        "singleValueField should not be present when true (default)");
    Assert.assertFalse(dimSpec.has("allowTrailingZeros"),
        "allowTrailingZeros should not be present when false (default)");

    // Check metric field spec - should not have defaultNullValue since 0.0 is the default
    final JsonNode metricSpecs = jsonNode.get("metricFieldSpecs");
    Assert.assertNotNull(metricSpecs);
    Assert.assertEquals(metricSpecs.size(), 1);
    final JsonNode metricSpec = metricSpecs.get(0);
    Assert.assertFalse(metricSpec.has("defaultNullValue"),
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
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING, "custom_default")
        .addMetric("metric1", FieldSpec.DataType.DOUBLE, 99.9)
        .addMultiValueDimension("mvDim", FieldSpec.DataType.INT)
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

    Assert.assertNotNull(dim1);
    Assert.assertTrue(dim1.has("defaultNullValue"),
        "defaultNullValue should be present for non-default value");
    Assert.assertEquals(dim1.get("defaultNullValue").asText(), "custom_default");

    // Check multi-value dimension has singleValueField: false
    Assert.assertNotNull(mvDim);
    Assert.assertTrue(mvDim.has("singleValueField"),
        "singleValueField should be present when false (non-default)");
    Assert.assertFalse(mvDim.get("singleValueField").asBoolean());

    // Check metric with custom default
    final JsonNode metricSpecs = jsonNode.get("metricFieldSpecs");
    Assert.assertNotNull(metricSpecs);
    final JsonNode metric1 = metricSpecs.get(0);
    Assert.assertTrue(metric1.has("defaultNullValue"),
        "defaultNullValue should be present for non-default value");
    Assert.assertEquals(metric1.get("defaultNullValue").asDouble(), 99.9);
  }

  /**
   * Tests that empty field spec arrays are omitted from serialization.
   */
  @Test
  public void testJsonValueSerializationOmitsEmptyArrays()
      throws Exception {
    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Should have dimensionFieldSpecs
    Assert.assertTrue(jsonNode.has("dimensionFieldSpecs"));

    // Should NOT have empty metricFieldSpecs, dateTimeFieldSpecs, complexFieldSpecs
    Assert.assertFalse(jsonNode.has("metricFieldSpecs"),
        "Empty metricFieldSpecs should be omitted");
    Assert.assertFalse(jsonNode.has("dateTimeFieldSpecs"),
        "Empty dateTimeFieldSpecs should be omitted");
    Assert.assertFalse(jsonNode.has("complexFieldSpecs"),
        "Empty complexFieldSpecs should be omitted");
    Assert.assertFalse(jsonNode.has("timeFieldSpec"),
        "Null timeFieldSpec should be omitted");
    Assert.assertFalse(jsonNode.has("primaryKeyColumns"),
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
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .build();

    final String jsonStringDefault = JsonUtils.objectToString(schemaWithDefault);
    final JsonNode jsonNodeDefault = JsonUtils.stringToJsonNode(jsonStringDefault);

    Assert.assertTrue(jsonNodeDefault.has("enableColumnBasedNullHandling"),
        "enableColumnBasedNullHandling should always be present");
    Assert.assertFalse(jsonNodeDefault.get("enableColumnBasedNullHandling").asBoolean());

    // Test with non-default value (true)
    final Schema schemaWithEnabled = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .setEnableColumnBasedNullHandling(true)
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .build();

    final String jsonStringEnabled = JsonUtils.objectToString(schemaWithEnabled);
    final JsonNode jsonNodeEnabled = JsonUtils.stringToJsonNode(jsonStringEnabled);

    Assert.assertTrue(jsonNodeEnabled.has("enableColumnBasedNullHandling"));
    Assert.assertTrue(jsonNodeEnabled.get("enableColumnBasedNullHandling").asBoolean());
  }

  /**
   * Tests that ComplexFieldSpec with MAP type serializes correctly.
   */
  @Test
  public void testJsonValueSerializationWithComplexFieldSpecMap()
      throws Exception {
    final ComplexFieldSpec mapField = new ComplexFieldSpec("mapField", FieldSpec.DataType.MAP, true, Map.of());

    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addField(mapField)
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Should have complexFieldSpecs
    Assert.assertTrue(jsonNode.has("complexFieldSpecs"));
    final JsonNode complexSpecs = jsonNode.get("complexFieldSpecs");
    Assert.assertEquals(complexSpecs.size(), 1);

    final JsonNode mapSpec = complexSpecs.get(0);
    Assert.assertEquals(mapSpec.get("name").asText(), "mapField");
    Assert.assertEquals(mapSpec.get("dataType").asText(), "MAP");
    Assert.assertEquals(mapSpec.get("fieldType").asText(), "COMPLEX");

    // defaultNullValue should be omitted since empty Map is the default
    Assert.assertFalse(mapSpec.has("defaultNullValue"),
        "Empty Map default should not be serialized");

    // Verify round-trip
    final Schema deserializedSchema = Schema.fromString(jsonString);
    Assert.assertNotNull(deserializedSchema.getFieldSpecFor("mapField"));
    Assert.assertEquals(deserializedSchema.getFieldSpecFor("mapField").getDataType(), FieldSpec.DataType.MAP);
  }

  /**
   * Tests that ComplexFieldSpec with LIST type serializes correctly.
   */
  @Test
  public void testJsonValueSerializationWithComplexFieldSpecList()
      throws Exception {
    final ComplexFieldSpec listField = new ComplexFieldSpec("listField", FieldSpec.DataType.LIST, true, Map.of());

    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addField(listField)
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Should have complexFieldSpecs
    Assert.assertTrue(jsonNode.has("complexFieldSpecs"));
    final JsonNode complexSpecs = jsonNode.get("complexFieldSpecs");
    Assert.assertEquals(complexSpecs.size(), 1);

    final JsonNode listSpec = complexSpecs.get(0);
    Assert.assertEquals(listSpec.get("name").asText(), "listField");
    Assert.assertEquals(listSpec.get("dataType").asText(), "LIST");

    // Verify round-trip
    final Schema deserializedSchema = Schema.fromString(jsonString);
    Assert.assertNotNull(deserializedSchema.getFieldSpecFor("listField"));
    Assert.assertEquals(deserializedSchema.getFieldSpecFor("listField").getDataType(), FieldSpec.DataType.LIST);
  }

  /**
   * Tests that DateTimeFieldSpec serializes correctly with format and granularity.
   */
  @Test
  public void testJsonValueSerializationWithDateTimeFieldSpec()
      throws Exception {
    final Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addDateTime("timestamp", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    final String jsonString = JsonUtils.objectToString(schema);
    final JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    Assert.assertTrue(jsonNode.has("dateTimeFieldSpecs"));
    final JsonNode dateTimeSpecs = jsonNode.get("dateTimeFieldSpecs");
    Assert.assertEquals(dateTimeSpecs.size(), 1);

    final JsonNode dtSpec = dateTimeSpecs.get(0);
    Assert.assertEquals(dtSpec.get("name").asText(), "timestamp");
    Assert.assertEquals(dtSpec.get("dataType").asText(), "LONG");
    Assert.assertEquals(dtSpec.get("format").asText(), "1:MILLISECONDS:EPOCH");
    Assert.assertEquals(dtSpec.get("granularity").asText(), "1:MILLISECONDS");

    // defaultNullValue should be omitted since Long.MIN_VALUE is the default for DATE_TIME LONG
    Assert.assertFalse(dtSpec.has("defaultNullValue"),
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
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("dim2", FieldSpec.DataType.INT, 42)
        .addMultiValueDimension("mvDim", FieldSpec.DataType.DOUBLE)
        .addMetric("metric1", FieldSpec.DataType.LONG)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(Lists.newArrayList("dim1"))
        .build();

    // Get JSON from Jackson serialization
    final String jacksonJson = JsonUtils.objectToString(schema);
    final JsonNode jacksonNode = JsonUtils.stringToJsonNode(jacksonJson);

    // Get JSON from toJsonObject()
    final JsonNode toJsonObjectNode = schema.toJsonObject();

    // They should be equal
    Assert.assertEquals(jacksonNode, toJsonObjectNode,
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
        .addSingleValueDimension("stringDim", FieldSpec.DataType.STRING)
        .addSingleValueDimension("intDimWithDefault", FieldSpec.DataType.INT, 100)
        .addMultiValueDimension("mvStringDim", FieldSpec.DataType.STRING, "default")
        .addMetric("longMetric", FieldSpec.DataType.LONG)
        .addMetric("doubleMetricWithDefault", FieldSpec.DataType.DOUBLE, 3.14)
        .addDateTime("eventTime", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addDateTime("dayTime", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS")
        .setPrimaryKeyColumns(Lists.newArrayList("stringDim", "eventTime"))
        .build();

    // Serialize with Jackson
    final String jsonString = JsonUtils.objectToString(originalSchema);

    // Deserialize
    final Schema deserializedSchema = Schema.fromString(jsonString);

    // Verify all fields
    Assert.assertEquals(deserializedSchema.getSchemaName(), "complexTestSchema");
    Assert.assertTrue(deserializedSchema.isEnableColumnBasedNullHandling());

    // Verify dimensions
    Assert.assertNotNull(deserializedSchema.getDimensionSpec("stringDim"));
    Assert.assertEquals(deserializedSchema.getDimensionSpec("intDimWithDefault").getDefaultNullValue(), 100);
    Assert.assertFalse(deserializedSchema.getDimensionSpec("mvStringDim").isSingleValueField());

    // Verify metrics
    Assert.assertNotNull(deserializedSchema.getMetricSpec("longMetric"));
    Assert.assertEquals(deserializedSchema.getMetricSpec("doubleMetricWithDefault").getDefaultNullValue(), 3.14);

    // Verify date time
    Assert.assertNotNull(deserializedSchema.getDateTimeSpec("eventTime"));
    Assert.assertEquals(deserializedSchema.getDateTimeSpec("eventTime").getFormat(), "1:MILLISECONDS:EPOCH");

    // Verify primary keys
    Assert.assertEquals(deserializedSchema.getPrimaryKeyColumns(), Lists.newArrayList("stringDim", "eventTime"));
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
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addMetric("metric1", FieldSpec.DataType.LONG)
        .build();

    // Use a fresh ObjectMapper (simulates different Jackson configurations)
    final ObjectMapper freshMapper = new ObjectMapper();
    final String freshMapperJson = freshMapper.writeValueAsString(schema);

    // Should still produce toJsonObject() format
    Assert.assertFalse(freshMapperJson.contains("defaultNullValueString"),
        "Fresh ObjectMapper should also use @JsonValue and omit defaultNullValueString");

    // Should be deserializable
    final Schema deserializedSchema = Schema.fromString(freshMapperJson);
    Assert.assertEquals(deserializedSchema.getSchemaName(), "testSchema");
  }
}
