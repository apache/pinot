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

import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link SchemaDeserializer}.
 *
 * These tests verify that Schema deserialization properly handles ComplexFieldSpec
 * with MAP/LIST types, ensuring empty objects {} and arrays [] are deserialized
 * as Java collections rather than Scala collections.
 */
public class SchemaDeserializerTest {

  @Test
  public void testDeserializeSchemaWithMapType() throws Exception {
    final String json = "{"
        + "\"schemaName\": \"testSchema\","
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"mapField\","
        + "  \"dataType\": \"MAP\","
        + "  \"defaultNullValue\": \"{}\""
        + "}]"
        + "}";

    final Schema schema = JsonUtils.stringToObject(json, Schema.class);

    assertNotNull(schema);
    assertEquals(schema.getSchemaName(), "testSchema");
    assertEquals(schema.getComplexFieldSpecs().size(), 1);

    final ComplexFieldSpec fieldSpec = schema.getComplexFieldSpecs().get(0);
    assertEquals(fieldSpec.getName(), "mapField");
    assertEquals(fieldSpec.getDataType(), DataType.MAP);
  }

  @Test
  public void testDeserializeSchemaWithMapTypeEmptyObjectDefault() throws Exception {
    final String json = "{"
        + "\"schemaName\": \"mapDefaultSchema\","
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"dimensions\","
        + "  \"dataType\": \"MAP\","
        + "  \"childFieldSpecs\": {"
        + "    \"key\": {\"name\": \"key\", \"dataType\": \"STRING\", \"fieldType\": \"DIMENSION\"},"
        + "    \"value\": {\"name\": \"value\", \"dataType\": \"STRING\", \"fieldType\": \"DIMENSION\"}"
        + "  },"
        + "  \"defaultNullValue\": {}"
        + "}]"
        + "}";

    final Schema schema = JsonUtils.stringToObject(json, Schema.class);

    assertNotNull(schema);
    assertEquals(schema.getComplexFieldSpecs().size(), 1);

    final ComplexFieldSpec fieldSpec = schema.getComplexFieldSpecs().get(0);
    assertEquals(fieldSpec.getName(), "dimensions");
    assertEquals(fieldSpec.getDataType(), DataType.MAP);

    // Verify defaultNullValue is a Java Map, not Scala Map
    final Object defaultValue = fieldSpec.getDefaultNullValue();
    assertNotNull(defaultValue);
    assertTrue(defaultValue instanceof Map,
        "defaultNullValue should be java.util.Map, got: " + defaultValue.getClass().getName());
  }

  @Test
  public void testDeserializeSchemaWithListType() throws Exception {
    final String json = "{"
        + "\"schemaName\": \"listSchema\","
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"listField\","
        + "  \"dataType\": \"LIST\","
        + "  \"defaultNullValue\": \"[]\""
        + "}]"
        + "}";

    final Schema schema = JsonUtils.stringToObject(json, Schema.class);

    assertNotNull(schema);
    assertEquals(schema.getComplexFieldSpecs().size(), 1);

    final ComplexFieldSpec fieldSpec = schema.getComplexFieldSpecs().get(0);
    assertEquals(fieldSpec.getName(), "listField");
    assertEquals(fieldSpec.getDataType(), DataType.LIST);
  }

  @Test
  public void testDeserializeSchemaWithListTypeEmptyArrayDefault() throws Exception {
    final String json = "{"
        + "\"schemaName\": \"listDefaultSchema\","
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"items\","
        + "  \"dataType\": \"LIST\","
        + "  \"defaultNullValue\": []"
        + "}]"
        + "}";

    final Schema schema = JsonUtils.stringToObject(json, Schema.class);

    assertNotNull(schema);
    assertEquals(schema.getComplexFieldSpecs().size(), 1);

    final ComplexFieldSpec fieldSpec = schema.getComplexFieldSpecs().get(0);
    assertEquals(fieldSpec.getName(), "items");
    assertEquals(fieldSpec.getDataType(), DataType.LIST);

    // Verify defaultNullValue is a Java List, not Scala List
    final Object defaultValue = fieldSpec.getDefaultNullValue();
    assertNotNull(defaultValue);
    assertTrue(defaultValue instanceof List,
        "defaultNullValue should be java.util.List, got: " + defaultValue.getClass().getName());
  }

  @Test
  public void testEmptyMapDefaultValueIsJavaMap() throws Exception {
    final String json = "{"
        + "\"schemaName\": \"javaMapTest\","
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"testMap\","
        + "  \"dataType\": \"MAP\","
        + "  \"defaultNullValue\": {}"
        + "}]"
        + "}";

    final Schema schema = JsonUtils.stringToObject(json, Schema.class);
    final Object defaultValue = schema.getComplexFieldSpecs().get(0).getDefaultNullValue();

    // Must be a Java Map implementation, not Scala
    assertTrue(defaultValue instanceof java.util.Map,
        "Expected java.util.Map but got: " + defaultValue.getClass().getName());

    // Verify it's not a Scala collection by checking class name
    final String className = defaultValue.getClass().getName();
    assertTrue(className.startsWith("java.") || className.startsWith("com.fasterxml."),
        "Expected Java class but got: " + className);
  }

  @Test
  public void testEmptyListDefaultValueIsJavaList() throws Exception {
    final String json = "{"
        + "\"schemaName\": \"javaListTest\","
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"testList\","
        + "  \"dataType\": \"LIST\","
        + "  \"defaultNullValue\": []"
        + "}]"
        + "}";

    final Schema schema = JsonUtils.stringToObject(json, Schema.class);
    final Object defaultValue = schema.getComplexFieldSpecs().get(0).getDefaultNullValue();

    // Must be a Java List implementation, not Scala
    assertTrue(defaultValue instanceof java.util.List,
        "Expected java.util.List but got: " + defaultValue.getClass().getName());

    // Verify it's not a Scala collection by checking class name
    final String className = defaultValue.getClass().getName();
    assertTrue(className.startsWith("java.") || className.startsWith("com.fasterxml."),
        "Expected Java class but got: " + className);
  }

  @Test
  public void testDeserializeFullSchemaWithAllFieldTypes() throws Exception {
    final String json = "{"
        + "\"schemaName\": \"fullSchema\","
        + "\"primaryKeyColumns\": [\"id\"],"
        + "\"dimensionFieldSpecs\": ["
        + "  {\"name\": \"id\", \"dataType\": \"LONG\"},"
        + "  {\"name\": \"name\", \"dataType\": \"STRING\"}"
        + "],"
        + "\"metricFieldSpecs\": ["
        + "  {\"name\": \"count\", \"dataType\": \"LONG\"}"
        + "],"
        + "\"dateTimeFieldSpecs\": ["
        + "  {\"name\": \"ts\", \"dataType\": \"LONG\", "
        + "  \"format\": \"EPOCH|MILLISECONDS\", \"granularity\": \"1:MILLISECONDS\"}"
        + "],"
        + "\"complexFieldSpecs\": ["
        + "  {\"name\": \"metadata\", \"dataType\": \"MAP\", \"defaultNullValue\": {}}"
        + "]"
        + "}";

    final Schema schema = JsonUtils.stringToObject(json, Schema.class);

    assertNotNull(schema);
    assertEquals(schema.getSchemaName(), "fullSchema");
    assertEquals(schema.getPrimaryKeyColumns().size(), 1);
    assertEquals(schema.getDimensionFieldSpecs().size(), 2);
    assertEquals(schema.getMetricFieldSpecs().size(), 1);
    assertEquals(schema.getDateTimeFieldSpecs().size(), 1);
    assertEquals(schema.getComplexFieldSpecs().size(), 1);

    // Verify MAP field has Java Map as default
    final Object mapDefault = schema.getComplexFieldSpecs().get(0).getDefaultNullValue();
    assertTrue(mapDefault instanceof java.util.Map);
  }

  @Test
  public void testSchemaRoundTripSerialization() throws Exception {
    // Create schema programmatically
    final Schema originalSchema = new Schema.SchemaBuilder()
        .setSchemaName("roundTripSchema")
        .addSingleValueDimension("dim1", DataType.STRING)
        .addMetric("metric1", DataType.LONG)
        .addDateTime("ts", DataType.LONG, "EPOCH|MILLISECONDS", "1:MILLISECONDS")
        .build();

    // Serialize to JSON
    final String json = originalSchema.toJsonObject().toString();

    // Deserialize back
    final Schema deserializedSchema = JsonUtils.stringToObject(json, Schema.class);

    assertNotNull(deserializedSchema);
    assertEquals(deserializedSchema.getSchemaName(), originalSchema.getSchemaName());
    assertEquals(deserializedSchema.getDimensionFieldSpecs().size(), originalSchema.getDimensionFieldSpecs().size());
    assertEquals(deserializedSchema.getMetricFieldSpecs().size(), originalSchema.getMetricFieldSpecs().size());
    assertEquals(deserializedSchema.getDateTimeFieldSpecs().size(), originalSchema.getDateTimeFieldSpecs().size());
  }

  @Test
  public void testDeserializeSchemaWithComplexMapFieldAndChildSpecs() throws Exception {
    final String json = "{"
        + "\"schemaName\": \"preview_table\","
        + "\"dimensionFieldSpecs\": ["
        + "  {\"name\": \"firstName\", \"dataType\": \"STRING\"},"
        + "  {\"name\": \"lastName\", \"dataType\": \"STRING\"}"
        + "],"
        + "\"metricFieldSpecs\": ["
        + "  {\"name\": \"studentID\", \"dataType\": \"LONG\"}"
        + "],"
        + "\"dateTimeFieldSpecs\": ["
        + "  {\"name\": \"timestamp\", \"dataType\": \"LONG\", "
        + "  \"format\": \"EPOCH|MILLISECONDS\", \"granularity\": \"1:MILLISECONDS\"}"
        + "],"
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"address\","
        + "  \"dataType\": \"MAP\","
        + "  \"childFieldSpecs\": {"
        + "    \"key\": {\"name\": \"key\", \"dataType\": \"STRING\", \"fieldType\": \"DIMENSION\"},"
        + "    \"value\": {\"name\": \"value\", \"dataType\": \"STRING\", \"fieldType\": \"DIMENSION\"}"
        + "  },"
        + "  \"defaultNullValueString\": \"{}\","
        + "  \"defaultNullValue\": {}"
        + "}]"
        + "}";

    final Schema schema = JsonUtils.stringToObject(json, Schema.class);

    assertNotNull(schema);
    assertEquals(schema.getSchemaName(), "preview_table");
    assertEquals(schema.getDimensionFieldSpecs().size(), 2);
    assertEquals(schema.getMetricFieldSpecs().size(), 1);
    assertEquals(schema.getDateTimeFieldSpecs().size(), 1);
    assertEquals(schema.getComplexFieldSpecs().size(), 1);

    // Verify the MAP field is properly deserialized
    final ComplexFieldSpec mapField = schema.getComplexFieldSpecs().get(0);
    assertEquals(mapField.getName(), "address");
    assertEquals(mapField.getDataType(), DataType.MAP);

    // Verify defaultNullValue is Java Map
    final Object defaultValue = mapField.getDefaultNullValue();
    assertTrue(defaultValue instanceof java.util.Map,
        "Expected java.util.Map but got: " + defaultValue.getClass().getName());
  }

  @Test
  public void testGetStringValueReturnsValidJsonForEmptyMapDefault() throws Exception {
    final String json = "{"
        + "\"schemaName\": \"stringValueTest\","
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"testMap\","
        + "  \"dataType\": \"MAP\","
        + "  \"defaultNullValue\": {}"
        + "}]"
        + "}";

    final Schema schema = JsonUtils.stringToObject(json, Schema.class);
    final Object defaultValue = schema.getComplexFieldSpecs().get(0).getDefaultNullValue();

    // getStringValue should produce valid JSON
    final String stringValue = FieldSpec.getStringValue(defaultValue);
    assertEquals(stringValue, "{}",
        "getStringValue should return '{}' for empty Map, got: " + stringValue);
  }

  @Test
  public void testGetStringValueReturnsValidJsonForEmptyListDefault() throws Exception {
    final String json = "{"
        + "\"schemaName\": \"stringValueTest\","
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"testList\","
        + "  \"dataType\": \"LIST\","
        + "  \"defaultNullValue\": []"
        + "}]"
        + "}";

    final Schema schema = JsonUtils.stringToObject(json, Schema.class);
    final Object defaultValue = schema.getComplexFieldSpecs().get(0).getDefaultNullValue();

    // getStringValue should produce valid JSON
    final String stringValue = FieldSpec.getStringValue(defaultValue);
    assertEquals(stringValue, "[]",
        "getStringValue should return '[]' for empty List, got: " + stringValue);
  }
}
