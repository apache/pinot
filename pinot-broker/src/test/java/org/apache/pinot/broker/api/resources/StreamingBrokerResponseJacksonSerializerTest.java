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
package org.apache.pinot.broker.api.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.response.StreamingBrokerResponse;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class StreamingBrokerResponseJacksonSerializerTest {

  private ObjectMapper createMapper(Comparator<String> keysComparator) {
    ObjectMapper mapper = new ObjectMapper();
    StreamingBrokerResponseJacksonSerializer.registerModule(mapper, keysComparator);
    return mapper;
  }

  private String serialize(StreamingBrokerResponse response, Comparator<String> keysComparator) throws Exception {
    ObjectMapper mapper = createMapper(keysComparator);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    mapper.writeValue(outputStream, response);
    return outputStream.toString();
  }

  private JsonNode serializeToJsonNode(StreamingBrokerResponse response, Comparator<String> keysComparator)
      throws Exception {
    String json = serialize(response, keysComparator);
    return JsonUtils.stringToJsonNode(json);
  }

  @Test
  public void testBasicSerialization() throws Exception {
    DataSchema dataSchema = new DataSchema(
        new String[]{"col1", "col2", "col3"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.LONG
        }
    );

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, "value1", 100L});
    rows.add(new Object[]{2, "value2", 200L});
    rows.add(new Object[]{3, "value3", 300L});

    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(Collections.emptyList());
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    JsonNode json = serializeToJsonNode(response, Comparator.naturalOrder());

    // Verify resultTable exists
    assertTrue(json.has("resultTable"));
    JsonNode resultTable = json.get("resultTable");

    // Verify dataSchema
    assertTrue(resultTable.has("dataSchema"));
    JsonNode dataSchemaNode = resultTable.get("dataSchema");
    assertEquals(dataSchemaNode.get("columnNames").size(), 3);
    assertEquals(dataSchemaNode.get("columnNames").get(0).asText(), "col1");
    assertEquals(dataSchemaNode.get("columnNames").get(1).asText(), "col2");
    assertEquals(dataSchemaNode.get("columnNames").get(2).asText(), "col3");

    // Verify rows
    assertTrue(resultTable.has("rows"));
    JsonNode rowsNode = resultTable.get("rows");
    assertEquals(rowsNode.size(), 3);
    assertEquals(rowsNode.get(0).get(0).asInt(), 1);
    assertEquals(rowsNode.get(0).get(1).asText(), "value1");
    assertEquals(rowsNode.get(0).get(2).asLong(), 100L);
    assertEquals(rowsNode.get(1).get(0).asInt(), 2);
    assertEquals(rowsNode.get(1).get(1).asText(), "value2");
    assertEquals(rowsNode.get(1).get(2).asLong(), 200L);
  }

  @Test
  public void testEmptyResponse() throws Exception {
    DataSchema dataSchema = new DataSchema(
        new String[]{"col1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}
    );

    List<Object[]> rows = new ArrayList<>();
    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(Collections.emptyList());
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    JsonNode json = serializeToJsonNode(response, Comparator.naturalOrder());

    assertTrue(json.has("resultTable"));
    JsonNode resultTable = json.get("resultTable");
    assertTrue(resultTable.has("rows"));
    assertEquals(resultTable.get("rows").size(), 0);
  }

  @Test
  public void testNullValues() throws Exception {
    DataSchema dataSchema = new DataSchema(
        new String[]{"col1", "col2", "col3"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.DOUBLE
        }
    );

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{null, "value1", 1.5});
    rows.add(new Object[]{2, null, 2.5});
    rows.add(new Object[]{3, "value3", null});

    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(Collections.emptyList());
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    JsonNode json = serializeToJsonNode(response, Comparator.naturalOrder());

    JsonNode rowsNode = json.get("resultTable").get("rows");
    assertEquals(rowsNode.size(), 3);
    assertTrue(rowsNode.get(0).get(0).isNull());
    assertFalse(rowsNode.get(0).get(1).isNull());
    assertTrue(rowsNode.get(1).get(1).isNull());
    assertTrue(rowsNode.get(2).get(2).isNull());
  }

  @Test
  public void testMetainfoSerialization() throws Exception {
    DataSchema dataSchema = new DataSchema(
        new String[]{"col1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}
    );

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"test"});

    ObjectNode metainfoJson = JsonUtils.newObjectNode();
    metainfoJson.put("numDocsScanned", 1000L);
    metainfoJson.put("numServersQueried", 5);
    metainfoJson.put("numServersResponded", 5);

    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(Collections.emptyList(), metainfoJson);
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    JsonNode json = serializeToJsonNode(response, Comparator.naturalOrder());

    assertTrue(json.has("numDocsScanned"));
    assertEquals(json.get("numDocsScanned").asLong(), 1000L);
    assertTrue(json.has("numServersQueried"));
    assertEquals(json.get("numServersQueried").asInt(), 5);
    assertTrue(json.has("numServersResponded"));
    assertEquals(json.get("numServersResponded").asInt(), 5);
  }

  @Test
  public void testKeyOrdering() throws Exception {
    DataSchema dataSchema = new DataSchema(
        new String[]{"col1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}
    );

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"test"});

    ObjectNode metainfoJson = JsonUtils.newObjectNode();
    metainfoJson.put("zzzField", "last");
    metainfoJson.put("aaaField", "first");
    metainfoJson.put("mmmField", "middle");

    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(Collections.emptyList(), metainfoJson);
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    String json = serialize(response, Comparator.naturalOrder());

    // With natural order, aaaField should come before mmmField, which should come before zzzField
    int aaaIndex = json.indexOf("aaaField");
    int mmmIndex = json.indexOf("mmmField");
    int zzzIndex = json.indexOf("zzzField");

    assertTrue(aaaIndex > 0);
    assertTrue(mmmIndex > aaaIndex);
    assertTrue(zzzIndex > mmmIndex);
  }

  @Test
  public void testReverseKeyOrdering() throws Exception {
    DataSchema dataSchema = new DataSchema(
        new String[]{"col1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}
    );

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"test"});

    ObjectNode metainfoJson = JsonUtils.newObjectNode();
    metainfoJson.put("zzzField", "last");
    metainfoJson.put("aaaField", "first");
    metainfoJson.put("mmmField", "middle");

    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(Collections.emptyList(), metainfoJson);
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    String json = serialize(response, Comparator.reverseOrder());

    // With reverse order, zzzField should come before mmmField, which should come before aaaField
    int aaaIndex = json.indexOf("aaaField");
    int mmmIndex = json.indexOf("mmmField");
    int zzzIndex = json.indexOf("zzzField");

    assertTrue(zzzIndex > 0);
    assertTrue(mmmIndex > zzzIndex);
    assertTrue(aaaIndex > mmmIndex);
  }

  @Test
  public void testErrorResponse() throws Exception {
    StreamingBrokerResponse response = StreamingBrokerResponse.error(
        QueryErrorCode.QUERY_EXECUTION, "Test error message");

    JsonNode json = serializeToJsonNode(response, Comparator.naturalOrder());

    assertTrue(json.has("exceptions"));
    JsonNode exceptions = json.get("exceptions");
    assertEquals(exceptions.size(), 1);
    assertTrue(exceptions.get(0).get("message").asText().contains("Test error message"));
    assertEquals(exceptions.get(0).get("errorCode").asInt(), QueryErrorCode.QUERY_EXECUTION.getId());
  }

  @Test
  public void testNoDataSchema() throws Exception {
    StreamingBrokerResponse response = new StreamingBrokerResponse.EarlyResponse(
        new TestMetainfo(Collections.emptyList()));

    JsonNode json = serializeToJsonNode(response, Comparator.naturalOrder());

    assertEquals(json.toString(), "{}");
  }

  @Test
  public void testMultipleDataTypes() throws Exception {
    DataSchema dataSchema = new DataSchema(
        new String[]{"intCol", "longCol", "floatCol", "doubleCol", "stringCol"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.LONG,
            DataSchema.ColumnDataType.FLOAT,
            DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.STRING
        }
    );

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{42, 1234567890L, 3.14f, 2.71828, "test"});

    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(Collections.emptyList());
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    JsonNode json = serializeToJsonNode(response, Comparator.naturalOrder());

    JsonNode rowsNode = json.get("resultTable").get("rows");
    assertEquals(rowsNode.get(0).get(0).asInt(), 42);
    assertEquals(rowsNode.get(0).get(1).asLong(), 1234567890L);
    assertEquals(rowsNode.get(0).get(2).floatValue(), 3.14f, 0.001f);
    assertEquals(rowsNode.get(0).get(3).doubleValue(), 2.71828, 0.00001);
    assertEquals(rowsNode.get(0).get(4).asText(), "test");
  }

  @Test
  public void testWithExceptions() throws Exception {
    DataSchema dataSchema = new DataSchema(
        new String[]{"col1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}
    );

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"test"});

    List<QueryProcessingException> exceptions = new ArrayList<>();
    exceptions.add(new QueryProcessingException(QueryErrorCode.SERVER_SEGMENT_MISSING.getId(),
        "Segment not found"));
    exceptions.add(new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION.getId(),
        "Execution failed"));

    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(exceptions);
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    JsonNode json = serializeToJsonNode(response, Comparator.naturalOrder());

    assertTrue(json.has("exceptions"));
    JsonNode exceptionsNode = json.get("exceptions");
    assertEquals(exceptionsNode.size(), 2);
    assertTrue(exceptionsNode.get(0).get("message").asText().contains("Segment not found"));
    assertTrue(exceptionsNode.get(1).get("message").asText().contains("Execution failed"));
  }

  @Test
  public void testLargeResponse() throws Exception {
    DataSchema dataSchema = new DataSchema(
        new String[]{"id", "value"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        }
    );

    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      rows.add(new Object[]{i, "value_" + i});
    }

    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(Collections.emptyList());
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    JsonNode json = serializeToJsonNode(response, Comparator.naturalOrder());

    JsonNode rowsNode = json.get("resultTable").get("rows");
    assertEquals(rowsNode.size(), 1000);
    assertEquals(rowsNode.get(0).get(0).asInt(), 0);
    assertEquals(rowsNode.get(999).get(0).asInt(), 999);
    assertEquals(rowsNode.get(999).get(1).asText(), "value_999");
  }

  @Test
  public void testArrayTypes() throws Exception {
    DataSchema dataSchema = new DataSchema(
        new String[]{"intArray", "stringArray"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT_ARRAY,
            DataSchema.ColumnDataType.STRING_ARRAY
        }
    );

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{new int[]{1, 2, 3}, new String[]{"a", "b", "c"}});

    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(Collections.emptyList());
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    JsonNode json = serializeToJsonNode(response, Comparator.naturalOrder());

    JsonNode rowsNode = json.get("resultTable").get("rows");
    JsonNode intArray = rowsNode.get(0).get(0);
    assertEquals(intArray.size(), 3);
    assertEquals(intArray.get(0).asInt(), 1);
    assertEquals(intArray.get(1).asInt(), 2);
    assertEquals(intArray.get(2).asInt(), 3);

    JsonNode stringArray = rowsNode.get(0).get(1);
    assertEquals(stringArray.size(), 3);
    assertEquals(stringArray.get(0).asText(), "a");
    assertEquals(stringArray.get(1).asText(), "b");
    assertEquals(stringArray.get(2).asText(), "c");
  }

  @Test
  public void testModuleRegistration() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    StreamingBrokerResponseJacksonSerializer.registerModule(mapper, Comparator.naturalOrder());

    // Verify the module is registered by serializing a simple response
    DataSchema dataSchema = new DataSchema(
        new String[]{"col1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}
    );
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"test"});
    StreamingBrokerResponse.Metainfo metainfo = new TestMetainfo(Collections.emptyList());
    StreamingBrokerResponse response = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, metainfo, rows);

    // Should not throw an exception
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    mapper.writeValue(outputStream, response);
    assertTrue(outputStream.size() > 0);
  }

  /// Helper class for creating test metainfo
  private static class TestMetainfo implements StreamingBrokerResponse.Metainfo {
    private final List<QueryProcessingException> _exceptions;
    private final ObjectNode _additionalFields;

    public TestMetainfo(List<QueryProcessingException> exceptions) {
      this(exceptions, JsonUtils.newObjectNode());
    }

    public TestMetainfo(List<QueryProcessingException> exceptions, ObjectNode additionalFields) {
      _exceptions = exceptions;
      _additionalFields = additionalFields;
    }

    @Override
    public List<QueryProcessingException> getExceptions() {
      return _exceptions;
    }

    @Override
    public ObjectNode asJson() {
      ObjectNode json = JsonUtils.newObjectNode();
      if (!_exceptions.isEmpty()) {
        json.set("exceptions", JsonUtils.objectToJsonNode(_exceptions));
      }
      _additionalFields.fieldNames().forEachRemaining(fieldName -> json.set(fieldName, _additionalFields.get(fieldName)));
      return json;
    }
  }
}