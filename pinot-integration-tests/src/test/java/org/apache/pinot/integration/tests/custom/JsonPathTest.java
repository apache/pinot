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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.spi.cache.Cache;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(suiteName = "CustomClusterIntegrationTest")
public class JsonPathTest extends CustomDataQueryClusterIntegrationTest {

  protected static final String DEFAULT_TABLE_NAME = "JsonPathTest";

  protected static final int NUM_TOTAL_DOCS = 1000;
  private static final String MY_MAP_STR_FIELD_NAME = "myMapStr";
  private static final String MY_MAP_STR_K1_FIELD_NAME = "myMapStr_k1";
  private static final String MY_MAP_STR_K2_FIELD_NAME = "myMapStr_k2";
  private static final String COMPLEX_MAP_STR_FIELD_NAME = "complexMapStr";
  private static final String COMPLEX_MAP_STR_K3_FIELD_NAME = "complexMapStr_k3";

  protected final List<String> _sortedSequenceIds = new ArrayList<>(NUM_TOTAL_DOCS);

  @Override
  protected long getCountStarResult() {
    return NUM_TOTAL_DOCS;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension("myMap", FieldSpec.DataType.STRING)
        .addSingleValueDimension(MY_MAP_STR_FIELD_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(MY_MAP_STR_K1_FIELD_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(MY_MAP_STR_K2_FIELD_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(COMPLEX_MAP_STR_FIELD_NAME, FieldSpec.DataType.STRING)
        .addMultiValueDimension(COMPLEX_MAP_STR_K3_FIELD_NAME, FieldSpec.DataType.STRING).build();
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    List<TransformConfig> transformConfigs = Arrays.asList(
        new TransformConfig(MY_MAP_STR_K1_FIELD_NAME, "jsonPathString(" + MY_MAP_STR_FIELD_NAME + ", '$.k1')"),
        new TransformConfig(MY_MAP_STR_K2_FIELD_NAME, "jsonPathString(" + MY_MAP_STR_FIELD_NAME + ", '$.k2')"),
        new TransformConfig(COMPLEX_MAP_STR_K3_FIELD_NAME,
            "jsonPathArray(" + COMPLEX_MAP_STR_FIELD_NAME + ", '$.k3')"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).setIngestionConfig(ingestionConfig)
        .build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    List<org.apache.avro.Schema.Field> fields =
        Arrays.asList(new org.apache.avro.Schema.Field(MY_MAP_STR_FIELD_NAME, org.apache.avro.Schema.create(
                org.apache.avro.Schema.Type.STRING), null, null),
            new org.apache.avro.Schema.Field(COMPLEX_MAP_STR_FIELD_NAME, org.apache.avro.Schema.create(
                org.apache.avro.Schema.Type.STRING), null, null));
    avroSchema.setFields(fields);

    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_TOTAL_DOCS; i++) {
        Map<String, String> map = new HashMap<>();
        map.put("k1", "value-k1-" + i);
        map.put("k2", "value-k2-" + i);
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(MY_MAP_STR_FIELD_NAME, JsonUtils.objectToString(map));

        Map<String, Object> complexMap = new HashMap<>();
        complexMap.put("k1", "value-k1-" + i);
        complexMap.put("k2", "value-k2-" + i);
        complexMap.put("k3", Arrays.asList("value-k3-0-" + i, "value-k3-1-" + i, "value-k3-2-" + i));
        complexMap.put("k4",
            ImmutableMap.of("k4-k1", "value-k4-k1-" + i, "k4-k2", "value-k4-k2-" + i, "k4-k3", "value-k4-k3-" + i,
                "met", i));
        record.put(COMPLEX_MAP_STR_FIELD_NAME, JsonUtils.objectToString(complexMap));
        fileWriter.append(record);
        _sortedSequenceIds.add(String.valueOf(i));
      }
    }
    Collections.sort(_sortedSequenceIds);

    return avroFile;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    //Selection Query
    String query = "Select myMapStr from " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    Assert.assertNotNull(rows);
    Assert.assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Assert.assertTrue(value.indexOf("-k1-") > 0);
    }

    //Filter Query
    query = "Select jsonExtractScalar(myMapStr,'$.k1','STRING') from " + getTableName()
        + "  where jsonExtractScalar(myMapStr,'$.k1','STRING') = 'value-k1-0'";
    pinotResponse = postQuery(query);
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    Assert.assertNotNull(rows);
    Assert.assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Assert.assertEquals(value, "value-k1-0");
    }

    //selection order by
    query = "Select jsonExtractScalar(myMapStr,'$.k1','STRING') from " + getTableName()
        + " order by jsonExtractScalar(myMapStr,'$.k1','STRING')";
    pinotResponse = postQuery(query);
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    Assert.assertNotNull(rows);
    Assert.assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Assert.assertTrue(value.indexOf("-k1-") > 0);
    }

    //Group By Query
    query = "Select jsonExtractScalar(myMapStr,'$.k1','STRING'), count(*) from " + getTableName()
        + " group by jsonExtractScalar(myMapStr,'$.k1','STRING')";
    pinotResponse = postQuery(query);
    Assert.assertNotNull(pinotResponse.get("resultTable"));
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Assert.assertTrue(value.indexOf("-k1-") > 0);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testComplexQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    //Selection Query
    String query = "Select complexMapStr from " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");

    Assert.assertNotNull(rows);
    Assert.assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Map results = JsonUtils.stringToObject(value, Map.class);
      Assert.assertTrue(value.indexOf("-k1-") > 0);
      Assert.assertEquals(results.get("k1"), "value-k1-" + i);
      Assert.assertEquals(results.get("k2"), "value-k2-" + i);
      final List k3 = (List) results.get("k3");
      Assert.assertEquals(k3.size(), 3);
      Assert.assertEquals(k3.get(0), "value-k3-0-" + i);
      Assert.assertEquals(k3.get(1), "value-k3-1-" + i);
      Assert.assertEquals(k3.get(2), "value-k3-2-" + i);
      final Map k4 = (Map) results.get("k4");
      Assert.assertEquals(k4.size(), 4);
      Assert.assertEquals(k4.get("k4-k1"), "value-k4-k1-" + i);
      Assert.assertEquals(k4.get("k4-k2"), "value-k4-k2-" + i);
      Assert.assertEquals(k4.get("k4-k3"), "value-k4-k3-" + i);
      Assert.assertEquals(Double.parseDouble(k4.get("met").toString()), (double) i);
    }

    //Filter Query
    query = "Select jsonExtractScalar(complexMapStr,'$.k4','STRING') from " + getTableName()
        + "  where jsonExtractScalar(complexMapStr,'$.k4.k4-k1','STRING') = 'value-k4-k1-0'";
    pinotResponse = postQuery(query);
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    Assert.assertNotNull(rows);
    Assert.assertEquals(rows.size(), 1);
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Assert.assertEquals(value,
          "{\"k4-k1\":\"value-k4-k1-0\",\"k4-k2\":\"value-k4-k2-0\",\"k4-k3\":\"value-k4-k3-0\",\"met\":0}");
    }

    //selection order by
    query = "Select complexMapStr from " + getTableName()
        + " order by jsonExtractScalar(complexMapStr,'$.k4.k4-k1','STRING') DESC LIMIT " + NUM_TOTAL_DOCS;
    pinotResponse = postQuery(query);
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    Assert.assertNotNull(rows);
    Assert.assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Assert.assertTrue(value.indexOf("-k1-") > 0);
      Map results = JsonUtils.stringToObject(value, Map.class);
      String seqId = _sortedSequenceIds.get(NUM_TOTAL_DOCS - 1 - i);
      Assert.assertEquals(results.get("k1"), "value-k1-" + seqId);
      Assert.assertEquals(results.get("k2"), "value-k2-" + seqId);
      final List k3 = (List) results.get("k3");
      Assert.assertEquals(k3.get(0), "value-k3-0-" + seqId);
      Assert.assertEquals(k3.get(1), "value-k3-1-" + seqId);
      Assert.assertEquals(k3.get(2), "value-k3-2-" + seqId);
      final Map k4 = (Map) results.get("k4");
      Assert.assertEquals(k4.get("k4-k1"), "value-k4-k1-" + seqId);
      Assert.assertEquals(k4.get("k4-k2"), "value-k4-k2-" + seqId);
      Assert.assertEquals(k4.get("k4-k3"), "value-k4-k3-" + seqId);
      Assert.assertEquals(Double.parseDouble(k4.get("met").toString()), Double.parseDouble(seqId));
    }
  }

  @Test(dataProvider = "useV1QueryEngine")
  public void testComplexGroupByQueryV1(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    //Group By Query
    String query = "Select" + " jsonExtractScalar(complexMapStr,'$.k1','STRING'),"
        + " sum(jsonExtractScalar(complexMapStr,'$.k4.met','INT'))" + " from " + getTableName()
        + " group by jsonExtractScalar(complexMapStr,'$.k1','STRING')"
        + " order by sum(jsonExtractScalar(complexMapStr,'$.k4.met','INT')) DESC";
    JsonNode pinotResponse = postQuery(query);
    Assert.assertNotNull(pinotResponse.get("resultTable").get("rows"));
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      String seqId = _sortedSequenceIds.get(NUM_TOTAL_DOCS - 1 - i);
      final JsonNode row = rows.get(i);
      Assert.assertEquals(row.get(0).asText(), "value-k1-" + seqId);
      Assert.assertEquals(row.get(1).asDouble(), Double.parseDouble(seqId));
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testComplexGroupByQueryV2(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    //Group By Query
    String query = "Select" + " jsonExtractScalar(complexMapStr,'$.k1','STRING'),"
        + " sum(jsonExtractScalar(complexMapStr,'$.k4.met','INT'))" + " from " + getTableName()
        + " group by jsonExtractScalar(complexMapStr,'$.k1','STRING')"
        + " order by sum(jsonExtractScalar(complexMapStr,'$.k4.met','INT')) DESC";
    JsonNode pinotResponse = postQuery(query);
    Assert.assertNotNull(pinotResponse.get("resultTable").get("rows"));
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      String seqId = String.valueOf(NUM_TOTAL_DOCS - 1 - i);
      final JsonNode row = rows.get(i);
      Assert.assertEquals(row.get(0).asText(), "value-k1-" + seqId);
      Assert.assertEquals(row.get(1).asDouble(), Double.parseDouble(seqId));
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryWithIntegerDefault(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    //Group By Query
    String query = "Select" + " jsonExtractScalar(complexMapStr,'$.inExistKey','STRING','defaultKey'),"
        + " sum(jsonExtractScalar(complexMapStr,'$.inExistMet','INT','1'))" + " from " + getTableName()
        + " group by jsonExtractScalar(complexMapStr,'$.inExistKey','STRING','defaultKey')"
        + " order by sum(jsonExtractScalar(complexMapStr,'$.inExistMet','INT','1')) DESC";
    JsonNode pinotResponse = postQuery(query);
    Assert.assertNotNull(pinotResponse.get("resultTable").get("rows"));
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 1);
    final JsonNode row = rows.get(0);
    Assert.assertEquals(row.get(0).asText(), "defaultKey");
    Assert.assertEquals(row.get(1).asDouble(), 1000.0);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryWithDoubleDefault(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    //Group By Query
    String query = "Select" + " jsonExtractScalar(complexMapStr,'$.inExistKey','STRING', 'defaultKey'),"
        + " sum(jsonExtractScalar(complexMapStr,'$.inExistMet','DOUBLE','0.1'))" + " from " + getTableName()
        + " group by jsonExtractScalar(complexMapStr,'$.inExistKey','STRING','defaultKey')"
        + " order by sum(jsonExtractScalar(complexMapStr,'$.inExistMet','DOUBLE','0.1')) DESC";
    JsonNode pinotResponse = postQuery(query);
    Assert.assertNotNull(pinotResponse.get("resultTable").get("rows"));
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 1);
    final JsonNode row = rows.get(0);
    Assert.assertEquals(row.get(0).asText(), "defaultKey");
    Assert.assertTrue(Math.abs(row.get(1).asDouble() - 100.0) < 1e-10);
  }

  @Test(dataProvider = "useBothQueryEngines")
  void testFailedQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "Select jsonExtractScalar(myMapStr,\"$.k1\",\"STRING\") from " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    int expectedStatusCode;
    if (useMultiStageQueryEngine) {
      expectedStatusCode = QueryException.QUERY_PLANNING_ERROR_CODE;
    } else {
      expectedStatusCode = QueryException.SQL_PARSING_ERROR_CODE;
    }
    Assert.assertEquals(pinotResponse.get("exceptions").get(0).get("errorCode").asInt(), expectedStatusCode);
    Assert.assertEquals(pinotResponse.get("numDocsScanned").asInt(), 0);
    Assert.assertEquals(pinotResponse.get("totalDocs").asInt(), 0);

    query = "Select myMapStr from " + getTableName()
        + "  where jsonExtractScalar(myMapStr, '$.k1',\"STRING\") = 'value-k1-0'";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").get(0).get("errorCode").asInt(), expectedStatusCode);
    Assert.assertEquals(pinotResponse.get("numDocsScanned").asInt(), 0);
    Assert.assertEquals(pinotResponse.get("totalDocs").asInt(), 0);

    query = "Select jsonExtractScalar(myMapStr,\"$.k1\", 'STRING') from " + getTableName()
        + "  where jsonExtractScalar(myMapStr, '$.k1', 'STRING') = 'value-k1-0'";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").get(0).get("errorCode").asInt(), expectedStatusCode);
    Assert.assertEquals(pinotResponse.get("numDocsScanned").asInt(), 0);
    Assert.assertEquals(pinotResponse.get("totalDocs").asInt(), 0);
  }

  @Test
  public void testJsonPathCache() {
    Cache cache = CacheProvider.getCache();
    Assert.assertTrue(cache instanceof JsonPathCache);
    Assert.assertTrue(((JsonPathCache) cache).size() > 0);
  }
}
