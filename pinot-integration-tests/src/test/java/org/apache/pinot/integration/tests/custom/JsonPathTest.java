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
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.USE_INDEX_BASED_DISTINCT_OPERATOR;


@Test(suiteName = "CustomClusterIntegrationTest")
public class JsonPathTest extends CustomDataQueryClusterIntegrationTest {

  protected static final String DEFAULT_TABLE_NAME = "JsonPathTest";

  protected static final int NUM_DOCS_PER_SEGMENT = 1000;
  private static final String MY_MAP_STR_FIELD_NAME = "myMapStr";
  private static final String MY_MAP_STR_K1_FIELD_NAME = "myMapStr_k1";
  private static final String MY_MAP_STR_K2_FIELD_NAME = "myMapStr_k2";
  private static final String COMPLEX_MAP_STR_FIELD_NAME = "complexMapStr";
  private static final String COMPLEX_MAP_STR_K3_FIELD_NAME = "complexMapStr_k3";

  protected final List<String> _sortedSequenceIds = new ArrayList<>(NUM_DOCS_PER_SEGMENT);

  @Override
  protected long getCountStarResult() {
    return (long) NUM_DOCS_PER_SEGMENT * getNumAvroFiles();
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
        .setJsonIndexColumns(Collections.singletonList(MY_MAP_STR_FIELD_NAME))
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    List<org.apache.avro.Schema.Field> fields =
        Arrays.asList(new org.apache.avro.Schema.Field(MY_MAP_STR_FIELD_NAME, org.apache.avro.Schema.create(
                org.apache.avro.Schema.Type.STRING), null, null),
            new org.apache.avro.Schema.Field(COMPLEX_MAP_STR_FIELD_NAME, org.apache.avro.Schema.create(
                org.apache.avro.Schema.Type.STRING), null, null));
    avroSchema.setFields(fields);

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      for (int i = 0; i < NUM_DOCS_PER_SEGMENT; i++) {
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
            Map.of("k4-k1", "value-k4-k1-" + i, "k4-k2", "value-k4-k2-" + i, "k4-k3", "value-k4-k3-" + i,
                "met", i));
        record.put(COMPLEX_MAP_STR_FIELD_NAME, JsonUtils.objectToString(complexMap));
        for (DataFileWriter<GenericData.Record> writer : avroFilesAndWriters.getWriters()) {
          writer.append(record);
        }
        _sortedSequenceIds.add(String.valueOf(i));
      }
      Collections.sort(_sortedSequenceIds);
      return avroFilesAndWriters.getAvroFiles();
    }
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
      Map<?, ?> results = JsonUtils.stringToObject(value, Map.class);
      Assert.assertTrue(value.indexOf("-k1-") > 0);
      Assert.assertEquals(results.get("k1"), "value-k1-" + i % NUM_DOCS_PER_SEGMENT);
      Assert.assertEquals(results.get("k2"), "value-k2-" + i % NUM_DOCS_PER_SEGMENT);
      final List<?> k3 = (List<?>) results.get("k3");
      Assert.assertEquals(k3.size(), 3);
      Assert.assertEquals(k3.get(0), "value-k3-0-" + i % NUM_DOCS_PER_SEGMENT);
      Assert.assertEquals(k3.get(1), "value-k3-1-" + i % NUM_DOCS_PER_SEGMENT);
      Assert.assertEquals(k3.get(2), "value-k3-2-" + i % NUM_DOCS_PER_SEGMENT);
      final Map<?, ?> k4 = (Map<?, ?>) results.get("k4");
      Assert.assertEquals(k4.size(), 4);
      Assert.assertEquals(k4.get("k4-k1"), "value-k4-k1-" + i % NUM_DOCS_PER_SEGMENT);
      Assert.assertEquals(k4.get("k4-k2"), "value-k4-k2-" + i % NUM_DOCS_PER_SEGMENT);
      Assert.assertEquals(k4.get("k4-k3"), "value-k4-k3-" + i % NUM_DOCS_PER_SEGMENT);
      Assert.assertEquals(Double.parseDouble(k4.get("met").toString()), i % NUM_DOCS_PER_SEGMENT);
    }

    //Filter Query
    query = "Select jsonExtractScalar(complexMapStr,'$.k4','STRING') from " + getTableName()
        + "  where jsonExtractScalar(complexMapStr,'$.k4.k4-k1','STRING') = 'value-k4-k1-0'";
    pinotResponse = postQuery(query);
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    Assert.assertNotNull(rows);
    Assert.assertEquals(rows.size(), getNumAvroFiles());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Map<?, ?> k4 = JsonUtils.stringToObject(value, Map.class);
      Assert.assertEquals(k4.size(), 4);
      Assert.assertEquals(k4.get("k4-k1"), "value-k4-k1-0");
      Assert.assertEquals(k4.get("k4-k2"), "value-k4-k2-0");
      Assert.assertEquals(k4.get("k4-k3"), "value-k4-k3-0");
      Assert.assertEquals(Double.parseDouble(k4.get("met").toString()), 0.0);
    }

    //selection order by
    query = "Select complexMapStr from " + getTableName()
        + " order by jsonExtractScalar(complexMapStr,'$.k4.k4-k1','STRING') DESC LIMIT " + NUM_DOCS_PER_SEGMENT;
    pinotResponse = postQuery(query);
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    Assert.assertNotNull(rows);
    Assert.assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Assert.assertTrue(value.indexOf("-k1-") > 0);
      Map<?, ?> results = JsonUtils.stringToObject(value, Map.class);
      String seqId = _sortedSequenceIds.get(NUM_DOCS_PER_SEGMENT - 1 - i / getNumAvroFiles());
      Assert.assertEquals(results.get("k1"), "value-k1-" + seqId);
      Assert.assertEquals(results.get("k2"), "value-k2-" + seqId);
      final List<?> k3 = (List<?>) results.get("k3");
      Assert.assertEquals(k3.get(0), "value-k3-0-" + seqId);
      Assert.assertEquals(k3.get(1), "value-k3-1-" + seqId);
      Assert.assertEquals(k3.get(2), "value-k3-2-" + seqId);
      final Map<?, ?> k4 = (Map<?, ?>) results.get("k4");
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
      String seqId = _sortedSequenceIds.get(NUM_DOCS_PER_SEGMENT - 1 - i);
      final JsonNode row = rows.get(i);
      Assert.assertEquals(row.get(0).asText(), "value-k1-" + seqId);
      Assert.assertEquals(row.get(1).asDouble(), Double.parseDouble(seqId) * getNumAvroFiles());
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
      String seqId = String.valueOf(NUM_DOCS_PER_SEGMENT - 1 - i);
      final JsonNode row = rows.get(i);
      Assert.assertEquals(row.get(0).asText(), "value-k1-" + seqId);
      Assert.assertEquals(row.get(1).asDouble(), Double.parseDouble(seqId) * getNumAvroFiles());
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
    Assert.assertEquals(row.get(1).asDouble(), 1000.0 * getNumAvroFiles());
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
    Assert.assertTrue(Math.abs(row.get(1).asDouble() - 100.0 * getNumAvroFiles()) < 1e-10);
  }

  @Test(dataProvider = "useBothQueryEngines")
  void testFailedQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "Select jsonExtractScalar(myMapStr,\"$.k1\",\"STRING\") from " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    int expectedStatusCode;
    if (useMultiStageQueryEngine) {
      expectedStatusCode = QueryErrorCode.UNKNOWN_COLUMN.getId();
    } else {
      expectedStatusCode = QueryErrorCode.SQL_PARSING.getId();
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

  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonKeysQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT jsonExtractKey(myMapStr, '$.*', 'maxDepth=1') FROM " + getTableName() + " LIMIT 1";
    JsonNode pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    Assert.assertEquals(row.size(), 1);
    // JsonPath returns keys in JsonPath format like "$['key']"
    JsonNode keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    Assert.assertTrue(keys.size() > 0);

    query = "SELECT jsonExtractKey(complexMapStr, '$.*', 'maxDepth=2') FROM " + getTableName() + " LIMIT 1";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 1);
    row = rows.get(0);
    Assert.assertEquals(row.size(), 1);
    keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    Assert.assertTrue(keys.size() > 0);

    query = "SELECT jsonExtractKey(complexMapStr, '$.*', 'maxDepth=3') FROM " + getTableName() + " LIMIT 1";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 1);
    row = rows.get(0);
    Assert.assertEquals(row.size(), 1);
    keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    Assert.assertTrue(keys.size() > 0);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonKeysQueriesWithDotNotation(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Test optional parameter jsonExtractKey with dotNotation=true (simple JSON)
    String query =
        "SELECT jsonExtractKey(myMapStr, '$.*', 'maxDepth=1; dotNotation=true') FROM " + getTableName() + " LIMIT 1";
    JsonNode pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    Assert.assertEquals(row.size(), 1);
    JsonNode keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    Assert.assertEquals(keys.size(), 2); // k1, k2
    // Should contain simple key names, not JsonPath format
    List<String> keyList = new ArrayList<>();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    Assert.assertTrue(keyList.contains("k1"));
    Assert.assertTrue(keyList.contains("k2"));
    // Should NOT contain JsonPath format like "$['k1']"
    Assert.assertFalse(keyList.contains("$['k1']"));
    Assert.assertFalse(keyList.contains("$['k2']"));

    // Test optional parameter jsonExtractKey with dotNotation=false (JsonPath format)
    query =
        "SELECT jsonExtractKey(myMapStr, '$.*', 'maxDepth=1; dotNotation=false') FROM " + getTableName() + " LIMIT 1";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    Assert.assertEquals(keys.size(), 2);
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain JsonPath format
    Assert.assertTrue(keyList.contains("$['k1']"));
    Assert.assertTrue(keyList.contains("$['k2']"));

    // Test recursive key extraction with dot notation on complex JSON
    query = "SELECT jsonExtractKey(complexMapStr, '$..**', 'maxDepth=2; dotNotation=true') FROM " + getTableName()
        + " LIMIT 1";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    Assert.assertTrue(keys.size() >= 4); // At least k1, k2, k3, k4
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain top-level keys in dot notation
    Assert.assertTrue(keyList.contains("k1"));
    Assert.assertTrue(keyList.contains("k2"));
    Assert.assertTrue(keyList.contains("k3"));
    Assert.assertTrue(keyList.contains("k4"));

    // Test recursive key extraction with JsonPath format
    query = "SELECT jsonExtractKey(complexMapStr, '$..**', 'maxDepth=2; dotNotation=false') FROM " + getTableName()
        + " LIMIT 1";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain JsonPath format
    Assert.assertTrue(keyList.contains("$['k1']"));
    Assert.assertTrue(keyList.contains("$['k2']"));
    Assert.assertTrue(keyList.contains("$['k3']"));
    Assert.assertTrue(keyList.contains("$['k4']"));

    // Test deeper recursive extraction with dot notation
    query = "SELECT jsonExtractKey(complexMapStr, '$..**', 'maxDepth=3; dotNotation=true') FROM " + getTableName()
        + " LIMIT 1";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    Assert.assertTrue(keys.size() > 4); // Should include nested keys
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain nested keys in dot notation
    Assert.assertTrue(keyList.contains("k4.k4-k1"));
    Assert.assertTrue(keyList.contains("k4.k4-k2"));
    Assert.assertTrue(keyList.contains("k4.k4-k3"));
    Assert.assertTrue(keyList.contains("k4.met"));
    // Should contain array indices in dot notation
    Assert.assertTrue(keyList.contains("k3.0"));
    Assert.assertTrue(keyList.contains("k3.1"));
    Assert.assertTrue(keyList.contains("k3.2"));

    // Test deeper recursive extraction with JsonPath format
    query = "SELECT jsonExtractKey(complexMapStr, '$..**', 'maxDepth=3; dotNotation=false') FROM " + getTableName()
        + " LIMIT 1";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain nested keys in JsonPath format
    Assert.assertTrue(keyList.contains("$['k4']['k4-k1']"));
    Assert.assertTrue(keyList.contains("$['k4']['k4-k2']"));
    Assert.assertTrue(keyList.contains("$['k4']['k4-k3']"));
    Assert.assertTrue(keyList.contains("$['k4']['met']"));
    // Should contain array indices in JsonPath format
    Assert.assertTrue(keyList.contains("$['k3'][0]"));
    Assert.assertTrue(keyList.contains("$['k3'][1]"));
    Assert.assertTrue(keyList.contains("$['k3'][2]"));

    // Test specific path extraction with dot notation
    query = "SELECT jsonExtractKey(complexMapStr, '$.k4.*', 'maxDepth=2; dotNotation=true') FROM " + getTableName()
        + " LIMIT 1";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    Assert.assertEquals(keys.size(), 4); // k4-k1, k4-k2, k4-k3, met
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain nested keys in dot notation format
    Assert.assertTrue(keyList.contains("k4.k4-k1"));
    Assert.assertTrue(keyList.contains("k4.k4-k2"));
    Assert.assertTrue(keyList.contains("k4.k4-k3"));
    Assert.assertTrue(keyList.contains("k4.met"));

    // Test backward compatibility - 2-parameter version should default to JsonPath format
    query = "SELECT jsonExtractKey(myMapStr, '$.*') FROM " + getTableName() + " LIMIT 1";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should default to JsonPath format
    Assert.assertTrue(keyList.contains("$['k1']"));
    Assert.assertTrue(keyList.contains("$['k2']"));

    // Test backward compatibility - no dotNotation should default to JsonPath format
    query = "SELECT jsonExtractKey(myMapStr, '$.*', 'maxDepth=1') FROM " + getTableName() + " LIMIT 1";
    pinotResponse = postQuery(query);
    Assert.assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    Assert.assertTrue(keys.isArray());
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should default to JsonPath format
    Assert.assertTrue(keyList.contains("$['k1']"));
    Assert.assertTrue(keyList.contains("$['k2']"));
  }

  // --- JsonIndexDistinctOperator tests (useIndexBasedDistinctOperator) ---

  /**
   * Without useIndexBasedDistinctOperator (disabled by default), SELECT DISTINCT jsonExtractIndex(...) uses
   * default DistinctOperator and returns correct results.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctOperatorDisabledByDefault(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') FROM "
        + getTableName() + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') LIMIT 10000";
    JsonNode response = postQuery(query);
    Assert.assertEquals(response.get("exceptions").size(), 0);
    List<String> values = extractOrderedDistinctValues(response);
    Assert.assertFalse(values.isEmpty(),
        "Baseline (operator disabled) should return distinct values. Engine="
            + (useMultiStageQueryEngine ? "MSE" : "SSE"));
  }

  /**
   * With useIndexBasedDistinctOperator, JsonIndexDistinctOperator produces same results as baseline.
   * Compares ordered rows (not just sets) to verify ORDER BY semantics.
   * For SSE, verifies numEntriesScannedPostFilter=0 (index path, no doc scan).
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctOperatorWithPinotJsonIndex(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') FROM "
        + getTableName() + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertEquals(optimizedRows, baselineRows,
        "JsonIndexDistinctOperator should produce same ordered results as baseline. "
            + "Engine=" + (useMultiStageQueryEngine ? "MSE" : "SSE"));

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asLong(), 0L,
          "JsonIndexDistinctOperator (SSE) uses index only (numEntriesScannedPostFilter=0).");
    }
  }

  /**
   * JsonIndexDistinctOperator with filter produces same ordered results as baseline.
   * For SSE, verifies numEntriesScannedPostFilter=0 (index path, no doc scan).
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctOperatorWithFilter(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') FROM "
        + getTableName() + " WHERE jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k2', 'STRING') = 'value-k2-0'"
        + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') LIMIT 10000";
    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertEquals(optimizedRows, baselineRows,
        "JsonIndexDistinctOperator with filter should match baseline. Engine="
            + (useMultiStageQueryEngine ? "MSE" : "SSE"));

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asLong(), 0L,
          "JsonIndexDistinctOperator with filter (SSE) uses index only (numEntriesScannedPostFilter=0).");
    }
  }

  /**
   * Verifies that JsonIndexDistinctOperator correctly materializes the defaultValue for docs where the JSON path
   * is absent, matching baseline JsonExtractIndexTransformFunction behavior.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctOperatorWithDefaultValue(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Query a non-existent path with a defaultValue — all docs should produce the default
    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.nonexistent', 'STRING', 'N/A') FROM " + getTableName()
        + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.nonexistent', 'STRING', 'N/A') LIMIT 10";

    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertEquals(optimizedRows, baselineRows,
        "JsonIndexDistinctOperator with defaultValue should match baseline. Engine="
            + (useMultiStageQueryEngine ? "MSE" : "SSE"));
    Assert.assertTrue(optimizedRows.contains("N/A"),
        "defaultValue 'N/A' should appear in results for non-existent path. Engine="
            + (useMultiStageQueryEngine ? "MSE" : "SSE"));
  }

  /**
   * Verifies that JsonIndexDistinctOperator throws when the JSON path is absent for some docs and no defaultValue
   * is provided (matching baseline JsonExtractIndexTransformFunction behavior which throws "Illegal Json Path").
   * Only tested on SSE because MSE may handle errors differently.
   */
  @Test(dataProvider = "useV1QueryEngine")
  public void testJsonIndexDistinctOperatorMissingPathNoDefault(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Query a non-existent path WITHOUT defaultValue — should produce an error
    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.nonexistent', 'STRING') FROM " + getTableName() + " LIMIT 10";

    // Baseline also throws for missing path without defaultValue
    JsonNode baselineResponse = postQuery(query);
    Assert.assertTrue(baselineResponse.get("exceptions").size() > 0,
        "Baseline should throw for missing JSON path without defaultValue");

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertTrue(optimizedResponse.get("exceptions").size() > 0,
        "JsonIndexDistinctOperator should throw for missing JSON path without defaultValue");
  }

  // --- Same-path JSON_MATCH predicate tests (trigger getMatchingDistinctValues fast path) ---

  /**
   * Same-path REGEXP_LIKE: fully pushed down, single dict scan, no posting list reads.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathRegexpLike(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // REGEXP_LIKE on $.k1 matching a subset of values
    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') FROM "
        + getTableName() + " WHERE JSON_MATCH(" + MY_MAP_STR_FIELD_NAME
        + ", 'REGEXP_LIKE(\"$.k1\", ''value-k1-[0-9]'')')"
        + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertFalse(baselineRows.isEmpty(), "REGEXP_LIKE should match single-digit k1 values");
    Assert.assertEquals(optimizedRows, baselineRows,
        "Same-path REGEXP_LIKE fast path should match baseline");

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asLong(), 0L);
    }
  }

  /**
   * Same-path EQ: fully pushed down.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathEq(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') FROM "
        + getTableName() + " WHERE JSON_MATCH(" + MY_MAP_STR_FIELD_NAME
        + ", '\"$.k1\" = ''value-k1-0''')"
        + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertEquals(optimizedRows, baselineRows,
        "Same-path EQ fast path should match baseline");
    Assert.assertTrue(optimizedRows.contains("value-k1-0"));

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asLong(), 0L);
    }
  }

  /**
   * Same-path NOT_EQ: fully pushed down.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathNotEq(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') FROM "
        + getTableName() + " WHERE JSON_MATCH(" + MY_MAP_STR_FIELD_NAME
        + ", '\"$.k1\" != ''value-k1-0''')"
        + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertEquals(optimizedRows, baselineRows,
        "Same-path NOT_EQ fast path should match baseline");
    Assert.assertFalse(optimizedRows.contains("value-k1-0"));

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asLong(), 0L);
    }
  }

  /**
   * Same-path IN: fully pushed down.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathIn(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') FROM "
        + getTableName() + " WHERE JSON_MATCH(" + MY_MAP_STR_FIELD_NAME
        + ", '\"$.k1\" IN (''value-k1-0'', ''value-k1-1'', ''value-k1-2'')')"
        + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertEquals(optimizedRows, baselineRows,
        "Same-path IN fast path should match baseline");
    Assert.assertEquals(optimizedRows.size(), 3);

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asLong(), 0L);
    }
  }

  /**
   * Same-path NOT_IN: fully pushed down.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathNotIn(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') FROM "
        + getTableName() + " WHERE JSON_MATCH(" + MY_MAP_STR_FIELD_NAME
        + ", '\"$.k1\" NOT IN (''value-k1-0'', ''value-k1-1'')')"
        + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertEquals(optimizedRows, baselineRows,
        "Same-path NOT_IN fast path should match baseline");
    Assert.assertFalse(optimizedRows.contains("value-k1-0"));
    Assert.assertFalse(optimizedRows.contains("value-k1-1"));

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asLong(), 0L);
    }
  }

  /**
   * Same-path IS NOT NULL: fully pushed down.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathIsNotNull(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') FROM "
        + getTableName() + " WHERE JSON_MATCH(" + MY_MAP_STR_FIELD_NAME + ", '\"$.k1\" IS NOT NULL')"
        + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertEquals(optimizedRows, baselineRows,
        "Same-path IS NOT NULL fast path should match baseline");
    Assert.assertEquals(optimizedRows.size(), NUM_DOCS_PER_SEGMENT,
        "IS NOT NULL should return all values since every doc has $.k1");

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asLong(), 0L);
    }
  }

  /**
   * Same-path REGEXP_LIKE with 4-arg form (defaultValue): fully pushed down fast path still works with defaults.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathRegexpLikeWithDefault(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.k1', 'STRING', 'fallback') FROM "
        + getTableName() + " WHERE JSON_MATCH(" + MY_MAP_STR_FIELD_NAME
        + ", 'REGEXP_LIKE(\"$.k1\", ''value-k1-[0-9]'')')"
        + " ORDER BY jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING', 'fallback') LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertEquals(optimizedRows, baselineRows,
        "Same-path REGEXP_LIKE 4-arg fast path should match baseline");
    // The default should NOT appear since the filter only matches docs that HAVE $.k1
    Assert.assertFalse(optimizedRows.contains("fallback"),
        "Same-path filter ensures all matching docs have the path, so no default should appear");

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asLong(), 0L);
    }
  }

  /**
   * Same-path REGEXP_LIKE without ORDER BY: verify LIMIT is respected with fast path.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathWithLimit(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING') FROM "
        + getTableName() + " WHERE JSON_MATCH(" + MY_MAP_STR_FIELD_NAME
        + ", '\"$.k1\" IS NOT NULL') LIMIT 5";

    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);

    JsonNode optimizedResponse = postQueryWithOptions(query, USE_INDEX_BASED_DISTINCT_OPERATOR + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    Assert.assertEquals(optimizedRows.size(), 5, "LIMIT 5 should be respected by fast path");
    Assert.assertEquals(baselineRows.size(), 5);

    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asLong(), 0L);
    }
  }

  private static List<String> extractOrderedDistinctValues(JsonNode response) {
    List<String> values = new ArrayList<>();
    JsonNode rows = response.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      JsonNode cell = rows.get(i).get(0);
      values.add(cell.isNull() ? null : cell.asText());
    }
    return values;
  }
}
