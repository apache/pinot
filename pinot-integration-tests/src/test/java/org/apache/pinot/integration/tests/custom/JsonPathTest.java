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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class JsonPathTest extends CustomDataQueryClusterIntegrationTest {
  protected static final String DEFAULT_TABLE_NAME = "JsonPathTest";

  protected static final int NUM_DOCS_PER_SEGMENT = 1000;
  // Number of distinct values for myMapStr.$.k1 across the segment. Setting this lower than NUM_DOCS_PER_SEGMENT
  // forces value repetition, so the JsonIndexDistinct path (which enumerates dictionary values once) and the scan
  // path (which visits every doc) return the same result set but follow visibly different code paths.
  private static final int NUM_DISTINCT_K1 = 100;
  private static final String MY_MAP_STR_FIELD_NAME = "myMapStr";
  private static final String MY_MAP_STR_K1_FIELD_NAME = "myMapStr_k1";
  private static final String MY_MAP_STR_K2_FIELD_NAME = "myMapStr_k2";
  private static final String COMPLEX_MAP_STR_FIELD_NAME = "complexMapStr";
  private static final String COMPLEX_MAP_STR_K3_FIELD_NAME = "complexMapStr_k3";

  // Query-option strings passed to postQueryWithOptions.
  private static final String OPT_USE_INDEX = QueryOptionKey.USE_INDEX_BASED_DISTINCT_OPERATOR + "=true";
  private static final String OPT_USE_INDEX_SKIP_MISSING_PATH =
      OPT_USE_INDEX + ";" + QueryOptionKey.JSON_INDEX_DISTINCT_SKIP_MISSING_PATH + "=true";

  protected final List<String> _sortedSequenceIds = new ArrayList<>(NUM_DOCS_PER_SEGMENT);

  @Override
  protected long getCountStarResult() {
    return (long) NUM_DOCS_PER_SEGMENT * getNumAvroFiles();
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension("myMap", DataType.STRING)
        .addSingleValueDimension(MY_MAP_STR_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(MY_MAP_STR_K1_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(MY_MAP_STR_K2_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(COMPLEX_MAP_STR_FIELD_NAME, DataType.STRING)
        .addMultiValueDimension(COMPLEX_MAP_STR_K3_FIELD_NAME, DataType.STRING)
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    List<TransformConfig> transformConfigs = List.of(
        new TransformConfig(MY_MAP_STR_K1_FIELD_NAME, "jsonPathString(" + MY_MAP_STR_FIELD_NAME + ", '$.k1')"),
        new TransformConfig(MY_MAP_STR_K2_FIELD_NAME, "jsonPathString(" + MY_MAP_STR_FIELD_NAME + ", '$.k2')"),
        new TransformConfig(COMPLEX_MAP_STR_K3_FIELD_NAME, "jsonPathArray(" + COMPLEX_MAP_STR_FIELD_NAME + ", '$.k3')")
    );
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setIngestionConfig(ingestionConfig)
        .setJsonIndexColumns(List.of(MY_MAP_STR_FIELD_NAME))
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    List<org.apache.avro.Schema.Field> fields = List.of(
        new org.apache.avro.Schema.Field(MY_MAP_STR_FIELD_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(COMPLEX_MAP_STR_FIELD_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null)
    );
    avroSchema.setFields(fields);

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      for (int i = 0; i < NUM_DOCS_PER_SEGMENT; i++) {
        Map<String, String> map = new HashMap<>();
        map.put("k1", "value-k1-" + (i % NUM_DISTINCT_K1));
        map.put("k2", "value-k2-" + i);
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(MY_MAP_STR_FIELD_NAME, JsonUtils.objectToString(map));

        Map<String, Object> complexMap = new HashMap<>();
        complexMap.put("k1", "value-k1-" + i);
        complexMap.put("k2", "value-k2-" + i);
        complexMap.put("k3", List.of("value-k3-0-" + i, "value-k3-1-" + i, "value-k3-2-" + i));
        complexMap.put("k4", Map.of(
            "k4-k1", "value-k4-k1-" + i,
            "k4-k2", "value-k4-k2-" + i,
            "k4-k3", "value-k4-k3-" + i,
            "met", i)
        );
        record.put(COMPLEX_MAP_STR_FIELD_NAME, JsonUtils.objectToString(complexMap));
        for (DataFileWriter<GenericData.Record> writer : avroFilesAndWriters.getWriters()) {
          writer.append(record);
        }
        _sortedSequenceIds.add(String.valueOf(i));
      }
      _sortedSequenceIds.sort(null);
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    //Selection Query
    String query = "SELECT myMapStr FROM " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    assertNotNull(rows);
    assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      assertTrue(value.indexOf("-k1-") > 0);
    }

    //Filter Query
    String expr = "jsonExtractScalar(myMapStr,'$.k1','STRING')";
    query = "SELECT " + expr + " FROM " + getTableName() + " WHERE " + expr + " = 'value-k1-0'";
    pinotResponse = postQuery(query);
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    assertNotNull(rows);
    assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      assertEquals(value, "value-k1-0");
    }

    //selection order by
    query = "SELECT " + expr + " FROM " + getTableName() + " ORDER BY " + expr;
    pinotResponse = postQuery(query);
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    assertNotNull(rows);
    assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      assertTrue(value.indexOf("-k1-") > 0);
    }

    //Group By Query
    query = "SELECT " + expr + ", count(*) FROM " + getTableName() + " GROUP BY " + expr;
    pinotResponse = postQuery(query);
    assertNotNull(pinotResponse.get("resultTable"));
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      assertTrue(value.indexOf("-k1-") > 0);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testComplexQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    //Selection Query
    String query = "SELECT complexMapStr FROM " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");

    assertNotNull(rows);
    assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Map<?, ?> results = JsonUtils.stringToObject(value, Map.class);
      assertTrue(value.indexOf("-k1-") > 0);
      assertEquals(results.get("k1"), "value-k1-" + i % NUM_DOCS_PER_SEGMENT);
      assertEquals(results.get("k2"), "value-k2-" + i % NUM_DOCS_PER_SEGMENT);
      List<?> k3 = (List<?>) results.get("k3");
      assertEquals(k3.size(), 3);
      assertEquals(k3.get(0), "value-k3-0-" + i % NUM_DOCS_PER_SEGMENT);
      assertEquals(k3.get(1), "value-k3-1-" + i % NUM_DOCS_PER_SEGMENT);
      assertEquals(k3.get(2), "value-k3-2-" + i % NUM_DOCS_PER_SEGMENT);
      Map<?, ?> k4 = (Map<?, ?>) results.get("k4");
      assertEquals(k4.size(), 4);
      assertEquals(k4.get("k4-k1"), "value-k4-k1-" + i % NUM_DOCS_PER_SEGMENT);
      assertEquals(k4.get("k4-k2"), "value-k4-k2-" + i % NUM_DOCS_PER_SEGMENT);
      assertEquals(k4.get("k4-k3"), "value-k4-k3-" + i % NUM_DOCS_PER_SEGMENT);
      assertEquals(Double.parseDouble(k4.get("met").toString()), i % NUM_DOCS_PER_SEGMENT);
    }

    //Filter Query
    query = "SELECT jsonExtractScalar(complexMapStr,'$.k4','STRING') FROM " + getTableName()
        + " WHERE jsonExtractScalar(complexMapStr,'$.k4.k4-k1','STRING') = 'value-k4-k1-0'";
    pinotResponse = postQuery(query);
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    assertNotNull(rows);
    assertEquals(rows.size(), getNumAvroFiles());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      Map<?, ?> k4 = JsonUtils.stringToObject(value, Map.class);
      assertEquals(k4.size(), 4);
      assertEquals(k4.get("k4-k1"), "value-k4-k1-0");
      assertEquals(k4.get("k4-k2"), "value-k4-k2-0");
      assertEquals(k4.get("k4-k3"), "value-k4-k3-0");
      assertEquals(Double.parseDouble(k4.get("met").toString()), 0.0);
    }

    //selection order by
    query = "SELECT complexMapStr FROM " + getTableName()
        + " ORDER BY jsonExtractScalar(complexMapStr,'$.k4.k4-k1','STRING') DESC LIMIT " + NUM_DOCS_PER_SEGMENT;
    pinotResponse = postQuery(query);
    rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    assertNotNull(rows);
    assertFalse(rows.isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String value = rows.get(i).get(0).textValue();
      assertTrue(value.indexOf("-k1-") > 0);
      Map<?, ?> results = JsonUtils.stringToObject(value, Map.class);
      String seqId = _sortedSequenceIds.get(NUM_DOCS_PER_SEGMENT - 1 - i / getNumAvroFiles());
      assertEquals(results.get("k1"), "value-k1-" + seqId);
      assertEquals(results.get("k2"), "value-k2-" + seqId);
      List<?> k3 = (List<?>) results.get("k3");
      assertEquals(k3.get(0), "value-k3-0-" + seqId);
      assertEquals(k3.get(1), "value-k3-1-" + seqId);
      assertEquals(k3.get(2), "value-k3-2-" + seqId);
      Map<?, ?> k4 = (Map<?, ?>) results.get("k4");
      assertEquals(k4.get("k4-k1"), "value-k4-k1-" + seqId);
      assertEquals(k4.get("k4-k2"), "value-k4-k2-" + seqId);
      assertEquals(k4.get("k4-k3"), "value-k4-k3-" + seqId);
      assertEquals(Double.parseDouble(k4.get("met").toString()), Double.parseDouble(seqId));
    }
  }

  @Test(dataProvider = "useV1QueryEngine")
  public void testComplexGroupByQueryV1(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    //Group By Query
    String groupExpr = "jsonExtractScalar(complexMapStr,'$.k1','STRING')";
    String sumExpr = "SUM(jsonExtractScalar(complexMapStr,'$.k4.met','INT'))";
    String query = "SELECT " + groupExpr + ", " + sumExpr + " FROM " + getTableName()
        + " GROUP BY " + groupExpr + " ORDER BY " + sumExpr + " DESC";
    JsonNode pinotResponse = postQuery(query);
    assertNotNull(pinotResponse.get("resultTable").get("rows"));
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      String seqId = _sortedSequenceIds.get(NUM_DOCS_PER_SEGMENT - 1 - i);
      JsonNode row = rows.get(i);
      assertEquals(row.get(0).asText(), "value-k1-" + seqId);
      assertEquals(row.get(1).asDouble(), Double.parseDouble(seqId) * getNumAvroFiles());
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testComplexGroupByQueryV2(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    //Group By Query
    String groupExpr = "jsonExtractScalar(complexMapStr,'$.k1','STRING')";
    String sumExpr = "SUM(jsonExtractScalar(complexMapStr,'$.k4.met','INT'))";
    String query = "SELECT " + groupExpr + ", " + sumExpr + " FROM " + getTableName()
        + " GROUP BY " + groupExpr + " ORDER BY " + sumExpr + " DESC";
    JsonNode pinotResponse = postQuery(query);
    assertNotNull(pinotResponse.get("resultTable").get("rows"));
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      String seqId = String.valueOf(NUM_DOCS_PER_SEGMENT - 1 - i);
      JsonNode row = rows.get(i);
      assertEquals(row.get(0).asText(), "value-k1-" + seqId);
      assertEquals(row.get(1).asDouble(), Double.parseDouble(seqId) * getNumAvroFiles());
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryWithIntegerDefault(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    //Group By Query
    String groupExpr = "jsonExtractScalar(complexMapStr,'$.inExistKey','STRING','defaultKey')";
    String sumExpr = "SUM(jsonExtractScalar(complexMapStr,'$.inExistMet','INT','1'))";
    String query = "SELECT " + groupExpr + ", " + sumExpr + " FROM " + getTableName()
        + " GROUP BY " + groupExpr + " ORDER BY " + sumExpr + " DESC";
    JsonNode pinotResponse = postQuery(query);
    assertNotNull(pinotResponse.get("resultTable").get("rows"));
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.get(0).asText(), "defaultKey");
    assertEquals(row.get(1).asDouble(), 1000.0 * getNumAvroFiles());
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryWithDoubleDefault(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    //Group By Query
    String groupExpr = "jsonExtractScalar(complexMapStr,'$.inExistKey','STRING','defaultKey')";
    String sumExpr = "SUM(jsonExtractScalar(complexMapStr,'$.inExistMet','DOUBLE','0.1'))";
    String query = "SELECT " + groupExpr + ", " + sumExpr + " FROM " + getTableName()
        + " GROUP BY " + groupExpr + " ORDER BY " + sumExpr + " DESC";
    JsonNode pinotResponse = postQuery(query);
    assertNotNull(pinotResponse.get("resultTable").get("rows"));
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.get(0).asText(), "defaultKey");
    assertTrue(Math.abs(row.get(1).asDouble() - 100.0 * getNumAvroFiles()) < 1e-10);
  }

  @Test(dataProvider = "useBothQueryEngines")
  void testFailedQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT jsonExtractScalar(myMapStr,\"$.k1\",\"STRING\") FROM " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    int expectedStatusCode;
    if (useMultiStageQueryEngine) {
      expectedStatusCode = QueryErrorCode.UNKNOWN_COLUMN.getId();
    } else {
      expectedStatusCode = QueryErrorCode.SQL_PARSING.getId();
    }
    assertEquals(pinotResponse.get("exceptions").get(0).get("errorCode").asInt(), expectedStatusCode);
    assertEquals(pinotResponse.get("numDocsScanned").asInt(), 0);
    assertEquals(pinotResponse.get("totalDocs").asInt(), 0);

    query = "SELECT myMapStr FROM " + getTableName()
        + " WHERE jsonExtractScalar(myMapStr, '$.k1',\"STRING\") = 'value-k1-0'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").get(0).get("errorCode").asInt(), expectedStatusCode);
    assertEquals(pinotResponse.get("numDocsScanned").asInt(), 0);
    assertEquals(pinotResponse.get("totalDocs").asInt(), 0);

    query = "SELECT jsonExtractScalar(myMapStr,\"$.k1\", 'STRING') FROM " + getTableName()
        + " WHERE jsonExtractScalar(myMapStr, '$.k1', 'STRING') = 'value-k1-0'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").get(0).get("errorCode").asInt(), expectedStatusCode);
    assertEquals(pinotResponse.get("numDocsScanned").asInt(), 0);
    assertEquals(pinotResponse.get("totalDocs").asInt(), 0);
  }

  @Test
  public void testJsonPathCache() {
    Cache cache = CacheProvider.getCache();
    assertTrue(cache instanceof JsonPathCache);
    assertTrue(((JsonPathCache) cache).size() > 0);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonKeysQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT jsonExtractKey(myMapStr, '$.*', 'maxDepth=1') FROM " + getTableName() + " LIMIT 1";
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 1);
    // JsonPath returns keys in JsonPath format like "$['key']"
    JsonNode keys = row.get(0);
    assertTrue(keys.isArray());
    assertFalse(keys.isEmpty());

    query = "SELECT jsonExtractKey(complexMapStr, '$.*', 'maxDepth=2') FROM " + getTableName() + " LIMIT 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 1);
    keys = row.get(0);
    assertTrue(keys.isArray());
    assertFalse(keys.isEmpty());

    query = "SELECT jsonExtractKey(complexMapStr, '$.*', 'maxDepth=3') FROM " + getTableName() + " LIMIT 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 1);
    keys = row.get(0);
    assertTrue(keys.isArray());
    assertFalse(keys.isEmpty());
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonKeysQueriesWithDotNotation(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Test optional parameter jsonExtractKey with dotNotation=true (simple JSON)
    String query =
        "SELECT jsonExtractKey(myMapStr, '$.*', 'maxDepth=1; dotNotation=true') FROM " + getTableName() + " LIMIT 1";
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 1);
    JsonNode keys = row.get(0);
    assertTrue(keys.isArray());
    assertEquals(keys.size(), 2); // k1, k2
    // Should contain simple key names, not JsonPath format
    List<String> keyList = new ArrayList<>();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    assertTrue(keyList.contains("k1"));
    assertTrue(keyList.contains("k2"));
    // Should NOT contain JsonPath format like "$['k1']"
    assertFalse(keyList.contains("$['k1']"));
    assertFalse(keyList.contains("$['k2']"));

    // Test optional parameter jsonExtractKey with dotNotation=false (JsonPath format)
    query =
        "SELECT jsonExtractKey(myMapStr, '$.*', 'maxDepth=1; dotNotation=false') FROM " + getTableName() + " LIMIT 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    assertTrue(keys.isArray());
    assertEquals(keys.size(), 2);
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain JsonPath format
    assertTrue(keyList.contains("$['k1']"));
    assertTrue(keyList.contains("$['k2']"));

    // Test recursive key extraction with dot notation on complex JSON
    query = "SELECT jsonExtractKey(complexMapStr, '$..**', 'maxDepth=2; dotNotation=true') FROM " + getTableName()
        + " LIMIT 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    assertTrue(keys.isArray());
    assertTrue(keys.size() >= 4); // At least k1, k2, k3, k4
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain top-level keys in dot notation
    assertTrue(keyList.contains("k1"));
    assertTrue(keyList.contains("k2"));
    assertTrue(keyList.contains("k3"));
    assertTrue(keyList.contains("k4"));

    // Test recursive key extraction with JsonPath format
    query = "SELECT jsonExtractKey(complexMapStr, '$..**', 'maxDepth=2; dotNotation=false') FROM " + getTableName()
        + " LIMIT 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    assertTrue(keys.isArray());
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain JsonPath format
    assertTrue(keyList.contains("$['k1']"));
    assertTrue(keyList.contains("$['k2']"));
    assertTrue(keyList.contains("$['k3']"));
    assertTrue(keyList.contains("$['k4']"));

    // Test deeper recursive extraction with dot notation
    query = "SELECT jsonExtractKey(complexMapStr, '$..**', 'maxDepth=3; dotNotation=true') FROM " + getTableName()
        + " LIMIT 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    assertTrue(keys.isArray());
    assertTrue(keys.size() > 4); // Should include nested keys
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain nested keys in dot notation
    assertTrue(keyList.contains("k4.k4-k1"));
    assertTrue(keyList.contains("k4.k4-k2"));
    assertTrue(keyList.contains("k4.k4-k3"));
    assertTrue(keyList.contains("k4.met"));
    // Should contain array indices in dot notation
    assertTrue(keyList.contains("k3.0"));
    assertTrue(keyList.contains("k3.1"));
    assertTrue(keyList.contains("k3.2"));

    // Test deeper recursive extraction with JsonPath format
    query = "SELECT jsonExtractKey(complexMapStr, '$..**', 'maxDepth=3; dotNotation=false') FROM " + getTableName()
        + " LIMIT 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    assertTrue(keys.isArray());
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain nested keys in JsonPath format
    assertTrue(keyList.contains("$['k4']['k4-k1']"));
    assertTrue(keyList.contains("$['k4']['k4-k2']"));
    assertTrue(keyList.contains("$['k4']['k4-k3']"));
    assertTrue(keyList.contains("$['k4']['met']"));
    // Should contain array indices in JsonPath format
    assertTrue(keyList.contains("$['k3'][0]"));
    assertTrue(keyList.contains("$['k3'][1]"));
    assertTrue(keyList.contains("$['k3'][2]"));

    // Test specific path extraction with dot notation
    query = "SELECT jsonExtractKey(complexMapStr, '$.k4.*', 'maxDepth=2; dotNotation=true') FROM " + getTableName()
        + " LIMIT 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    assertTrue(keys.isArray());
    assertEquals(keys.size(), 4); // k4-k1, k4-k2, k4-k3, met
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should contain nested keys in dot notation format
    assertTrue(keyList.contains("k4.k4-k1"));
    assertTrue(keyList.contains("k4.k4-k2"));
    assertTrue(keyList.contains("k4.k4-k3"));
    assertTrue(keyList.contains("k4.met"));

    // Test backward compatibility - 2-parameter version should default to JsonPath format
    query = "SELECT jsonExtractKey(myMapStr, '$.*') FROM " + getTableName() + " LIMIT 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    assertTrue(keys.isArray());
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should default to JsonPath format
    assertTrue(keyList.contains("$['k1']"));
    assertTrue(keyList.contains("$['k2']"));

    // Test backward compatibility - no dotNotation should default to JsonPath format
    query = "SELECT jsonExtractKey(myMapStr, '$.*', 'maxDepth=1') FROM " + getTableName() + " LIMIT 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    row = rows.get(0);
    keys = row.get(0);
    assertTrue(keys.isArray());
    keyList.clear();
    for (JsonNode key : keys) {
      keyList.add(key.asText());
    }
    // Should default to JsonPath format
    assertTrue(keyList.contains("$['k1']"));
    assertTrue(keyList.contains("$['k2']"));
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

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10000";
    JsonNode response = postQuery(query);
    assertEquals(response.get("exceptions").size(), 0);
    List<String> values = extractOrderedDistinctValues(response);
    assertFalse(values.isEmpty(),
        "Baseline (operator disabled) should return distinct values. Engine=" + (useMultiStageQueryEngine ? "MSE"
            : "SSE"));
  }

  /**
   * With useIndexBasedDistinctOperator, JsonIndexDistinctOperator produces same results as baseline.
   * Compares ordered rows (not just sets) to verify ORDER BY semantics. The numEntriesScannedPostFilter assertion
   * pins the operator's per-value iteration: one increment per entry in the value-to-docs map, so the expected
   * count is NUM_DISTINCT_K1 * getNumAvroFiles().
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctOperatorWithPinotJsonIndex(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    assertEquals(optimizedRows, baselineRows,
        "JsonIndexDistinctOperator should produce same ordered results as baseline. " + "Engine=" + (
            useMultiStageQueryEngine ? "MSE" : "SSE"));

    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(), NUM_DISTINCT_K1 * getNumAvroFiles());
  }

  /**
   * JsonIndexDistinctOperator with a WHERE filter on a different path produces the same ordered results as the
   * baseline. The operator still iterates every entry in the $.k1 value-to-docs map (the WHERE filter is applied
   * via per-entry bitmap intersection, not by shrinking the map), so numEntriesScannedPostFilter is
   * NUM_DISTINCT_K1 * getNumAvroFiles() — same as the no-filter case.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctOperatorWithFilter(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String k1Expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k1', 'STRING')";
    String k2Expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.k2', 'STRING')";
    String query = "SELECT DISTINCT " + k1Expr + " FROM " + getTableName() + " WHERE " + k2Expr + " = 'value-k2-0'"
        + " ORDER BY " + k1Expr + " LIMIT 10000";
    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    assertEquals(optimizedRows, baselineRows,
        "JsonIndexDistinctOperator with filter should match baseline. Engine=" + (useMultiStageQueryEngine ? "MSE"
            : "SSE"));

    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(), NUM_DISTINCT_K1 * getNumAvroFiles());
  }

  /**
   * Verifies that JsonIndexDistinctOperator correctly materializes the defaultValue for docs WHERE the JSON path
   * is absent, matching baseline JsonExtractIndexTransformFunction behavior.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctOperatorWithDefaultValue(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Query a non-existent path with a defaultValue — all docs should produce the default
    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.nonexistent', 'STRING', 'N/A')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10";

    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    assertEquals(optimizedRows, baselineRows,
        "JsonIndexDistinctOperator with defaultValue should match baseline. Engine=" + (useMultiStageQueryEngine ? "MSE"
            : "SSE"));
    assertTrue(optimizedRows.contains("N/A"),
        "defaultValue 'N/A' should appear in results for non-existent path. Engine=" + (useMultiStageQueryEngine ? "MSE"
            : "SSE"));

    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(), 0);
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
    String query = "SELECT DISTINCT jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME + ", '$.nonexistent', 'STRING') FROM "
        + getTableName() + " LIMIT 10";

    // Baseline also throws for missing path without defaultValue
    JsonNode baselineResponse = postQuery(query);
    assertFalse(baselineResponse.get("exceptions").isEmpty(),
        "Baseline should throw for missing JSON path without defaultValue");

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertFalse(optimizedResponse.get("exceptions").isEmpty(),
        "JsonIndexDistinctOperator should throw for missing JSON path without defaultValue");
  }

  // --- 5-arg jsonExtractIndex(column, path, type, default, filterJsonExpression) tests ---
  //
  // The 5-arg form pushes the JSON_MATCH-style filter into the JSON-index lookup itself. Each filter that doesn't
  // match every doc causes `handleMissingDocs` (or the transform's per-row default branch) to add the literal
  // default to the distinct set, so the expected result is `{matching values} ∪ {default}`.

  /// REGEXP_LIKE pushed down via the 5-arg filterJsonExpression.
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathRegexpLike(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.k1', 'STRING', 'missing', 'REGEXP_LIKE(\"$.k1\", ''value-k1-[0-9]'')')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    assertEquals(optimizedRows, baselineRows, "5-arg REGEXP_LIKE should match baseline");
    // Single-digit suffix matches value-k1-0..value-k1-9 (10 values); non-matching docs add 'missing'.
    assertEquals(optimizedRows.size(), 11);
    assertTrue(optimizedRows.contains("missing"));

    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(), 10 * getNumAvroFiles());
  }

  /// EQ pushed down via the 5-arg filterJsonExpression.
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathEq(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.k1', 'STRING', 'missing', '\"$.k1\" = ''value-k1-0''')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    assertEquals(optimizedRows, baselineRows, "5-arg EQ should match baseline");
    assertEquals(optimizedRows, List.of("missing", "value-k1-0"));

    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(), getNumAvroFiles());
  }

  /// NOT_EQ pushed down via the 5-arg filterJsonExpression.
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathNotEq(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.k1', 'STRING', 'missing', '\"$.k1\" != ''value-k1-0''')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    assertEquals(optimizedRows, baselineRows, "5-arg NOT_EQ should match baseline");
    // 99 matching k1 values (everything except value-k1-0) + 'missing' for the excluded docs.
    assertEquals(optimizedRows.size(), NUM_DISTINCT_K1);
    assertFalse(optimizedRows.contains("value-k1-0"));
    assertTrue(optimizedRows.contains("missing"));

    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(),
        (NUM_DISTINCT_K1 - 1) * getNumAvroFiles());
  }

  /// IN pushed down via the 5-arg filterJsonExpression.
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathIn(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.k1', 'STRING', 'missing', '\"$.k1\" IN (''value-k1-0'', ''value-k1-1'', ''value-k1-2'')')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    assertEquals(optimizedRows, baselineRows, "5-arg IN should match baseline");
    assertEquals(optimizedRows, List.of("missing", "value-k1-0", "value-k1-1", "value-k1-2"));

    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(), 3 * getNumAvroFiles());
  }

  /// NOT_IN pushed down via the 5-arg filterJsonExpression.
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathNotIn(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.k1', 'STRING', 'missing', '\"$.k1\" NOT IN (''value-k1-0'', ''value-k1-1'')')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    assertEquals(optimizedRows, baselineRows, "5-arg NOT_IN should match baseline");
    // 98 matching k1 values + 'missing' for the excluded docs.
    assertEquals(optimizedRows.size(), NUM_DISTINCT_K1 - 1);
    assertFalse(optimizedRows.contains("value-k1-0"));
    assertFalse(optimizedRows.contains("value-k1-1"));
    assertTrue(optimizedRows.contains("missing"));

    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(),
        (NUM_DISTINCT_K1 - 2) * getNumAvroFiles());
  }

  /// IS NOT NULL pushed down via the 5-arg filterJsonExpression. The filter matches every doc (every row has
  /// `$.k1`), so the literal default is never added and the result is exactly the distinct `$.k1` set.
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathIsNotNull(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.k1', 'STRING', 'missing', '\"$.k1\" IS NOT NULL')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    assertEquals(optimizedRows, baselineRows, "5-arg IS NOT NULL should match baseline");
    assertEquals(optimizedRows.size(), NUM_DISTINCT_K1);
    assertFalse(optimizedRows.contains("missing"),
        "Filter matches every doc, so the default literal should never be added");

    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(), NUM_DISTINCT_K1 * getNumAvroFiles());
  }

  /// 5-arg filterJsonExpression with LIMIT (no ORDER BY): only the LIMIT row-count is enforced.
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSamePathWithLimit(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.k1', 'STRING', 'missing', '\"$.k1\" IS NOT NULL')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " LIMIT 5";

    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    assertEquals(extractOrderedDistinctValues(baselineResponse).size(), 5);
    assertEquals(extractOrderedDistinctValues(optimizedResponse).size(), 5);
    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(), 5 * getNumAvroFiles());
  }

  /// Cross-path 5-arg form: filter on `$.k2`, extract `$.k1`. `getMatchingFlattenedDocsMap` applies the filter
  /// independently of the extracted path, so the returned value-to-docs map holds the `$.k1` values for only the
  /// docs satisfying `$.k2 = 'value-k2-0'`. `$.k2` is unique across the segment, so exactly one doc matches per
  /// segment, and that doc's `$.k1` is `value-k1-0`. Every other doc falls through to the literal default.
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctCrossPathFilter(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.k1', 'STRING', 'missing', '\"$.k2\" = ''value-k2-0''')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10000";

    JsonNode baselineResponse = postQuery(query);
    assertTrue(baselineResponse.get("exceptions").isEmpty());

    JsonNode optimizedResponse = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(optimizedResponse.get("exceptions").isEmpty());

    List<String> baselineRows = extractOrderedDistinctValues(baselineResponse);
    List<String> optimizedRows = extractOrderedDistinctValues(optimizedResponse);
    assertEquals(optimizedRows, baselineRows, "Cross-path 5-arg filter should match baseline");
    assertEquals(optimizedRows, List.of("missing", "value-k1-0"));

    assertEquals(optimizedResponse.get("numEntriesScannedPostFilter").asInt(), getNumAvroFiles());
  }

  /// The new `jsonIndexDistinctSkipMissingPath` query option suppresses `handleMissingDocs`, so the literal default
  /// never appears in the result — even when the 5-arg filter excludes most docs. Same EQ-filter query as
  /// `testJsonIndexDistinctSamePathEq`: without the option the operator returns `[missing, value-k1-0]`; with the
  /// option it collapses to `[value-k1-0]`.
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctSkipMissingPath(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String expr = "jsonExtractIndex(" + MY_MAP_STR_FIELD_NAME
        + ", '$.k1', 'STRING', 'missing', '\"$.k1\" = ''value-k1-0''')";
    String query = "SELECT DISTINCT " + expr + " FROM " + getTableName() + " ORDER BY " + expr + " LIMIT 10000";

    // Operator without skip: same shape as testJsonIndexDistinctSamePathEq — default appears.
    JsonNode withoutSkip = postQueryWithOptions(query, OPT_USE_INDEX);
    assertTrue(withoutSkip.get("exceptions").isEmpty());
    assertEquals(extractOrderedDistinctValues(withoutSkip), List.of("missing", "value-k1-0"));

    // Operator with skip: default is never added.
    JsonNode withSkip = postQueryWithOptions(query, OPT_USE_INDEX_SKIP_MISSING_PATH);
    assertTrue(withSkip.get("exceptions").isEmpty());
    assertEquals(extractOrderedDistinctValues(withSkip), List.of("value-k1-0"));

    assertEquals(withSkip.get("numEntriesScannedPostFilter").asInt(), getNumAvroFiles());
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
