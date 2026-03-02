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
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration tests for JsonIndexDistinctOperator with standard Pinot JSON index.
 * <p>
 * JsonIndexDistinctOperator is disabled by default and must be enabled via query option
 * {@code useJsonIndexDistinct=true}. This test verifies both baseline (default DistinctOperator)
 * and optimized (JsonIndexDistinctOperator) produce the same results.
 * </p>
 * Tests both Single-Stage Engine (SSE) and Multi-Stage Engine (MSE).
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class JsonIndexDistinctOperatorIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME = "JsonIndexDistinctTest";
  private static final String JSON_FIELD = "myMapStr";
  private static final int NUM_DOCS_PER_SEGMENT = 1000;
  private static final int NUM_DISTINCT_K1 = 100; // k1 = value-k1-(i % 100)

  @Override
  protected long getCountStarResult() {
    return (long) NUM_DOCS_PER_SEGMENT * getNumAvroFiles();
  }

  @Override
  public org.apache.pinot.spi.data.Schema createSchema() {
    return new org.apache.pinot.spi.data.Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(JSON_FIELD, FieldSpec.DataType.STRING).build();
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setJsonIndexColumns(Collections.singletonList(JSON_FIELD))
        .setIngestionConfig(ingestionConfig)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    Schema avroSchema = Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(Collections.singletonList(
        new Schema.Field(JSON_FIELD, Schema.create(Schema.Type.STRING), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      for (int i = 0; i < NUM_DOCS_PER_SEGMENT; i++) {
        Map<String, String> map = new HashMap<>();
        map.put("k1", "value-k1-" + (i % NUM_DISTINCT_K1));
        map.put("k2", "value-k2-" + i);
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(JSON_FIELD, JsonUtils.objectToString(map));
        for (DataFileWriter<GenericData.Record> writer : avroFilesAndWriters.getWriters()) {
          writer.append(record);
        }
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  /**
   * Verifies that without useJsonIndexDistinct (operator disabled by default), the query uses
   * default DistinctOperator and returns correct results.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctOperatorDisabledByDefault(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // No query option = operator disabled, uses default DistinctOperator
    String query = "SELECT DISTINCT jsonExtractIndex(" + JSON_FIELD + ", '$.k1', 'STRING') FROM " + getTableName()
        + " ORDER BY jsonExtractIndex(" + JSON_FIELD + ", '$.k1', 'STRING') LIMIT 10000";
    JsonNode response = postQuery(query);
    Assert.assertEquals(response.get("exceptions").size(), 0);

    Set<String> values = extractDistinctValues(response);
    Assert.assertEquals(values.size(), NUM_DISTINCT_K1,
        "Baseline (operator disabled) should return " + NUM_DISTINCT_K1 + " distinct values. Engine="
            + (useMultiStageQueryEngine ? "MSE" : "SSE"));
    for (int i = 0; i < NUM_DISTINCT_K1; i++) {
      Assert.assertTrue(values.contains("value-k1-" + i), "Missing value-k1-" + i);
    }
  }

  /**
   * Verifies that with useJsonIndexDistinct=true, JsonIndexDistinctOperator produces same results
   * as the baseline (default DistinctOperator).
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctOperatorWithPinotJsonIndex(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + JSON_FIELD + ", '$.k1', 'STRING') FROM " + getTableName()
        + " ORDER BY jsonExtractIndex(" + JSON_FIELD + ", '$.k1', 'STRING') LIMIT 10000";

    // Baseline: operator disabled (default)
    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);
    Set<String> baselineValues = extractDistinctValues(baselineResponse);

    // With useJsonIndexDistinct=true: JsonIndexDistinctOperator (opt-in)
    JsonNode optimizedResponse =
        postQueryWithOptions(query, QueryOptionKey.USE_JSON_INDEX_DISTINCT + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);
    Set<String> optimizedValues = extractDistinctValues(optimizedResponse);

    Assert.assertEquals(optimizedValues, baselineValues,
        "JsonIndexDistinctOperator (useJsonIndexDistinct=true) should produce same results as baseline. "
            + "Engine=" + (useMultiStageQueryEngine ? "MSE" : "SSE"));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonIndexDistinctOperatorWithFilter(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT DISTINCT jsonExtractIndex(" + JSON_FIELD + ", '$.k1', 'STRING') FROM " + getTableName()
        + " WHERE jsonExtractIndex(" + JSON_FIELD + ", '$.k2', 'STRING') = 'value-k2-0'"
        + " ORDER BY jsonExtractIndex(" + JSON_FIELD + ", '$.k1', 'STRING') LIMIT 10000";
    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);
    Set<String> baselineValues = extractDistinctValues(baselineResponse);

    JsonNode optimizedResponse = postQueryWithOptions(query, QueryOptionKey.USE_JSON_INDEX_DISTINCT + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);
    Set<String> optimizedValues = extractDistinctValues(optimizedResponse);

    Assert.assertEquals(optimizedValues, baselineValues,
        "JsonIndexDistinctOperator with filter should match baseline. Engine="
            + (useMultiStageQueryEngine ? "MSE" : "SSE"));
  }

  private static Set<String> extractDistinctValues(JsonNode response) {
    Set<String> values = new HashSet<>();
    JsonNode rows = response.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      JsonNode cell = rows.get(i).get(0);
      values.add(cell.isNull() ? null : cell.asText());
    }
    return values;
  }
}
