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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests for InvertedIndexDistinctOperator.
 * <p>
 * InvertedIndexDistinctOperator is disabled by default and must be enabled via query option
 * {@code useInvertedIndexDistinct=true}.
 * </p>
 * Tests both Single-Stage Engine (SSE) and Multi-Stage Engine (MSE).
 */
public class InvertedIndexDistinctOperatorIntegrationTest extends BaseClusterIntegrationTestSet {

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload schema and table config (uses default On_Time data with inverted index on Origin)
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    setUpH2Connection(avroFiles);
    waitForAllDocsLoaded(600_000L);
  }

  /**
   * Verifies that without useInvertedIndexDistinct (operator disabled by default), the query uses
   * default DistinctOperator and returns correct results.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testInvertedIndexDistinctOperatorDisabledByDefault(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // No query option = operator disabled, uses default DistinctOperator
    String query = "SELECT DISTINCT Origin FROM mytable ORDER BY Origin";
    JsonNode response = postQuery(query);
    Assert.assertEquals(response.get("exceptions").size(), 0);
    Set<String> values = extractDistinctValues(response);
    Assert.assertFalse(values.isEmpty(), "Baseline (operator disabled) should return distinct values");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testInvertedIndexDistinctOperator(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Baseline: SELECT DISTINCT Origin without the optimization
    String query = "SELECT DISTINCT Origin FROM mytable ORDER BY Origin";
    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);
    Set<String> baselineValues = extractDistinctValues(baselineResponse);

    // With useInvertedIndexDistinct=true - should use InvertedIndexDistinctOperator
    String queryWithOption = "SELECT DISTINCT Origin FROM mytable ORDER BY Origin";
    JsonNode optimizedResponse =
        postQueryWithOptions(queryWithOption, QueryOptionKey.USE_INVERTED_INDEX_DISTINCT + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);
    Set<String> optimizedValues = extractDistinctValues(optimizedResponse);

    Assert.assertEquals(optimizedValues, baselineValues,
        "InvertedIndexDistinctOperator (useInvertedIndexDistinct=true) should produce same results as baseline. "
            + "Engine=" + (useMultiStageQueryEngine ? "MSE" : "SSE"));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testInvertedIndexDistinctOperatorWithFilter(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query =
        "SELECT DISTINCT Origin FROM mytable WHERE Carrier = 'AA' ORDER BY Origin";
    JsonNode baselineResponse = postQuery(query);
    Assert.assertEquals(baselineResponse.get("exceptions").size(), 0);
    Set<String> baselineValues = extractDistinctValues(baselineResponse);

    JsonNode optimizedResponse = postQueryWithOptions(query,
        QueryOptionKey.USE_INVERTED_INDEX_DISTINCT + "=true");
    Assert.assertEquals(optimizedResponse.get("exceptions").size(), 0);
    Set<String> optimizedValues = extractDistinctValues(optimizedResponse);

    Assert.assertEquals(optimizedValues, baselineValues,
        "InvertedIndexDistinctOperator with filter should match baseline. Engine="
            + (useMultiStageQueryEngine ? "MSE" : "SSE"));
  }

  private static Set<String> extractDistinctValues(JsonNode response) {
    Set<String> values = new HashSet<>();
    JsonNode rows = response.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      values.add(rows.get(i).get(0).asText());
    }
    return values;
  }
}
