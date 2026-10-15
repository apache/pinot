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
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// End-to-end test of the cluster config that rewrites exact aggregations into their approximate counterparts. It
/// covers the two properties unit tests cannot show: the config takes effect on a running broker without a restart,
/// and the rewrite is invisible to the caller apart from the flag on the response.
public class ApproximateFunctionOverrideIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String DISTINCT_COUNT_QUERY =
      "SELECT Carrier, DISTINCTCOUNT(AirlineID) FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 100";
  private static final String COUNT_DISTINCT_QUERY =
      "SELECT Carrier, COUNT(DISTINCT AirlineID) FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 100";
  private static final String PERCENTILE_QUERY =
      "SELECT Carrier, PERCENTILE(ArrDelay, 90) FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 100";

  private HelixConfigScope _clusterScope;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();
    _clusterScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);
    waitForAllDocsLoaded(600_000L);
  }

  /// Clears the config after every method, so that a method that fails part way cannot make the next one fail too.
  @AfterMethod
  public void clearOverride() {
    clearApproximateFunctionOverride();
    waitForOverride(false);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testClusterConfigAppliesWithoutRestart(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode exactDistinctCount = postQuery(DISTINCT_COUNT_QUERY);
    JsonNode exactPercentile = postQuery(PERCENTILE_QUERY);
    assertApplied(exactDistinctCount, false);
    assertApplied(exactPercentile, false);

    // A threshold far above the cardinality of the test data keeps the rewritten functions exact, which is what makes
    // the answers comparable before and after the flip.
    setApproximateFunctionOverride("threshold=1000000");
    waitForOverride(true);

    JsonNode distinctCount = postQuery(DISTINCT_COUNT_QUERY);
    assertApplied(distinctCount, true);
    assertSameResult(exactDistinctCount, distinctCount);
    JsonNode percentile = postQuery(PERCENTILE_QUERY);
    assertApplied(percentile, true);
    assertSameResult(exactPercentile, percentile);

    // COUNT(DISTINCT x) is the standard SQL spelling and must be rewritten too.
    assertApplied(postQuery(COUNT_DISTINCT_QUERY), true);

    // The query option is the escape hatch back to exact results.
    JsonNode forcedExact = postQuery("SET useApproximateFunction = false; " + DISTINCT_COUNT_QUERY);
    assertApplied(forcedExact, false);
    assertSameResult(exactDistinctCount, forcedExact);

    // A threshold below the cardinality has to make the answer approximate rather than fail, keeping the same shape.
    // This is also what proves the parameters reach the servers and parse there.
    setApproximateFunctionOverride("threshold=1");
    waitForOverride(true);
    for (JsonNode exact : List.of(exactDistinctCount, exactPercentile)) {
      String query = exact == exactDistinctCount ? DISTINCT_COUNT_QUERY : PERCENTILE_QUERY;
      JsonNode approximate = postQuery(query);
      assertApplied(approximate, true);
      assertEquals(approximate.get("exceptions").size(), 0, approximate.toString());
      assertEquals(columnDataTypes(approximate), columnDataTypes(exact));
      assertEquals(rows(approximate).size(), rows(exact).size());
    }
  }

  /// Pins the one visible difference the rewrite makes to a single-stage response, so that changing it is a
  /// deliberate decision. The multi-stage engine keeps the Calcite-derived column name.
  @Test
  public void testSingleStageRenamesTheColumn()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    assertEquals(columnName(postQuery(DISTINCT_COUNT_QUERY)), "distinctcount(AirlineID)");

    setApproximateFunctionOverride("threshold=1000000");
    waitForOverride(true);
    assertEquals(columnName(postQuery(DISTINCT_COUNT_QUERY)), "distinctcountsmarthll(AirlineID)");

    // An explicit alias is the way to keep the old name.
    assertEquals(columnName(postQuery(
        "SELECT Carrier, DISTINCTCOUNT(AirlineID) AS dc FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 100")),
        "dc");
  }

  /// Compares the part of the result the rewrite must not change: the values and their types. The column name is
  /// excluded on purpose, see [#testSingleStageRenamesTheColumn].
  private static void assertSameResult(JsonNode expected, JsonNode actual) {
    assertEquals(columnDataTypes(actual), columnDataTypes(expected));
    assertEquals(rows(actual), rows(expected));
  }

  private static void assertApplied(JsonNode response, boolean expected) {
    assertEquals(response.get("approximateFunctionApplied").asBoolean(), expected, response.toString());
  }

  private static JsonNode columnDataTypes(JsonNode response) {
    return response.get("resultTable").get("dataSchema").get("columnDataTypes");
  }

  private static JsonNode rows(JsonNode response) {
    return response.get("resultTable").get("rows");
  }

  private static String columnName(JsonNode response) {
    return response.get("resultTable").get("dataSchema").get("columnNames").get(1).asText();
  }

  private void setApproximateFunctionOverride(String params) {
    _helixManager.getConfigAccessor().set(_clusterScope, Broker.USE_APPROXIMATE_FUNCTION, "true");
    _helixManager.getConfigAccessor().set(_clusterScope, Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS, params);
    _helixManager.getConfigAccessor().set(_clusterScope, Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS, params);
  }

  /// Removes the keys, so the broker falls back to its own conf.
  private void clearApproximateFunctionOverride() {
    _helixManager.getConfigAccessor().remove(_clusterScope, Broker.USE_APPROXIMATE_FUNCTION);
    _helixManager.getConfigAccessor().remove(_clusterScope, Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS);
    _helixManager.getConfigAccessor().remove(_clusterScope, Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS);
  }

  /// The broker picks the change up from a ZooKeeper watch, so the test waits for it instead of restarting anything.
  private void waitForOverride(boolean expected) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return postQuery(DISTINCT_COUNT_QUERY).get("approximateFunctionApplied").asBoolean() == expected;
      } catch (Exception e) {
        return false;
      }
    }, 100L, 60_000L, "Broker did not pick up the approximate function cluster config");
  }
}
