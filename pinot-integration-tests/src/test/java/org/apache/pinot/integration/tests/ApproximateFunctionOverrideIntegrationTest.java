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
import java.util.Locale;
import java.util.Map;
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
import static org.testng.Assert.assertFalse;


/// End-to-end test of the cluster config that rewrites exact aggregations into their approximate counterparts. It
/// covers the properties unit tests cannot show: the config takes effect on a running broker without a restart, the
/// rewrite is invisible to the caller while the rewritten functions stay exact, and the `approximatedResult` flag on
/// the response reports a result that turned approximate.
public class ApproximateFunctionOverrideIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String DISTINCT_COUNT_QUERY =
      "SELECT Carrier, DISTINCTCOUNT(Origin) FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 100";
  private static final String COUNT_DISTINCT_QUERY =
      "SELECT Carrier, COUNT(DISTINCT Origin) FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 100";
  private static final String PERCENTILE_QUERY =
      "SELECT Carrier, PERCENTILE(ArrDelay, 90) FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 100";
  // The last predicate matches no row, so the exact multi-stage count is 0 and a counted NULL would show up as 1.
  private static final String CONDITIONAL_DISTINCT_COUNT_QUERY = "SELECT Carrier, "
      + "COUNT(DISTINCT CASE WHEN ArrDelay > 0 THEN Origin ELSE NULL END), "
      + "DISTINCTCOUNT(CASE WHEN ArrDelay > 0 THEN Origin ELSE NULL END), "
      + "COUNT(DISTINCT CASE WHEN ArrDelay > 1000000 THEN Origin ELSE NULL END) "
      + "FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 100";

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
    assertApproximated(exactDistinctCount, false);
    assertApproximated(exactPercentile, false);

    // A threshold far above the cardinality of the test data keeps the rewritten functions exact, which is what makes
    // the answers comparable before and after the flip. The results stay exact, so the flag stays off.
    setApproximateFunctionOverride("threshold=1000000");
    waitForOverride(true);

    JsonNode distinctCount = postQuery(DISTINCT_COUNT_QUERY);
    assertApproximated(distinctCount, false);
    assertSameResult(exactDistinctCount, distinctCount);
    JsonNode percentile = postQuery(PERCENTILE_QUERY);
    assertApproximated(percentile, false);
    assertSameResult(exactPercentile, percentile);

    // A threshold below the cardinality has to make the answer approximate rather than fail, keeping the same shape,
    // and the flag reports it. This is also what proves the parameters reach the servers and parse there.
    setApproximateFunctionOverride("threshold=1");
    waitForApproximatedResult();

    // COUNT(DISTINCT x) is the standard SQL spelling and must be rewritten too.
    assertApproximated(postQuery(COUNT_DISTINCT_QUERY), true);

    // The query option is the escape hatch back to exact results.
    JsonNode forcedExact = postQuery("SET useApproximateFunction = false; " + DISTINCT_COUNT_QUERY);
    assertApproximated(forcedExact, false);
    assertSameResult(exactDistinctCount, forcedExact);

    for (JsonNode exact : List.of(exactDistinctCount, exactPercentile)) {
      String query = exact == exactDistinctCount ? DISTINCT_COUNT_QUERY : PERCENTILE_QUERY;
      JsonNode approximate = postQuery(query);
      assertApproximated(approximate, true);
      assertEquals(approximate.get("exceptions").size(), 0, approximate.toString());
      assertEquals(columnDataTypes(approximate), columnDataTypes(exact));
      assertEquals(rows(approximate).size(), rows(exact).size());
    }
  }

  /// The flag reports the result, however the SMART function got into the query: a query that asks for one itself
  /// sets it too, once the function switches to a sketch.
  @Test(dataProvider = "useBothQueryEngines")
  public void testExplicitSmartFunction(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (int threshold : new int[]{1, 1000000}) {
      JsonNode response = postQuery("SELECT Carrier, DISTINCTCOUNTSMARTHLL(Origin, 'threshold=" + threshold
          + "') FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 100");
      assertEquals(response.get("exceptions").size(), 0, response.toString());
      assertApproximated(response, threshold == 1);
    }
  }

  /// The multi-stage engine turns `COUNT(DISTINCT CASE WHEN c THEN x END)` into `COUNT(DISTINCT x) FILTER (WHERE c)`,
  /// and the rewrite must not stop that, or the NULL of every row that fails `c` counts as one more value. The
  /// single-stage engine has no such conversion, but its answer must not change either.
  @Test(dataProvider = "useBothQueryEngines")
  public void testConditionalDistinctCountMatchesExact(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode exact = postQuery(CONDITIONAL_DISTINCT_COUNT_QUERY);
    assertEquals(exact.get("exceptions").size(), 0, exact.toString());
    assertFalse(rows(exact).isEmpty(), exact.toString());

    // The threshold keeps the rewritten functions exact, so the flag stays off.
    setApproximateFunctionOverride("threshold=1000000");
    waitForOverride(true);
    JsonNode approximate = postQuery(CONDITIONAL_DISTINCT_COUNT_QUERY);
    assertEquals(approximate.get("exceptions").size(), 0, approximate.toString());
    assertApproximated(approximate, false);
    assertSameResult(exact, approximate);
    if (useMultiStageQueryEngine) {
      for (JsonNode row : rows(approximate)) {
        assertEquals(row.get(3).asLong(), 0L, approximate.toString());
      }
    }
  }

  /// Pins the one visible difference the rewrite makes to a single-stage response, so that changing it is a
  /// deliberate decision. The multi-stage engine keeps the Calcite-derived column name.
  @Test
  public void testSingleStageRenamesTheColumn()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    assertEquals(columnName(postQuery(DISTINCT_COUNT_QUERY)), "distinctcount(Origin)");

    setApproximateFunctionOverride("threshold=1000000");
    waitForOverride(true);
    assertEquals(columnName(postQuery(DISTINCT_COUNT_QUERY)), "distinctcountsmarthll(Origin)");

    // An explicit alias is the way to keep the old name.
    assertEquals(columnName(postQuery(
        "SELECT Carrier, DISTINCTCOUNT(Origin) AS dc FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 100")),
        "dc");
  }

  /// Compares the part of the result the rewrite must not change: the values and their types. The column name is
  /// excluded on purpose, see [#testSingleStageRenamesTheColumn].
  private static void assertSameResult(JsonNode expected, JsonNode actual) {
    assertEquals(columnDataTypes(actual), columnDataTypes(expected));
    assertEquals(rows(actual), rows(expected));
  }

  private static void assertApproximated(JsonNode response, boolean expected) {
    assertEquals(response.get("approximatedResult").asBoolean(), expected, response.toString());
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

  /// The rewrite is already on, so [#waitForOverride] would return before the new threshold arrives. Waits until both
  /// engines report an approximated percentile instead. [#setApproximateFunctionOverride] writes all keys at once, so
  /// the distinct count params have arrived too by then.
  private void waitForApproximatedResult() {
    boolean useMultiStageQueryEngine = useMultiStageQueryEngine();
    try {
      TestUtils.waitForCondition(aVoid -> {
        try {
          setUseMultiStageQueryEngine(false);
          boolean singleStage = postQuery(PERCENTILE_QUERY).get("approximatedResult").asBoolean();
          setUseMultiStageQueryEngine(true);
          boolean multiStage = postQuery(PERCENTILE_QUERY).get("approximatedResult").asBoolean();
          return singleStage && multiStage;
        } catch (Exception e) {
          return false;
        }
      }, 100L, 60_000L, "Broker did not pick up the new conversion threshold");
    } finally {
      setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    }
  }

  /// Writes all keys at once, so the broker cannot see the flag on before the parameters arrive.
  private void setApproximateFunctionOverride(String params) {
    _helixManager.getConfigAccessor().set(_clusterScope, Map.of(Broker.USE_APPROXIMATE_FUNCTION, "true",
        Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS, params, Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS,
        params));
  }

  /// Removes the keys, so the broker falls back to its own conf.
  private void clearApproximateFunctionOverride() {
    _helixManager.getConfigAccessor().remove(_clusterScope, Broker.USE_APPROXIMATE_FUNCTION);
    _helixManager.getConfigAccessor().remove(_clusterScope, Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS);
    _helixManager.getConfigAccessor().remove(_clusterScope, Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS);
  }

  /// The broker picks the change up from a ZooKeeper watch, so the test waits for it instead of restarting anything.
  /// The response flag only reports a result that turned approximate, so the wait checks the rewrite itself, in both
  /// engines: the single-stage column name, and the multi-stage plan.
  private void waitForOverride(boolean expected) {
    boolean useMultiStageQueryEngine = useMultiStageQueryEngine();
    try {
      TestUtils.waitForCondition(aVoid -> {
        try {
          setUseMultiStageQueryEngine(false);
          boolean singleStage = columnName(postQuery(DISTINCT_COUNT_QUERY)).startsWith("distinctcountsmarthll");
          setUseMultiStageQueryEngine(true);
          boolean multiStage = postQuery("EXPLAIN PLAN FOR " + DISTINCT_COUNT_QUERY).toString()
              .toUpperCase(Locale.ROOT).contains("DISTINCTCOUNTSMARTHLL");
          return singleStage == expected && multiStage == expected;
        } catch (Exception e) {
          return false;
        }
      }, 100L, 60_000L, "Broker did not pick up the approximate function cluster config");
    } finally {
      setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    }
  }
}
