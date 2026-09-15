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
package org.apache.pinot.query;

import java.util.Map;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests the multi-stage counterpart of the single-stage approximate function override.
public class ApproximateAggregateRewriteTest extends QueryEnvironmentTestBase {

  @Test
  public void testDisabledByDefault() {
    String plan = explain(newQueryEnvironment(false, "", ""), "SELECT DISTINCTCOUNT(col1) FROM a");
    assertTrue(plan.toLowerCase().contains("distinctcount"), plan);
    assertFalse(plan.toLowerCase().contains("distinctcountsmarthll"), plan);
  }

  @Test
  public void testRewritesDistinctCountAndPercentile() {
    QueryEnvironment env = newQueryEnvironment(true, "", "");

    assertRewritten(explain(env, "SELECT DISTINCTCOUNT(col1) FROM a"), "distinctcountsmarthll");
    assertRewritten(explain(env, "SELECT col2, DISTINCTCOUNT(col1) FROM a GROUP BY col2"), "distinctcountsmarthll");
    assertRewritten(explain(env, "SELECT PERCENTILE(col3, 90) FROM a"), "percentilesmarttdigest");
    assertRewritten(explain(env, "SELECT col2, PERCENTILE(col3, 90) FROM a GROUP BY col2"), "percentilesmarttdigest");

    // COUNT(DISTINCT x) is the standard SQL spelling, and the plan only renames it to DISTINCTCOUNT after this rule
    // has run, so the rule has to recognise it by kind.
    assertRewritten(explain(env, "SELECT COUNT(DISTINCT col1) FROM a"), "distinctcountsmarthll");
    assertRewritten(explain(env, "SELECT col2, COUNT(DISTINCT col1) FROM a GROUP BY col2"), "distinctcountsmarthll");

    // The multi-valued spellings too, so that the two engines agree.
    assertRewritten(explain(env, "SELECT DISTINCTCOUNTMV(mcol1) FROM e"), "distinctcountsmarthll");
    assertRewritten(explain(env, "SELECT PERCENTILEMV(mcol2, 90) FROM e"), "percentilesmarttdigest");
  }

  @Test
  public void testLeavesOtherAggregationsAlone() {
    QueryEnvironment env = newQueryEnvironment(true, "", "");
    assertNotRewritten(explain(env, "SELECT DISTINCTCOUNTHLL(col1) FROM a"));
    assertNotRewritten(explain(env, "SELECT PERCENTILETDIGEST(col3, 90) FROM a"));
    assertNotRewritten(explain(env, "SELECT SUM(col3), COUNT(*) FROM a"));
    // COUNT(DISTINCT a, b) means something different from DISTINCT_COUNT.
    assertNotRewritten(explain(env, "SELECT COUNT(DISTINCT col1, col2) FROM a"));
  }

  @Test
  public void testAppendsConfiguredParams() {
    QueryEnvironment env = newQueryEnvironment(true, "threshold=17", "threshold=23;compression=50");

    // Separate queries, so that each parameter string is seen to reach only its own function.
    String distinctCount = explain(env, "SELECT col2, DISTINCTCOUNT(col1) FROM a GROUP BY col2");
    assertRewritten(distinctCount, "distinctcountsmarthll");
    assertTrue(distinctCount.contains("threshold=17"), distinctCount);
    assertFalse(distinctCount.contains("threshold=23"), distinctCount);

    String percentile = explain(env, "SELECT col2, PERCENTILE(col3, 90) FROM a GROUP BY col2");
    assertRewritten(percentile, "percentilesmarttdigest");
    assertTrue(percentile.contains("threshold=23;compression=50"), percentile);

    // Both in one query needs two literals in the same project.
    String both = explain(env, "SELECT col2, DISTINCTCOUNT(col1), PERCENTILE(col3, 90) FROM a GROUP BY col2");
    assertRewritten(both, "distinctcountsmarthll");
    assertRewritten(both, "percentilesmarttdigest");
    assertTrue(both.contains("threshold=17"), both);
    assertTrue(both.contains("threshold=23;compression=50"), both);
  }

  /// The physical optimizer splits aggregates in its own rule, which also keys the leaf-to-final intermediate format
  /// off the function name, so the rewrite has to hold on that path too.
  @Test
  public void testRewritesUnderThePhysicalOptimizer() {
    QueryEnvironment env = newQueryEnvironment(true, "threshold=17", "threshold=23", true);
    String distinctCount = explain(env, "SELECT col2, DISTINCTCOUNT(col1) FROM a GROUP BY col2");
    assertRewritten(distinctCount, "distinctcountsmarthll");
    assertTrue(distinctCount.contains("threshold=17"), distinctCount);

    String percentile = explain(env, "SELECT col2, PERCENTILE(col3, 90) FROM a GROUP BY col2");
    assertRewritten(percentile, "percentilesmarttdigest");
    assertTrue(percentile.contains("threshold=23"), percentile);
  }

  private static void assertRewritten(String plan, String expectedFunction) {
    assertTrue(plan.toLowerCase().contains(expectedFunction), "Expected " + expectedFunction + " in:\n" + plan);
  }

  private static void assertNotRewritten(String plan) {
    assertFalse(plan.toLowerCase().contains("smart"), "Unexpected rewrite in:\n" + plan);
  }

  private static String explain(QueryEnvironment env, String query) {
    return env.explainQuery("EXPLAIN PLAN FOR " + query, RANDOM_REQUEST_ID_GEN.nextLong());
  }

  private static QueryEnvironment newQueryEnvironment(boolean useApproximateFunction, String distinctCountParams,
      String percentileParams) {
    return newQueryEnvironment(useApproximateFunction, distinctCountParams, percentileParams, false);
  }

  private static QueryEnvironment newQueryEnvironment(boolean useApproximateFunction, String distinctCountParams,
      String percentileParams, boolean usePhysicalOptimizer) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    for (Map.Entry<String, Schema> entry : TABLE_SCHEMAS.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey());
    }
    SERVER1_SEGMENTS.forEach((table, segments) -> segments.forEach(s -> factory.registerSegment(1, table, s)));
    SERVER2_SEGMENTS.forEach((table, segments) -> segments.forEach(s -> factory.registerSegment(2, table, s)));
    RoutingManager routingManager = factory.buildRoutingManager(null);
    return new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(-1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(factory.buildTableCache())
        .workerManager(new WorkerManager("Broker_localhost", "localhost", 3, routingManager))
        .useApproximateFunction(useApproximateFunction)
        .approximateFunctionDistinctCountParams(distinctCountParams)
        .approximateFunctionPercentileParams(percentileParams)
        .defaultUsePhysicalOptimizer(usePhysicalOptimizer)
        .build());
  }
}
