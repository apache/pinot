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

import java.util.List;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.MvAggregateMode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// The modes of the MV aggregate gate ([QueryEnvironment.Config#defaultMvAggregateMode()]) and the `useMvAggregate`
/// query option override. The base test environment uses the broker default, `auto`.
public class MvAggregateGateTest extends QueryEnvironmentTestBase {
  private static final String QUERY =
      "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM e GROUP BY ARRAY_TO_MV(mcol1)";
  private static final String FILTER_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), SUM(col3) FROM e "
      + "WHERE col2 = 'a' AND ARRAY_TO_MV(mcol1) = 'b' GROUP BY ARRAY_TO_MV(mcol1)";
  // With the planner default, the IN subquery becomes a dynamic-broadcast semi-join in the leaf stage.
  private static final String SEMI_JOIN_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM e "
      + "WHERE col1 IN (SELECT col1 FROM a WHERE col2 = 'x') GROUP BY ARRAY_TO_MV(mcol1)";
  private static final String JOIN_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(e.mcol1), COUNT(*) FROM e "
      + "JOIN a ON e.col1 = a.col1 GROUP BY ARRAY_TO_MV(e.mcol1)";
  private static final String LOOKUP_JOIN_QUERY = "EXPLAIN PLAN FOR SELECT /*+ joinOptions(join_strategy='lookup') */ "
      + "ARRAY_TO_MV(e.mcol1), COUNT(*) FROM e JOIN a ON e.col1 = a.col1 GROUP BY ARRAY_TO_MV(e.mcol1)";
  private static final String SORTED_SUBQUERY_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM "
      + "(SELECT mcol1 FROM e ORDER BY col1 LIMIT 10) GROUP BY ARRAY_TO_MV(mcol1)";
  private static final String UNNEST_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(e.mcol1), COUNT(*) FROM e "
      + "CROSS JOIN UNNEST(e.mcol2) AS u(elem) GROUP BY ARRAY_TO_MV(e.mcol1)";
  // The queries whose aggregate runs whole in the leaf stage. Without the MV aggregate they keep the plain plan.
  private static final List<String> LEAF_ONLY_QUERIES = List.of(QUERY, FILTER_QUERY, SEMI_JOIN_QUERY);
  // The queries whose aggregate runs in an intermediate stage. Without the MV aggregate they fail, as before.
  private static final List<String> INTERMEDIATE_QUERIES =
      List.of(JOIN_QUERY, LOOKUP_JOIN_QUERY, SORTED_SUBQUERY_QUERY, UNNEST_QUERY);
  private static final String MV_AGGREGATE = "PinotLogicalMvAggregate";
  private static final String SUPPORTED_USES = "ARRAY_TO_MV is supported only";

  private QueryEnvironment _gateOnEnvironment;
  private QueryEnvironment _gateOffEnvironment;

  @BeforeClass
  public void setUpGateEnvironments() {
    _gateOnEnvironment = getQueryEnvironment(3, 1, 2, TABLE_SCHEMAS, SERVER1_SEGMENTS, SERVER2_SEGMENTS,
        PARTITIONED_SEGMENTS_MAP, config -> config.defaultMvAggregateMode(MvAggregateMode.TRUE));
    _gateOffEnvironment = getQueryEnvironment(3, 1, 2, TABLE_SCHEMAS, SERVER1_SEGMENTS, SERVER2_SEGMENTS,
        PARTITIONED_SEGMENTS_MAP, config -> config.defaultMvAggregateMode(MvAggregateMode.FALSE));
  }

  @Test
  public void testBrokerDefaultIsAuto() {
    assertEquals(CommonConstants.Broker.DEFAULT_USE_MV_AGGREGATE, "auto");
    assertEquals(QueryEnvironment.configBuilder().requestId(-1L).database("default").tableCache(mock(TableCache.class))
        .workerManager(mock(WorkerManager.class)).build().defaultMvAggregateMode(), MvAggregateMode.AUTO);
  }

  @Test
  public void testAutoKeepsThePlanOfQueriesThatPlanWithoutMvAggregate() {
    for (String query : LEAF_ONLY_QUERIES) {
      String plan = explain(_queryEnvironment, query);
      assertEquals(plan, explain(_gateOffEnvironment, query));
      assertFalse(plan.contains(MV_AGGREGATE), plan);
    }
  }

  @Test
  public void testAutoFallsBackToMvAggregateWhenThePlanFailsWithoutIt() {
    for (String query : INTERMEDIATE_QUERIES) {
      String plan = explain(_queryEnvironment, query);
      assertTrue(plan.contains(MV_AGGREGATE), plan);
      assertEquals(plan, explain(_gateOnEnvironment, query));
    }
  }

  @Test
  public void testAutoFallsBackToMvAggregateWithoutImplementation() {
    String query = JOIN_QUERY.replace("EXPLAIN PLAN FOR", "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR");
    String plan = explain(_queryEnvironment, query);
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    String leafOnlyQuery = QUERY.replace("EXPLAIN PLAN FOR", "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR");
    plan = explain(_queryEnvironment, leafOnlyQuery);
    assertFalse(plan.contains(MV_AGGREGATE), plan);
  }

  @Test
  public void testAutoKeepsTheErrorOfUnsupportedUses() {
    String query = "EXPLAIN PLAN FOR SELECT SUM(ARRAY_TO_MV(e.mcol2)) FROM e JOIN a ON e.col1 = a.col1";
    Exception e = expectThrows(Exception.class, () -> explain(_queryEnvironment, query));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
  }

  @Test
  public void testAutoFallsBackOnlyForArrayToMvGroupKeys() {
    // A plain GROUP BY mvCol fails the validation, but the MV aggregate cannot help: no second plan.
    QueryException e = expectThrows(QueryException.class,
        () -> explain(_queryEnvironment, "EXPLAIN PLAN FOR SELECT mcol1, COUNT(*) FROM e GROUP BY mcol1"));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
    assertEquals(e.getSuppressed().length, 0);
    // Grouping sets keep the plain aggregate in the second plan, so it fails too. The error of the second plan is
    // thrown, with the error of the first plan as a suppressed exception.
    e = expectThrows(QueryException.class, () -> explain(_queryEnvironment, "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV("
        + "e.mcol1), COUNT(*) FROM e JOIN a ON e.col1 = a.col1 GROUP BY GROUPING SETS ((ARRAY_TO_MV(e.mcol1)), ())"));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
    assertEquals(e.getSuppressed().length, 1);
  }

  @Test
  public void testGateOnPlansMvAggregateEverywhere() {
    for (String query : List.of(QUERY, FILTER_QUERY, SEMI_JOIN_QUERY, JOIN_QUERY, LOOKUP_JOIN_QUERY,
        SORTED_SUBQUERY_QUERY, UNNEST_QUERY)) {
      String plan = explain(_gateOnEnvironment, query);
      assertTrue(plan.contains(MV_AGGREGATE), plan);
    }
  }

  @Test
  public void testGateOffKeepsThePlainLeafAggregate() {
    for (String query : LEAF_ONLY_QUERIES) {
      String plan = explain(_gateOffEnvironment, query);
      assertFalse(plan.contains(MV_AGGREGATE), plan);
      assertTrue(plan.contains("ARRAY_TO_MV"), plan);
      assertTrue(plan.contains("aggType=[LEAF]"), plan);
    }
  }

  @Test
  public void testGateOffPushesArrayToMvBelowTheSemiJoin() {
    // As before the MV aggregate, ARRAY_TO_MV is computed below the semi-join, in the leaf stage. In the plan text, a
    // node comes after its parent.
    for (QueryEnvironment queryEnvironment : List.of(_gateOffEnvironment, _queryEnvironment)) {
      String plan = explain(queryEnvironment, SEMI_JOIN_QUERY);
      assertTrue(plan.indexOf("ARRAY_TO_MV") > plan.indexOf("Join"), plan);
    }
    // With the MV aggregate, ARRAY_TO_MV stays above the semi-join.
    String plan = explain(_gateOnEnvironment, SEMI_JOIN_QUERY);
    assertTrue(plan.indexOf("ARRAY_TO_MV") < plan.indexOf("Join"), plan);
  }

  @Test
  public void testGateOffRejectsArrayToMvInIntermediateStages() {
    for (String query : INTERMEDIATE_QUERIES) {
      Exception e = expectThrows(Exception.class, () -> explain(_gateOffEnvironment, query));
      assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
    }
  }

  @Test
  public void testFilterOnMvKeyStaysAboveTheAggregate() {
    // Below the aggregate, the filter would become a multi-value predicate in the leaf stage, which keeps the other
    // values of a matching row. In the plan text, a node comes after its parent.
    String joinQuery = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(e.mcol1), COUNT(*) FROM e JOIN a ON e.col1 = a.col1 "
        + "GROUP BY ARRAY_TO_MV(e.mcol1) HAVING ARRAY_TO_MV(e.mcol1) = 'x'";
    String plan = explain(_queryEnvironment, joinQuery);
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    assertTrue(plan.indexOf("LogicalFilter") < plan.indexOf(MV_AGGREGATE), plan);
    String outerFilterQuery = "EXPLAIN PLAN FOR SELECT * FROM (SELECT ARRAY_TO_MV(mcol1) AS k, COUNT(*) AS cnt FROM e "
        + "GROUP BY ARRAY_TO_MV(mcol1)) WHERE k = 'x'";
    for (QueryEnvironment queryEnvironment : List.of(_queryEnvironment, _gateOnEnvironment, _gateOffEnvironment)) {
      plan = explain(queryEnvironment, outerFilterQuery);
      assertTrue(plan.indexOf("LogicalFilter") < plan.indexOf("Aggregate"), plan);
    }
  }

  @Test
  public void testFilterOnSvKeyGoesBelowTheAggregate() {
    // The condition on the MV key stays above the aggregate. The condition on the single-value key goes below it, into
    // the leaf stage. In the plan text, a node comes after its parent.
    String query = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), col2, COUNT(*) FROM e "
        + "GROUP BY ARRAY_TO_MV(mcol1), col2 HAVING ARRAY_TO_MV(mcol1) = 'x' AND col2 = 'y'";
    for (QueryEnvironment queryEnvironment : List.of(_queryEnvironment, _gateOnEnvironment, _gateOffEnvironment)) {
      String plan = explain(queryEnvironment, query);
      int mvCondition = plan.indexOf("'x'");
      // The planner then pulls col2 = 'y' up as a constant, so 'y' also shows above the aggregate.
      int svCondition = plan.lastIndexOf("'y'");
      int firstAggregate = plan.indexOf("Aggregate");
      assertTrue(mvCondition >= 0 && mvCondition < firstAggregate, plan);
      assertTrue(svCondition > plan.lastIndexOf("Aggregate"), plan);
    }
  }

  @Test
  public void testQueryOptionOverridesTheBrokerMode() {
    // true plans the MV aggregate also for a leaf-only aggregate.
    String plan = explain(_queryEnvironment, "SET useMvAggregate=true; " + QUERY);
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    // false disables the fallback of auto.
    Exception e =
        expectThrows(Exception.class, () -> explain(_queryEnvironment, "SET useMvAggregate=false; " + JOIN_QUERY));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
    plan = explain(_gateOnEnvironment, "SET useMvAggregate='FALSE'; " + QUERY);
    assertFalse(plan.contains(MV_AGGREGATE), plan);
    // auto enables the fallback when the broker mode is false.
    plan = explain(_gateOffEnvironment, "SET useMvAggregate='auto'; " + JOIN_QUERY);
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    plan = explain(_gateOffEnvironment, "SET useMvAggregate=true; " + QUERY);
    assertTrue(plan.contains(MV_AGGREGATE), plan);
  }

  @Test
  public void testQueryOptionInvalidValueFailsValidation() {
    for (String value : List.of("yes", "1")) {
      QueryException e = expectThrows(QueryException.class,
          () -> explain(_queryEnvironment, "SET useMvAggregate='" + value + "'; " + QUERY));
      assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
      assertTrue(e.getMessage().contains("useMvAggregate"), e.getMessage());
    }
  }

  private static String getFullMessage(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      sb.append(cause.getMessage()).append('\n');
    }
    return sb.toString();
  }

  private static String explain(QueryEnvironment queryEnvironment, String query) {
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(query)) {
      return compiledQuery.explain(RANDOM_REQUEST_ID_GEN.nextLong(), null).getExplainPlan();
    }
  }
}
