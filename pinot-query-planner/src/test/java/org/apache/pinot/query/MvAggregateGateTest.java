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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// The broker default of the MV aggregate gate ([QueryEnvironment.Config#defaultUseMvAggregate()]) and the
/// `useMvAggregate` query option override. The base test environment enables the MV aggregate. The gate-off
/// environment uses the broker default.
public class MvAggregateGateTest extends QueryEnvironmentTestBase {
  private static final String QUERY =
      "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM e GROUP BY ARRAY_TO_MV(mcol1)";
  private static final String FILTER_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), SUM(col3) FROM e "
      + "WHERE col2 = 'a' AND ARRAY_TO_MV(mcol1) = 'b' GROUP BY ARRAY_TO_MV(mcol1)";
  // With the planner default, the IN subquery becomes a dynamic-broadcast semi-join in the leaf stage.
  private static final String SEMI_JOIN_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM e "
      + "WHERE col1 IN (SELECT col1 FROM a) GROUP BY ARRAY_TO_MV(mcol1)";
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
      List.of(JOIN_QUERY, LOOKUP_JOIN_QUERY, SORTED_SUBQUERY_QUERY);
  private static final String MV_AGGREGATE = "PinotLogicalMvAggregate";
  private static final String SUPPORTED_USES = "ARRAY_TO_MV is supported only";

  private QueryEnvironment _gateOffEnvironment;

  @BeforeClass
  public void setUpGateOffEnvironment() {
    _gateOffEnvironment = getQueryEnvironment(3, 1, 2, TABLE_SCHEMAS, SERVER1_SEGMENTS, SERVER2_SEGMENTS,
        PARTITIONED_SEGMENTS_MAP,
        config -> config.defaultUseMvAggregate(CommonConstants.Broker.DEFAULT_USE_MV_AGGREGATE));
  }

  @Test
  public void testBrokerDefaultIsOff() {
    assertFalse(CommonConstants.Broker.DEFAULT_USE_MV_AGGREGATE);
    assertFalse(QueryEnvironment.configBuilder().requestId(-1L).database("default").tableCache(mock(TableCache.class))
        .workerManager(mock(WorkerManager.class)).build().defaultUseMvAggregate());
  }

  @Test
  public void testGateOnPlansMvAggregateEverywhere() {
    for (String query : List.of(QUERY, FILTER_QUERY, SEMI_JOIN_QUERY, JOIN_QUERY, LOOKUP_JOIN_QUERY,
        SORTED_SUBQUERY_QUERY, UNNEST_QUERY)) {
      String plan = explain(_queryEnvironment, query);
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
  public void testGateOffRejectsArrayToMvInIntermediateStages() {
    for (String query : INTERMEDIATE_QUERIES) {
      Exception e = expectThrows(Exception.class, () -> explain(_gateOffEnvironment, query));
      assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
    }
  }

  @Test
  public void testQueryOptionTrueOverridesGateOff() {
    for (String query : List.of(QUERY, JOIN_QUERY)) {
      String plan = explain(_gateOffEnvironment, "SET useMvAggregate=true; " + query);
      assertTrue(plan.contains(MV_AGGREGATE), plan);
    }
  }

  @Test
  public void testQueryOptionFalseOverridesGateOn() {
    String plan = explain(_queryEnvironment, "SET useMvAggregate='FALSE'; " + QUERY);
    assertFalse(plan.contains(MV_AGGREGATE), plan);
    Exception e =
        expectThrows(Exception.class, () -> explain(_queryEnvironment, "SET useMvAggregate=false; " + JOIN_QUERY));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
  }

  @Test
  public void testQueryOptionInvalidValueFailsValidation() {
    for (String value : List.of("yes", "auto")) {
      QueryException e = expectThrows(QueryException.class,
          () -> explain(_gateOffEnvironment, "SET useMvAggregate='" + value + "'; " + QUERY));
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
