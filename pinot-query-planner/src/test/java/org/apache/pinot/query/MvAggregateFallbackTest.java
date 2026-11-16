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
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// The MV aggregate fallback of `QueryEnvironment.CompiledQuery`: the planner plans a query without the MV aggregate
/// first, and plans it again with the MV aggregate only when that plan fails the `ARRAY_TO_MV` validation.
public class MvAggregateFallbackTest extends QueryEnvironmentTestBase {
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
  // In the first plan, the hint keeps the aggregate out of the leaf stage. The second plan ignores the hint, so its MV
  // aggregate reads directly from the table scan.
  private static final String SKIP_LEAF_QUERY = "EXPLAIN PLAN FOR SELECT /*+ aggOptions(is_skip_leaf_stage_group_by="
      + "'true') */ ARRAY_TO_MV(mcol1), COUNT(*) FROM e GROUP BY ARRAY_TO_MV(mcol1)";
  // The queries whose aggregate runs whole in the leaf stage. They keep the plain plan.
  private static final List<String> LEAF_ONLY_QUERIES = List.of(QUERY, FILTER_QUERY, SEMI_JOIN_QUERY);
  // The queries whose aggregate cannot run in the leaf stage. They fail without the MV aggregate.
  private static final List<String> FALLBACK_QUERIES =
      List.of(JOIN_QUERY, LOOKUP_JOIN_QUERY, SORTED_SUBQUERY_QUERY, UNNEST_QUERY, SKIP_LEAF_QUERY);
  private static final String MV_AGGREGATE = "PinotLogicalMvAggregate";
  private static final String SUPPORTED_USES = "ARRAY_TO_MV is supported only";

  @Test
  public void testKeepsThePlainPlanOfQueriesThatPlanWithoutMvAggregate() {
    for (String query : LEAF_ONLY_QUERIES) {
      String plan = explain(query);
      assertFalse(plan.contains(MV_AGGREGATE), plan);
      assertTrue(plan.contains("ARRAY_TO_MV"), plan);
      assertTrue(plan.contains("aggType=[LEAF]"), plan);
    }
  }

  @Test
  public void testFallsBackToMvAggregateWhenThePlanFailsWithoutIt() {
    for (String query : FALLBACK_QUERIES) {
      String plan = explain(query);
      assertTrue(plan.contains(MV_AGGREGATE), plan);
    }
  }

  @Test
  public void testFallsBackToMvAggregateWithoutImplementation() {
    String query = JOIN_QUERY.replace("EXPLAIN PLAN FOR", "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR");
    String plan = explain(query);
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    String leafOnlyQuery = QUERY.replace("EXPLAIN PLAN FOR", "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR");
    plan = explain(leafOnlyQuery);
    assertFalse(plan.contains(MV_AGGREGATE), plan);
  }

  @Test
  public void testKeepsTheErrorOfUnsupportedUses() {
    String query = "EXPLAIN PLAN FOR SELECT SUM(ARRAY_TO_MV(e.mcol2)) FROM e JOIN a ON e.col1 = a.col1";
    Exception e = expectThrows(Exception.class, () -> explain(query));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
  }

  @Test
  public void testFallsBackOnlyForArrayToMvGroupKeys() {
    // A plain GROUP BY mvCol fails the validation, but the MV aggregate cannot help: no second plan.
    QueryException e = expectThrows(QueryException.class,
        () -> explain("EXPLAIN PLAN FOR SELECT mcol1, COUNT(*) FROM e GROUP BY mcol1"));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
    assertEquals(e.getSuppressed().length, 0);
    // Grouping sets keep the plain aggregate in the second plan, so it fails too. The error of the second plan is
    // thrown, with the error of the first plan as a suppressed exception.
    e = expectThrows(QueryException.class, () -> explain("EXPLAIN PLAN FOR SELECT ARRAY_TO_MV("
        + "e.mcol1), COUNT(*) FROM e JOIN a ON e.col1 = a.col1 GROUP BY GROUPING SETS ((ARRAY_TO_MV(e.mcol1)), ())"));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
    assertEquals(e.getSuppressed().length, 1);
  }

  @Test
  public void testPushesArrayToMvBelowTheSemiJoinOfALeafOnlyQuery() {
    // As before the MV aggregate, ARRAY_TO_MV is computed below the semi-join, in the leaf stage. In the plan text, a
    // node comes after its parent.
    String plan = explain(SEMI_JOIN_QUERY);
    assertTrue(plan.indexOf("ARRAY_TO_MV") > plan.indexOf("Join"), plan);
  }

  @Test
  public void testArrayToMvIsNotNullInThePlanWithoutMvAggregate() {
    // f has column-based null handling, so its MV column is nullable. ARRAY_TO_MV keeps the NOT NULL type that it had
    // before the MV aggregate, so Calcite removes COALESCE and folds IS NOT NULL as before. The leaf stage runs the
    // remaining multi-value predicates.
    String plan = explain("EXPLAIN PLAN FOR SELECT COUNT(*) FROM f WHERE COALESCE(ARRAY_TO_MV(mcol1), 'x') = 'a'");
    assertFalse(plan.contains("CASE") || plan.contains("COALESCE") || plan.contains("IS NOT NULL"), plan);
    plan = explain("EXPLAIN PLAN FOR SELECT COUNT(*) FROM f WHERE CAST(COALESCE(ARRAY_TO_MV(mcol1), '0') AS INT) = 1");
    assertFalse(plan.contains("CASE") || plan.contains("COALESCE") || plan.contains("IS NOT NULL"), plan);
  }

  @Test
  public void testArrayToMvIsNullableInThePlanWithMvAggregate() {
    // Above a LEFT JOIN, the array of e is nullable. In the plan with the MV aggregate, ARRAY_TO_MV is nullable too,
    // so the null check on the key stays: a null array forms the null group.
    String plan = explain("EXPLAIN PLAN FOR SELECT CASE WHEN ARRAY_TO_MV(e.mcol1) IS NULL THEN 'none' ELSE "
        + "ARRAY_TO_MV(e.mcol1) END, COUNT(*) FROM a LEFT JOIN e ON a.col1 = e.col1 GROUP BY ARRAY_TO_MV(e.mcol1)");
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    assertTrue(plan.contains("IS NULL"), plan);
  }

  @Test
  public void testFilterOnMvKeyStaysAboveTheAggregate() {
    // Below the aggregate, the filter would become a multi-value predicate in the leaf stage, which keeps the other
    // values of a matching row. In the plan text, a node comes after its parent.
    String joinQuery = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(e.mcol1), COUNT(*) FROM e JOIN a ON e.col1 = a.col1 "
        + "GROUP BY ARRAY_TO_MV(e.mcol1) HAVING ARRAY_TO_MV(e.mcol1) = 'x'";
    String plan = explain(joinQuery);
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    assertTrue(plan.indexOf("LogicalFilter") < plan.indexOf(MV_AGGREGATE), plan);
    String outerFilterQuery = "EXPLAIN PLAN FOR SELECT * FROM (SELECT ARRAY_TO_MV(mcol1) AS k, COUNT(*) AS cnt FROM e "
        + "GROUP BY ARRAY_TO_MV(mcol1)) WHERE k = 'x'";
    plan = explain(outerFilterQuery);
    assertTrue(plan.indexOf("LogicalFilter") < plan.indexOf("Aggregate"), plan);
  }

  @Test
  public void testFilterOnSvKeyGoesBelowTheAggregate() {
    // The condition on the MV key stays above the aggregate. The condition on the single-value key goes below it, into
    // the leaf stage. In the plan text, a node comes after its parent.
    String plan = explain("EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), col2, COUNT(*) FROM e "
        + "GROUP BY ARRAY_TO_MV(mcol1), col2 HAVING ARRAY_TO_MV(mcol1) = 'x' AND col2 = 'y'");
    int mvCondition = plan.indexOf("'x'");
    // The planner then pulls col2 = 'y' up as a constant, so 'y' also shows above the aggregate.
    int svCondition = plan.lastIndexOf("'y'");
    int firstAggregate = plan.indexOf("Aggregate");
    assertTrue(mvCondition >= 0 && mvCondition < firstAggregate, plan);
    assertTrue(svCondition > plan.lastIndexOf("Aggregate"), plan);
  }

  @Test
  public void testPositiveFilterOnMvKeyIsCopiedBelowTheAggregate() {
    // The exact condition stays above the aggregate, and a copy filters the rows in the leaf stage
    for (String condition : List.of("= 'x'", "IN ('x', 'z')", "> 'x'", "BETWEEN 'x' AND 'z'")) {
      String plan = explain("EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM e GROUP BY ARRAY_TO_MV(mcol1) "
          + "HAVING ARRAY_TO_MV(mcol1) " + condition);
      assertTrue(plan.indexOf("'x'") < plan.indexOf("Aggregate"), plan);
      assertTrue(plan.lastIndexOf("'x'") > plan.lastIndexOf("Aggregate"), plan);
    }
  }

  @Test
  public void testNegatedFilterOnMvKeyIsNotCopiedBelowTheAggregate() {
    // As a multi-value predicate, a negated condition matches a row only when no element matches
    for (String condition : List.of("<> 'x'", "NOT IN ('x', 'z')")) {
      String plan = explain("EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM e GROUP BY ARRAY_TO_MV(mcol1) "
          + "HAVING ARRAY_TO_MV(mcol1) " + condition);
      assertTrue(plan.lastIndexOf("'x'") < plan.indexOf("Aggregate"), plan);
    }
  }

  @Test
  public void testFilterOnMvKeyAboveAJoinIsNotCopied() {
    // Without a table scan under the aggregate, the copy can end up in an intermediate stage
    String plan = explain("EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(e.mcol1), COUNT(*) FROM e JOIN a ON e.col1 = a.col1 "
        + "GROUP BY ARRAY_TO_MV(e.mcol1) HAVING ARRAY_TO_MV(e.mcol1) = 'x'");
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    assertTrue(plan.lastIndexOf("'x'") < plan.indexOf("Aggregate"), plan);
  }

  private static String getFullMessage(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      sb.append(cause.getMessage()).append('\n');
    }
    return sb.toString();
  }

  private String explain(String query) {
    try (QueryEnvironment.CompiledQuery compiledQuery = _queryEnvironment.compile(query)) {
      return compiledQuery.explain(RANDOM_REQUEST_ID_GEN.nextLong(), null).getExplainPlan();
    }
  }
}
