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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// When the planner plans `GROUP BY ARRAY_TO_MV(expr)` as an MV aggregate, and what stays above and below it.
public class MvAggregatePlanTest extends QueryEnvironmentTestBase {
  private static final String QUERY =
      "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM e GROUP BY ARRAY_TO_MV(mcol1)";
  private static final String FILTER_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), SUM(col3) FROM e "
      + "WHERE col2 = 'a' AND ARRAY_TO_MV(mcol1) = 'b' GROUP BY ARRAY_TO_MV(mcol1)";
  // With the planner default, the IN subquery becomes a dynamic-broadcast semi-join in the leaf stage.
  private static final String SEMI_JOIN_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM e "
      + "WHERE col1 IN (SELECT col1 FROM a WHERE col2 = 'x') GROUP BY ARRAY_TO_MV(mcol1)";
  // Two IN subqueries become two semi-joins in the leaf stage, one above the other.
  private static final String TWO_SEMI_JOINS_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM e "
      + "WHERE col1 IN (SELECT col1 FROM a) AND col2 IN (SELECT col2 FROM b) GROUP BY ARRAY_TO_MV(mcol1)";
  private static final String JOIN_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(e.mcol1), COUNT(*) FROM e "
      + "JOIN a ON e.col1 = a.col1 GROUP BY ARRAY_TO_MV(e.mcol1)";
  private static final String LOOKUP_JOIN_QUERY = "EXPLAIN PLAN FOR SELECT /*+ joinOptions(join_strategy='lookup') */ "
      + "ARRAY_TO_MV(e.mcol1), COUNT(*) FROM e JOIN a ON e.col1 = a.col1 GROUP BY ARRAY_TO_MV(e.mcol1)";
  private static final String SORTED_SUBQUERY_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM "
      + "(SELECT mcol1 FROM e ORDER BY col1 LIMIT 10) GROUP BY ARRAY_TO_MV(mcol1)";
  private static final String UNNEST_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(e.mcol1), COUNT(*) FROM e "
      + "CROSS JOIN UNNEST(e.mcol2) AS u(elem) GROUP BY ARRAY_TO_MV(e.mcol1)";
  // The plain plan with this hint runs the aggregate above the leaf stage, so the planner uses the MV aggregate and
  // ignores the hint.
  private static final String SKIP_LEAF_QUERY = "EXPLAIN PLAN FOR SELECT /*+ aggOptions(is_skip_leaf_stage_group_by="
      + "'true') */ ARRAY_TO_MV(mcol1), COUNT(*) FROM e GROUP BY ARRAY_TO_MV(mcol1)";
  // A semi-join with this hint does not run in the leaf stage.
  private static final String HASH_SEMI_JOIN_QUERY =
      SEMI_JOIN_QUERY.replace("SELECT ARRAY_TO_MV", "SELECT /*+ joinOptions(join_strategy='hash') */ ARRAY_TO_MV");
  // The left input of these semi-joins is not a table scan, so the semi-joins do not run in the leaf stage.
  private static final String SEMI_JOIN_ABOVE_JOIN_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(e.mcol1), COUNT(*) "
      + "FROM e JOIN b ON e.col1 = b.col1 WHERE e.col2 IN (SELECT col2 FROM a) GROUP BY ARRAY_TO_MV(e.mcol1)";
  private static final String SEMI_JOIN_ABOVE_SORT_QUERY = "EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), COUNT(*) FROM "
      + "(SELECT mcol1, col1 FROM e ORDER BY col2 LIMIT 10) WHERE col1 IN (SELECT col1 FROM a) "
      + "GROUP BY ARRAY_TO_MV(mcol1)";
  // The queries whose aggregate reads a table scan, also through a semi-join in the leaf stage. They keep the plain
  // plan, which a server that predates the MV aggregate can run.
  private static final List<String> TABLE_SCAN_QUERIES =
      List.of(QUERY, FILTER_QUERY, SEMI_JOIN_QUERY, TWO_SEMI_JOINS_QUERY);
  private static final List<String> MV_AGGREGATE_QUERIES = List.of(JOIN_QUERY, LOOKUP_JOIN_QUERY, HASH_SEMI_JOIN_QUERY,
      SEMI_JOIN_ABOVE_JOIN_QUERY, SEMI_JOIN_ABOVE_SORT_QUERY, SORTED_SUBQUERY_QUERY, UNNEST_QUERY, SKIP_LEAF_QUERY);
  private static final String MV_AGGREGATE = "PinotLogicalMvAggregate";
  private static final String SUPPORTED_USES = "ARRAY_TO_MV is supported only";

  @Test
  public void testKeepsThePlainPlanWhenTheAggregateReadsATableScan() {
    for (String query : TABLE_SCAN_QUERIES) {
      String plan = explain(query);
      assertFalse(plan.contains(MV_AGGREGATE), plan);
      assertTrue(plan.contains("ARRAY_TO_MV"), plan);
      assertTrue(plan.contains("aggType=[LEAF]"), plan);
    }
  }

  @Test
  public void testPlansMvAggregateWhenTheAggregateDoesNotReadATableScan() {
    for (String query : MV_AGGREGATE_QUERIES) {
      String plan = explain(query);
      assertTrue(plan.contains(MV_AGGREGATE), plan);
    }
  }

  @Test
  public void testPlansMvAggregateWithoutImplementation() {
    String query = JOIN_QUERY.replace("EXPLAIN PLAN FOR", "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR");
    String plan = explain(query);
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    String tableScanQuery = QUERY.replace("EXPLAIN PLAN FOR", "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR");
    plan = explain(tableScanQuery);
    assertFalse(plan.contains(MV_AGGREGATE), plan);
  }

  @Test
  public void testKeepsTheErrorOfUnsupportedUses() {
    String query = "EXPLAIN PLAN FOR SELECT SUM(ARRAY_TO_MV(e.mcol2)) FROM e JOIN a ON e.col1 = a.col1";
    Exception e = expectThrows(Exception.class, () -> explain(query));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
  }

  @Test
  public void testPlansMvAggregateOnlyForArrayToMvGroupKeys() {
    // A plain GROUP BY mvCol fails the validation: the MV aggregate cannot help
    QueryException e = expectThrows(QueryException.class,
        () -> explain("EXPLAIN PLAN FOR SELECT mcol1, COUNT(*) FROM e GROUP BY mcol1"));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
    // Grouping sets keep the plain aggregate, which fails the validation above a join
    e = expectThrows(QueryException.class, () -> explain("EXPLAIN PLAN FOR SELECT ARRAY_TO_MV("
        + "e.mcol1), COUNT(*) FROM e JOIN a ON e.col1 = a.col1 GROUP BY GROUPING SETS ((ARRAY_TO_MV(e.mcol1)), ())"));
    assertTrue(getFullMessage(e).contains(SUPPORTED_USES), getFullMessage(e));
  }

  @Test
  public void testMvKeyThroughASelectAlias() {
    // With the option, GROUP BY col2 is the alias of the MV key, not the column col2
    String plan = explain("SET preferSelectAliasInGroupBy=true; EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(e.mcol1) AS col2, "
        + "COUNT(*) FROM e JOIN a ON e.col1 = a.col1 GROUP BY col2");
    assertTrue(plan.contains(MV_AGGREGATE + "(group=[{0}], agg#0=[COUNT()], mvKeys=[[0]]"), plan);
  }

  @Test
  public void testWithinGroupIsNotSupportedWithAnMvKey() {
    Exception e = expectThrows(Exception.class, () -> explain("EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(mcol1), "
        + "LISTAGG(col2, ',') WITHIN GROUP (ORDER BY col2) FROM e GROUP BY ARRAY_TO_MV(mcol1)"));
    assertTrue(getFullMessage(e).contains("is not supported with WITHIN GROUP"), getFullMessage(e));
  }

  @Test
  public void testArrayToMvIsNullableOnANullableColumn() {
    // f has column-based null handling, so its MV column is nullable, and ARRAY_TO_MV of it is nullable too: a null
    // array forms the null group. So Calcite does not remove COALESCE. The leaf stage cannot run COALESCE on a
    // multi-value column, see MvAggregate.json.
    String plan = explain("EXPLAIN PLAN FOR SELECT COUNT(*) FROM f WHERE COALESCE(ARRAY_TO_MV(mcol1), 'x') = 'a'");
    assertTrue(plan.contains("COALESCE"), plan);
    plan = explain("EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(f.mcol1), COUNT(*) FROM f JOIN a ON f.col1 = a.col1 "
        + "WHERE COALESCE(ARRAY_TO_MV(f.mcol1), 'x') = 'a' GROUP BY ARRAY_TO_MV(f.mcol1)");
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    assertTrue(plan.contains("COALESCE"), plan);
    // The MV column of e is not nullable, so COALESCE goes away as before
    plan = explain("EXPLAIN PLAN FOR SELECT COUNT(*) FROM e WHERE COALESCE(ARRAY_TO_MV(mcol1), 'x') = 'a'");
    assertFalse(plan.contains("COALESCE"), plan);
  }

  @Test
  public void testArrayToMvIsNullableAboveAnOuterJoin() {
    // Above a LEFT JOIN, the array of e is nullable, so ARRAY_TO_MV is nullable too and the null check on the key
    // stays: a null array forms the null group.
    String plan = explain("EXPLAIN PLAN FOR SELECT CASE WHEN ARRAY_TO_MV(e.mcol1) IS NULL THEN 'none' ELSE "
        + "ARRAY_TO_MV(e.mcol1) END, COUNT(*) FROM a LEFT JOIN e ON a.col1 = e.col1 GROUP BY ARRAY_TO_MV(e.mcol1)");
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    assertTrue(plan.contains("IS NULL"), plan);
  }

  @Test
  public void testTableNamesKeepTheTableOfTheNullGroup() {
    // The broker reads the table names, for example to apply row-level security. With a NOT NULL type of ARRAY_TO_MV,
    // the HAVING condition folds to false and removes table e from the plan. The null group needs e.
    String query = "SELECT ARRAY_TO_MV(e.mcol1), COUNT(*) FROM a LEFT JOIN e ON a.col1 = e.col1 "
        + "GROUP BY ARRAY_TO_MV(e.mcol1) HAVING ARRAY_TO_MV(e.mcol1) IS NULL";
    try (QueryEnvironment.CompiledQuery compiledQuery = _queryEnvironment.compile(query)) {
      assertTrue(compiledQuery.getTableNames().contains("e"), compiledQuery.getTableNames().toString());
    }
  }

  @Test
  public void testArrayToMvStaysAboveAJoin() {
    // The MV key must be in the Project under the aggregate. In the plan text, a node comes after its parent.
    for (String query : List.of(JOIN_QUERY, HASH_SEMI_JOIN_QUERY)) {
      String plan = explain(query);
      assertTrue(plan.indexOf(MV_AGGREGATE) < plan.indexOf("Join"), plan);
      assertFalse(plan.contains("ARRAY_TO_MV"), plan);
    }
    // Above a semi-join in the leaf stage, the call stays above the join in the Project of the plain aggregate
    String plan = explain(SEMI_JOIN_QUERY);
    assertTrue(plan.indexOf("ARRAY_TO_MV") < plan.indexOf("Join"), plan);
  }

  @Test
  public void testFilterOnMvKeyStaysAboveTheMvAggregate() {
    // Below the aggregate, the filter would become a multi-value predicate in the leaf stage, which keeps the other
    // values of a matching row. In the plan text, a node comes after its parent.
    for (String condition : List.of("= 'x'", "<> 'x'", "IN ('x', 'z')", "= 'x' AND a.col2 = 'y'")) {
      String plan = explain("EXPLAIN PLAN FOR SELECT ARRAY_TO_MV(e.mcol1), a.col2, COUNT(*) FROM e JOIN a "
          + "ON e.col1 = a.col1 GROUP BY ARRAY_TO_MV(e.mcol1), a.col2 HAVING ARRAY_TO_MV(e.mcol1) " + condition);
      assertTrue(plan.contains(MV_AGGREGATE), plan);
      assertTrue(plan.lastIndexOf("'x'") < plan.indexOf(MV_AGGREGATE), plan);
    }
    String plan = explain(HASH_SEMI_JOIN_QUERY + " HAVING ARRAY_TO_MV(mcol1) = 'b'");
    assertTrue(plan.contains(MV_AGGREGATE), plan);
    assertTrue(plan.indexOf("'b'") < plan.indexOf(MV_AGGREGATE), plan);
  }

  @Test
  public void testFilterOnMvKeyStaysAboveTheAggregateWhenTheKeyComesFromASubquery() {
    // The filter rule can run while the WHERE of the outer query is still between the aggregate and the Project
    // with ARRAY_TO_MV. In the plan text, a node comes after its parent.
    for (String from : List.of("e", "e JOIN a ON e.col1 = a.col1")) {
      String plan = explain("EXPLAIN PLAN FOR WITH t AS (SELECT ARRAY_TO_MV(e.mcol1) AS x, e.col2 AS c FROM " + from
          + ") SELECT x, COUNT(*) FROM t WHERE c = 'z' GROUP BY x HAVING x = 'b'");
      assertTrue(plan.indexOf("'b'") < plan.indexOf("Aggregate"), plan);
      assertTrue(plan.indexOf("'z'") > plan.lastIndexOf("Aggregate"), plan);
    }
  }

  @Test
  public void testFilterOnMvKeyStaysAboveThePlainAggregate() {
    // The same holds when the aggregate reads a table scan and keeps the plain plan
    for (String query : List.of(QUERY, SEMI_JOIN_QUERY)) {
      String plan = explain(query + " HAVING ARRAY_TO_MV(mcol1) = 'b'");
      assertFalse(plan.contains(MV_AGGREGATE), plan);
      assertTrue(plan.indexOf("'b'") < plan.indexOf("Aggregate"), plan);
    }
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
