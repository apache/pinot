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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.rules.DefaultRuleSetCustomizer;
import org.apache.pinot.query.planner.rules.PinotRuleSet;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.apache.pinot.query.planner.spi.Phase;
import org.apache.pinot.query.planner.spi.RuleSetCustomizer;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Planning with sealed IN lists (see `SearchSealer`) must give the same plan as planning without sealing, must not
/// let any planner rule see a large Sarg, and must be fast in the positions where Calcite is super-linear.
public class SealedInListPlanningTest extends QueryEnvironmentTestBase {
  private static final int NUM_VALUES = 50;
  /// Threshold of the guarded planner, and the smallest Sarg or comparison list that the guard rejects.
  private static final int GUARD_THRESHOLD = 20;
  private static final String INTS = IntStream.range(0, NUM_VALUES).mapToObj(i -> Integer.toString(i * 7 + 3))
      .collect(Collectors.joining(", "));
  private static final String STRINGS = IntStream.range(0, NUM_VALUES).mapToObj(i -> "'v" + i * 7 + "'")
      .collect(Collectors.joining(", "));
  private static final String OTHER_INTS = IntStream.range(0, NUM_VALUES).mapToObj(i -> Integer.toString(i * 5 + 3))
      .collect(Collectors.joining(", "));
  private static final String TIMESTAMPS = IntStream.range(0, 25)
      .mapToObj(i -> String.format("TIMESTAMP '2024-01-01 00:00:%02d.%03d'", i, i)).collect(Collectors.joining(", "));
  private static final String BOOLEANS = IntStream.range(0, 24).mapToObj(i -> i % 2 == 0 ? "TRUE" : "FALSE")
      .collect(Collectors.joining(", "));
  private static final String OR_CHAIN = IntStream.range(0, NUM_VALUES).mapToObj(i -> "col3 = " + (i * 7 + 3))
      .collect(Collectors.joining(" OR "));
  /// Comparisons of many different operands, which cannot fold into one Sarg.
  private static final String WIDE_AND = IntStream.range(1, 25).mapToObj(i -> "col3 + " + i + " = " + i * 7)
      .collect(Collectors.joining(" AND "));
  private static final String WIDE_OR = IntStream.range(1, 25).mapToObj(i -> "col6 + " + i + " = " + i * 7)
      .collect(Collectors.joining(" OR "));

  /// The same tables with column-based null handling, so that their columns are nullable.
  private QueryEnvironment _nullableQueryEnvironment;
  /// The nullable tables, planned as the broker plans queries without null handling.
  private QueryEnvironment _withoutNullHandling;
  private QueryEnvironment _guardedWithoutNullHandling;

  @BeforeClass
  public void setUpNullableTables() {
    Map<String, Schema> schemas = new HashMap<>();
    for (String table : List.of("a", "b", "c")) {
      String tableName = TABLE_SCHEMAS.containsKey(table + "_REALTIME") ? table + "_REALTIME" : table + "_OFFLINE";
      schemas.put(tableName, getSchemaBuilder(table).setEnableColumnBasedNullHandling(true).build());
    }
    _nullableQueryEnvironment =
        getQueryEnvironment(3, 1, 2, schemas, SERVER1_SEGMENTS, SERVER2_SEGMENTS, PARTITIONED_SEGMENTS_MAP);
    Map<String, Schema> allSchemas = new HashMap<>(TABLE_SCHEMAS);
    allSchemas.putAll(schemas);
    _withoutNullHandling = newQueryEnvironment(allSchemas, false);
    _guardedWithoutNullHandling = newQueryEnvironment(allSchemas, true);
  }

  @DataProvider(name = "samePlanQueries")
  public Object[][] samePlanQueries() {
    List<String> queries = List.of(
        "SELECT col1, col3 FROM a WHERE col3 IN (" + INTS + ")",
        "SELECT * FROM a WHERE col3 IN (" + INTS + ")",
        "SELECT col1 FROM a WHERE col3 NOT IN (" + INTS + ")",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ", NULL)",
        "SELECT col1 FROM a WHERE col3 NOT IN (" + INTS + ", NULL)",
        "SELECT col1 FROM a WHERE col3 IN (col6, " + INTS + ")",
        "SELECT col1 FROM a WHERE col3 NOT IN (col6, " + INTS + ")",
        "SELECT col1 FROM a WHERE col1 IN (" + STRINGS + ")",
        "SELECT col1 FROM a WHERE UPPER(col1) IN (" + STRINGS + ")",
        "SELECT col1 FROM a WHERE col7 IN (" + INTS + ")",
        "SELECT col1 FROM a WHERE col4 IN (" + INTS + ")",
        "SELECT col1 FROM a WHERE col3 IN (" + STRINGS.replace("'v", "'") + ")",
        "SELECT col1 FROM a WHERE NOT (col3 IN (" + INTS + ") AND col1 = 'x')",
        "SELECT col1 FROM a WHERE " + OR_CHAIN,
        // All fields stay, so field trimming does not fold the chain before sealing.
        "SELECT * FROM a WHERE " + OR_CHAIN,
        "SELECT * FROM a WHERE " + OR_CHAIN.replaceAll("col3 = (\\d+)", "$1 = col3"),
        "SELECT SUM(CASE WHEN col3 IN (" + INTS + ") THEN 1 ELSE 0 END) FROM a",
        "SELECT SUM(CASE WHEN col3 NOT IN (" + INTS + ") THEN 1 ELSE 0 END) FROM a",
        "SELECT COUNT(*) FILTER (WHERE col3 IN (" + INTS + ")), COUNT(*) FROM a",
        "SELECT col3 IN (" + INTS + ") FROM a",
        "SELECT CASE WHEN col3 IN (" + INTS + ") THEN 'x' ELSE 'y' END, COUNT(*) FROM a "
            + "GROUP BY CASE WHEN col3 IN (" + INTS + ") THEN 'x' ELSE 'y' END",
        "SELECT col1, COUNT(*) FROM a GROUP BY col1 HAVING COUNT(*) IN (" + INTS + ")",
        "SELECT col1, ROW_NUMBER() OVER (PARTITION BY CASE WHEN col3 IN (" + INTS + ") THEN 1 ELSE 0 END "
            + "ORDER BY col6) FROM a",
        "SELECT a.col1 FROM a JOIN b ON a.col1 = b.col1 AND b.col3 IN (" + INTS + ")",
        "SELECT a.col1 FROM a LEFT JOIN b ON a.col1 = b.col1 AND a.col3 IN (" + INTS + ")",
        // The IN list rejects nulls, so the LEFT JOIN becomes an INNER JOIN.
        "SELECT a.col1 FROM a LEFT JOIN b ON a.col1 = b.col1 WHERE b.col3 IN (" + INTS + ")",
        "SELECT a.col1 FROM a LEFT JOIN b ON a.col1 = b.col1 WHERE b.col3 NOT IN (" + INTS + ")",
        // The IN list on the join key is copied to the other side by JoinPushTransitivePredicates.
        "SELECT a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3 WHERE a.col3 IN (" + INTS + ")",
        "SELECT a.col1, b.col2 FROM a LEFT JOIN b ON a.col3 = b.col3 WHERE a.col3 NOT IN (" + INTS + ")",
        "WITH t AS (SELECT col1, col3 FROM a WHERE col3 IN (" + INTS + ")) "
            + "SELECT t1.col1 FROM t t1 JOIN t t2 ON t1.col1 = t2.col1",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") UNION ALL SELECT col1 FROM b WHERE col3 IN (" + INTS + ")",
        "SELECT col1 FROM a WHERE col3 IN (SELECT col3 FROM b WHERE col6 IN (" + INTS + "))",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") AND col6 IN (" + INTS + ")",
        "SELECT col2, COUNT(*) FROM a WHERE col3 IN (" + INTS + ") GROUP BY col2 ORDER BY COUNT(*) DESC LIMIT 5",
        "SELECT a.col2, b.col2, COUNT(*) FILTER (WHERE a.col1 = 'x') FROM a LEFT JOIN b ON a.col1 = b.col1 "
            + "LEFT JOIN c ON a.col2 = c.col2 WHERE a.col3 IN (" + INTS + ") AND a.ts > 10 "
            + "GROUP BY a.col2, b.col2 ORDER BY 3 DESC LIMIT 50",
        // Correlated sub-queries (the list is sealed after decorrelation).
        "SELECT col1 FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.col1 = a.col1 AND b.col3 IN (" + INTS + "))",
        "SELECT a.col1, (SELECT MAX(b.col6) FROM b WHERE b.col1 = a.col1 AND b.col3 IN (" + INTS + ")) FROM a",
        "SELECT col1 FROM a WHERE col3 NOT IN (SELECT col3 FROM b WHERE col6 IN (" + INTS + "))",
        // Set operations, sort, partition column, hints.
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") INTERSECT SELECT col1 FROM b WHERE col3 NOT IN (" + INTS
            + ")",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") EXCEPT SELECT col1 FROM b WHERE col6 IN (" + INTS + ")",
        "SELECT * FROM a WHERE col3 IN (" + INTS + ") ORDER BY col1 LIMIT 5 OFFSET 2",
        "SELECT col1 FROM a WHERE col2 IN (" + STRINGS + ")",
        "SELECT /*+ aggOptions(is_partitioned_by_group_by_keys='true') */ col2, COUNT(*) FROM a "
            + "WHERE col3 IN (" + INTS + ") GROUP BY col2",
        "SELECT /*+ joinOptions(join_strategy = 'lookup') */ a.col1, b.col2 FROM a JOIN b ON a.col1 = b.col1 "
            + "WHERE a.col3 IN (" + INTS + ")",
        "SELECT /*+ joinOptions(join_strategy = 'hash') */ col1 FROM a WHERE col3 IN (SELECT col3 FROM b "
            + "WHERE col6 IN (" + INTS + "))",
        "SELECT a.col1 FROM a /*+ tableOptions(partition_function='hashcode', partition_key='col2', "
            + "partition_size='4') */ JOIN b /*+ tableOptions(partition_function='hashcode', partition_key='col1', "
            + "partition_size='4') */ ON a.col2 = b.col1 WHERE a.col3 IN (" + INTS + ")",
        "SET useSpools=true; WITH t AS (SELECT col1, col3 FROM a WHERE col3 IN (" + INTS + ")) "
            + "SELECT t1.col1 FROM t t1 JOIN t t2 ON t1.col1 = t2.col1",
        "SET useLiteMode=true; SELECT col1 FROM a WHERE col3 IN (" + INTS + ") LIMIT 10",
        // Other predicates on the same column fold into the list before it is sealed, as without sealing.
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") AND col3 IN (" + OTHER_INTS + ")",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") OR col3 IN (" + OTHER_INTS + ")",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") AND col3 = 10",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") AND col3 = 3",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") OR col3 > 1000000",
        "SELECT col1 FROM a WHERE col3 NOT IN (" + INTS + ") AND col3 > 0",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") OR col3 BETWEEN 1000 AND 2000",
        // NOT over a sealed call, which un-sealing folds into the negated Sarg.
        "SELECT NOT (col3 IN (" + INTS + ")) FROM a",
        "SELECT COUNT(CASE WHEN col3 IN (" + INTS + ") THEN NULL ELSE 1 END) FROM a",
        // Joins keep their distribution after un-sealing, so the exchange above stays pre-partitioned.
        "SELECT a.col1, b.col1 FROM a JOIN b ON a.col1 = b.col1 JOIN c ON a.col1 = c.col1 WHERE a.col3 IN (" + INTS
            + ")",
        "SELECT a.col1, COUNT(*) FROM a JOIN b ON a.col1 = b.col1 WHERE b.col3 IN (" + INTS + ") GROUP BY a.col1",
        // Other types.
        "SELECT col1 FROM a WHERE ts_timestamp IN (" + TIMESTAMPS + ")",
        "SELECT col1 FROM a WHERE col5 IN (" + BOOLEANS + ")",
        // Many comparisons of different operands, without an IN list.
        "SELECT col1 FROM a WHERE " + WIDE_AND,
        "SELECT col1 FROM a WHERE " + WIDE_OR,
        "SELECT a.col1 FROM a JOIN b ON a.col1 = b.col1 AND (" + WIDE_OR.replace("col6", "b.col6") + ")",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") AND " + WIDE_AND
    );
    List<Object[]> cases = new ArrayList<>();
    for (String query : queries) {
      cases.add(new Object[]{query});
      cases.add(new Object[]{"SET usePhysicalOptimizer=true; " + query});
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "samePlanQueries")
  public void testSamePlanAsWithoutSealing(String query) {
    String unsealed = "SET sealedInListThreshold=0; " + query;
    String explained = explain(query);
    assertFalse(explained.contains("$SEARCH#"), explained);
    assertEquals(explained, explain(unsealed));
    // The physical optimizer picks a random server for some single-worker stages, so hosts are not compared.
    assertEquals(implementationPlan(query), implementationPlan(unsealed));
    assertEquals(serializedStages(query), serializedStages(unsealed));
  }

  /// Null checks on the column of a large list must give the same plan as without sealing. This planner uses Calcite's
  /// `IS NULL` and `IS NOT NULL`, as the broker does with null handling. Calcite folds `x IS NOT NULL` into the Sarg of
  /// `x NOT IN (...)` as `NULL AS FALSE`, and the lists are sealed after this, so the plans stay the same.
  ///
  /// Not in this list: a null check in another clause than the list (for example the list in `JOIN ... ON` and
  /// `x IS NOT NULL` in `WHERE`) only meets the sealed list during optimization. Calcite then drops the null check as
  /// redundant, as it does next to `x < 5`. This is correct, because with null handling the servers apply SQL null
  /// semantics. [#testNullChecksWithoutNullHandling] covers these queries without null handling.
  @DataProvider(name = "nullableColumnQueries")
  public Object[][] nullableColumnQueries() {
    List<String> queries = List.of(
        "SELECT col1 FROM a WHERE col3 NOT IN (" + INTS + ") AND col3 IS NOT NULL",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") AND col3 IS NOT NULL",
        "SELECT col1 FROM a WHERE NOT (col3 IN (" + INTS + ") OR col3 IS NULL)",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") OR col3 IS NULL",
        "SELECT col1 FROM a WHERE col3 NOT IN (" + INTS + ") OR col3 IN (" + INTS + ")",
        "SELECT col1 FROM a WHERE col3 NOT IN (" + INTS + ", NULL)",
        "SELECT col1 FROM a WHERE col3 NOT IN (" + INTS + ") AND col6 IS NOT NULL",
        "SELECT col3 IN (" + INTS + ") OR col3 IS NULL, col3 NOT IN (" + INTS + ") FROM a",
        "SELECT a.col1 FROM a LEFT JOIN b ON a.col1 = b.col1 WHERE b.col3 IN (" + INTS + ")",
        "SELECT a.col1 FROM a LEFT JOIN b ON a.col1 = b.col1 WHERE b.col3 NOT IN (" + INTS + ") OR b.col3 IS NULL",
        "SELECT COUNT(*) FILTER (WHERE col3 NOT IN (" + INTS + ") AND col3 IS NOT NULL) FROM a"
    );
    List<Object[]> cases = new ArrayList<>();
    for (String query : queries) {
      for (String options : List.of("", "SET enableNullHandling=true; ", "SET usePhysicalOptimizer=true; ")) {
        cases.add(new Object[]{options + query});
      }
    }
    return cases.toArray(new Object[0][]);
  }

  /// Null checks with the planner set up as the broker does it for queries without null handling. The planner then uses
  /// Pinot's `IS NULL` and `IS NOT NULL` operators, which Calcite cannot see into. So a null check stays in the plan,
  /// wherever it is, and the plan is the same as without sealing.
  @DataProvider(name = "nullChecksWithoutNullHandling")
  public Object[][] nullChecksWithoutNullHandling() {
    List<String> queries = List.of(
        // The null check is in another clause than the list.
        "SELECT a.col1 FROM a JOIN b ON a.col1 = b.col1 AND b.col3 NOT IN (" + INTS + ") WHERE b.col3 IS NOT NULL",
        "SELECT a.col1 FROM a LEFT JOIN b ON a.col1 = b.col1 AND b.col3 NOT IN (" + INTS + ") "
            + "WHERE b.col3 IS NOT NULL",
        "SELECT col1 FROM (SELECT col1, col3 FROM a WHERE col3 NOT IN (" + INTS + ")) t WHERE t.col3 IS NOT NULL",
        "SELECT col1 FROM (SELECT col1, col3 FROM a WHERE col3 IN (" + INTS + ")) t WHERE t.col3 IS NOT NULL",
        "WITH t AS (SELECT col1, col3 FROM a WHERE col3 NOT IN (" + INTS + ")) SELECT col1 FROM t "
            + "WHERE col3 IS NOT NULL",
        // The null check is on a column that the column of the list is joined on.
        "SELECT a.col1 FROM a JOIN b ON a.col3 = b.col6 WHERE a.col3 NOT IN (" + INTS + ") AND b.col6 IS NOT NULL",
        // The null check is in the same condition as the list.
        "SELECT col1 FROM a WHERE col3 NOT IN (" + INTS + ") AND col3 IS NOT NULL",
        "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") OR col3 IS NULL",
        // NOT IN over a sub-query adds null checks of its own.
        "SELECT col1 FROM (SELECT col1, col3 FROM a WHERE col3 NOT IN (" + INTS + ")) t "
            + "WHERE t.col3 NOT IN (SELECT col6 FROM b)"
    );
    List<Object[]> cases = new ArrayList<>();
    for (String query : queries) {
      cases.add(new Object[]{query});
      cases.add(new Object[]{"SET usePhysicalOptimizer=true; " + query});
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "nullChecksWithoutNullHandling")
  public void testNullChecksWithoutNullHandling(String query) {
    String unsealed = "SET sealedInListThreshold=0; " + query;
    String explained = explain(_withoutNullHandling, query);
    if (query.contains(" IS NOT NULL") || query.contains(" IS NULL")) {
      assertTrue(explained.contains(" NULL($"), explained);
    }
    assertEquals(explained, explain(_withoutNullHandling, unsealed));
    assertEquals(serializedStages(_withoutNullHandling, query), serializedStages(_withoutNullHandling, unsealed));
    // The list is sealed: no planner rule sees it.
    try (QueryEnvironment.CompiledQuery compiledQuery = _guardedWithoutNullHandling.compile(query)) {
      compiledQuery.planQuery(1);
    }
  }

  @Test(dataProvider = "nullableColumnQueries")
  public void testSamePlanAsWithoutSealingForNullableColumns(String query) {
    String unsealed = "SET sealedInListThreshold=0; " + query;
    assertEquals(explain(_nullableQueryEnvironment, query), explain(_nullableQueryEnvironment, unsealed));
    assertEquals(serializedStages(_nullableQueryEnvironment, query),
        serializedStages(_nullableQueryEnvironment, unsealed));
  }

  /// An IN list and a range on the same column fold into one Sarg with points and ranges, as without sealing. The
  /// leaf gets one `IN` and the range, not one range per value.
  @Test
  public void testInListOrRangeShipsInAndRange() {
    for (String options : List.of("", "SET usePhysicalOptimizer=true; ")) {
      String stages = String.join("\n",
          serializedStages(options + "SELECT col1 FROM a WHERE col3 IN (" + INTS + ") OR col3 > 1000000"));
      assertTrue(stages.contains("functionName: \"IN\""), stages);
      assertTrue(stages.contains("functionName: \"GREATER_THAN\""), stages);
      assertFalse(stages.contains("LESS_THAN_OR_EQUAL"), stages);
    }
  }

  /// Rules can put a literal in place of the sealed operand, or combine the sealed call with a contradicting
  /// predicate. Planning must still work.
  @Test
  public void testSealedCallWithLiteralOperand() {
    for (String query : List.of(
        "SELECT x FROM (SELECT 5 AS x, col1 FROM a) WHERE x IN (" + INTS + ")",
        "SELECT x FROM (SELECT 10 AS x, col1 FROM a) WHERE x IN (" + INTS + ")",
        "SELECT col1 FROM a WHERE col3 = 5 AND col3 IN (" + INTS + ")",
        "SELECT col1 FROM a WHERE col3 = 10 AND col3 IN (" + INTS + ")",
        "SELECT col1 FROM a WHERE col3 IS NULL AND col3 IN (" + INTS + ")")) {
      for (String prefix : List.of("", "SET usePhysicalOptimizer=true; ")) {
        try (QueryEnvironment.CompiledQuery compiledQuery = _queryEnvironment.compile(prefix + query)) {
          assertFalse(compiledQuery.planQuery(1).getQueryPlan().getQueryStages().isEmpty());
        }
      }
    }
  }

  /// The operators that the SQL tree had before sealing are put back after the conversion to relational algebra.
  @Test
  public void testSqlTreeIsRestored() {
    String query = "SELECT SUM(CASE WHEN col3 IN (" + INTS + ") THEN 1 ELSE 0 END) FROM a WHERE col3 NOT IN ("
        + INTS + ")";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    try (QueryEnvironment.CompiledQuery compiledQuery = _queryEnvironment.compile(query, sqlNodeAndOptions)) {
      List<SqlCall> inCalls = new ArrayList<>();
      sqlNodeAndOptions.getSqlNode().accept(new SqlBasicVisitor<Void>() {
        @Override
        public Void visit(SqlCall call) {
          if (call.getKind() == SqlKind.IN || call.getKind() == SqlKind.NOT_IN) {
            inCalls.add(call);
          }
          return super.visit(call);
        }
      });
      assertEquals(inCalls.size(), 2);
      for (SqlCall call : inCalls) {
        assertTrue(call instanceof SqlBasicCall);
        assertTrue(call.getOperator() == SqlStdOperatorTable.IN || call.getOperator() == SqlStdOperatorTable.NOT_IN,
            call.getOperator().getClass().getName());
      }
    }
  }

  /// Without sealing, these positions plan in cubic time (minutes for a few thousand values).
  @Test(timeOut = 60_000)
  public void testLargeListsOutsideWhereAreFast() {
    String values = IntStream.range(0, 5_000).mapToObj(Integer::toString).collect(Collectors.joining(", "));
    for (String query : List.of(
        "SELECT SUM(CASE WHEN col3 IN (" + values + ") THEN 1 ELSE 0 END) FROM a",
        "SELECT COUNT(*) FILTER (WHERE col3 NOT IN (" + values + ")) FROM a",
        "SELECT col3 IN (" + values + ") FROM a",
        "SELECT a.col1 FROM a JOIN b ON a.col1 = b.col1 AND b.col3 IN (" + values + ")",
        "SELECT col1, ROW_NUMBER() OVER (PARTITION BY CASE WHEN col3 IN (" + values + ") THEN 1 ELSE 0 END) FROM a")) {
      try (QueryEnvironment.CompiledQuery compiledQuery = _queryEnvironment.compile(query)) {
        assertFalse(compiledQuery.planQuery(1).getQueryPlan().getQueryStages().isEmpty());
      }
    }
  }

  /// A rule placed in every phase checks that no planner rule is offered a plan with a large `SEARCH`, or with a
  /// large `AND`/`OR` of comparisons that Calcite could fold into one.
  @Test(dataProvider = "samePlanQueries")
  public void testNoRuleSeesLargeSarg(String query) {
    if (query.contains("partition_key")) {
      // The guarded planner has no partition metadata for partition hints.
      return;
    }
    QueryEnvironment guarded = getGuardedQueryEnvironment();
    try (QueryEnvironment.CompiledQuery compiledQuery = guarded.compile(query)) {
      compiledQuery.planQuery(1);
    }
  }

  /// The guard of [#testNoRuleSeesLargeSarg] fails when sealing is off.
  @Test
  public void testGuardFailsWithoutSealing() {
    QueryEnvironment guarded = getGuardedQueryEnvironment();
    Throwable error = expectThrows(Throwable.class, () -> {
      try (QueryEnvironment.CompiledQuery compiledQuery =
          guarded.compile("SET sealedInListThreshold=0; SELECT col1 FROM a WHERE col3 IN (" + INTS + ")")) {
        compiledQuery.planQuery(1);
      }
    });
    while (error.getCause() != null && !(error instanceof AssertionError)) {
      error = error.getCause();
    }
    assertTrue(error instanceof AssertionError && error.getMessage().contains("was offered SEARCH"),
        String.valueOf(error));
  }

  private String explain(String query) {
    return explain(_queryEnvironment, query);
  }

  private static String explain(QueryEnvironment queryEnvironment, String query) {
    return explain(queryEnvironment, query, "EXPLAIN PLAN FOR ");
  }

  private static String explain(QueryEnvironment queryEnvironment, String query, String explainPrefix) {
    int split = query.lastIndexOf(';') + 1;
    String explainQuery = query.substring(0, split) + " " + explainPrefix + query.substring(split).trim();
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(explainQuery)) {
      return compiledQuery.explain(1, null).getExplainPlan();
    }
  }

  private String implementationPlan(String query) {
    return explain(_queryEnvironment, query, "EXPLAIN IMPLEMENTATION PLAN FOR ").replaceAll("@localhost:\\d+",
        "@host");
  }

  private List<String> serializedStages(String query) {
    return serializedStages(_queryEnvironment, query);
  }

  private static List<String> serializedStages(QueryEnvironment queryEnvironment, String query) {
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(query)) {
      DispatchableSubPlan plan = compiledQuery.planQuery(1).getQueryPlan();
      List<String> stages = new ArrayList<>();
      for (DispatchablePlanFragment fragment : plan.getQueryStagesWithoutRoot()) {
        // Also compare the segments of each worker.
        stages.add(PlanNodeSerializer.process(fragment.getPlanFragment().getFragmentRoot()) + "\n"
            + fragment.getWorkerIdToSegmentsMap());
      }
      return stages;
    }
  }

  private static QueryEnvironment getGuardedQueryEnvironment() {
    return newQueryEnvironment(TABLE_SCHEMAS, true);
  }

  /// Returns a planner that is set up like the broker for queries without null handling. With `guarded`, a
  /// [LargeSargGuardRule] runs before the first rule and after each rule of every phase.
  private static QueryEnvironment newQueryEnvironment(Map<String, Schema> schemas, boolean guarded) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey());
    }
    for (Map.Entry<String, List<String>> entry : SERVER1_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(1, entry.getKey(), segment);
      }
    }
    for (Map.Entry<String, List<String>> entry : SERVER2_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(2, entry.getKey(), segment);
      }
    }
    RoutingManager routingManager = factory.buildRoutingManager(null);
    TableCache tableCache = factory.buildTableCache();
    RuleSetCustomizer guard = new RuleSetCustomizer() {
      @Override
      public void customize(Phase phase, List<RelOptRule> rules) {
        List<RelOptRule> guarded = new ArrayList<>();
        guarded.add(new LargeSargGuardRule(phase + "_guard_0"));
        for (RelOptRule rule : rules) {
          guarded.add(rule);
          guarded.add(new LargeSargGuardRule(phase + "_guard_" + guarded.size()));
        }
        rules.clear();
        rules.addAll(guarded);
      }
    };
    ImmutableQueryEnvironment.Config.Builder config = QueryEnvironment.configBuilder()
        .requestId(1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(tableCache)
        .workerManager(new WorkerManager("Broker_localhost", "localhost", 3, routingManager))
        .isNullHandlingEnabled(false)
        .defaultSealedInListThreshold(GUARD_THRESHOLD);
    if (guarded) {
      config.ruleSet(new PinotRuleSet(List.of(new DefaultRuleSetCustomizer(), guard)));
    }
    return new QueryEnvironment(config.build());
  }

  /// Never fires; fails when it is offered a node with an unsealed large Sarg, or with an AND/OR of many comparisons of
  /// one operand to literals.
  private static final class LargeSargGuardRule extends RelOptRule {
    LargeSargGuardRule(String description) {
      // Deprecated operand API, like the other Pinot rules that match any node: this rule never transforms.
      super(operand(RelNode.class, any()), description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      RelNode rel = call.rel(0);
      rel.accept(new RexShuttle() {
        @Override
        public RexNode visitCall(RexCall rexCall) {
          int size = 0;
          if (rexCall.getKind() == SqlKind.SEARCH) {
            size = ((RexLiteral) rexCall.getOperands().get(1)).getValueAs(Sarg.class).rangeSet.asRanges().size();
          } else if (rexCall.getKind() == SqlKind.AND || rexCall.getKind() == SqlKind.OR) {
            // Only comparisons of one operand can fold into one Sarg.
            Map<RexNode, Integer> comparisons = new HashMap<>();
            for (RexNode operand : rexCall.getOperands()) {
              RexNode compared = comparedToLiteral(operand);
              if (compared != null) {
                size = Math.max(size, comparisons.merge(compared, 1, Integer::sum));
              }
            }
          }
          if (size >= GUARD_THRESHOLD) {
            throw new AssertionError(description + " was offered " + rexCall.getKind() + " of size " + size + " in "
                + rel);
          }
          return super.visitCall(rexCall);
        }
      });
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
    }

    @Nullable
    private static RexNode comparedToLiteral(RexNode node) {
      if (!node.isA(SqlKind.COMPARISON)) {
        return null;
      }
      List<RexNode> operands = ((RexCall) node).getOperands();
      if (operands.size() != 2) {
        return null;
      }
      if (operands.get(1) instanceof RexLiteral) {
        return operands.get(0);
      }
      return operands.get(0) instanceof RexLiteral ? operands.get(1) : null;
    }
  }
}
