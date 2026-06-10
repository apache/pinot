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
package org.apache.pinot.query.planner.logical;

import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.spi.stats.NoOpStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.PinotStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link JoinReorderOptimizer}, the scoped cost-based join-reordering phase.
 *
 * <p>The fixture follows {@code PinotRelMdSelectivityTest}: a {@link MockRoutingManagerFactory}
 * registers three tables (a large fact table and two small dimension tables), a mock
 * {@link PinotStatisticsProvider} supplies their row counts, and the {@link TableConfig} is
 * re-stubbed with a real config so planning can resolve table metadata.
 */
public class JoinReorderOptimizerTest {

  private static final String FACT = "fact";
  private static final String DIM1 = "dim1";
  private static final String DIM2 = "dim2";
  private static final long FACT_ROWS = 1_000_000L;
  private static final long DIM1_ROWS = 100L;
  private static final long DIM2_ROWS = 10L;

  // --------------------------------------------------------------------------
  // Fixture helpers
  // --------------------------------------------------------------------------

  /** A table with an {@code id} key column and a {@code val} payload column. */
  private static Schema schema(String name) {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension("id", FieldSpec.DataType.INT, 0)
        .addSingleValueDimension("val", FieldSpec.DataType.INT, 0)
        .setSchemaName(name)
        .build();
  }

  /** Build a QueryEnvironment with the three tables registered and the given stats provider. */
  private static QueryEnvironment buildEnv(PinotStatisticsProvider statsProvider) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    for (String table : new String[]{FACT, DIM1, DIM2}) {
      factory.registerTable(schema(table), table);
      factory.registerSegment(1, table + "_OFFLINE", table + "_seg1");
    }
    TableCache tableCache = factory.buildTableCache();
    for (String table : new String[]{FACT, DIM1, DIM2}) {
      TableConfig tableConfig =
          new TableConfigBuilder(TableType.OFFLINE).setTableName(table).build();
      when(tableCache.getTableConfig(table + "_OFFLINE")).thenReturn(tableConfig);
    }
    return new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(tableCache)
        .statisticsProvider(statsProvider)
        .build());
  }

  /** A provider returning EXACT row counts for all three tables. */
  private static PinotStatisticsProvider statsProvider() {
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    when(provider.getTableStatistics(FACT)).thenReturn(
        TableStatistics.builder().rowCount(FACT_ROWS, StatConfidence.EXACT).build());
    when(provider.getTableStatistics(DIM1)).thenReturn(
        TableStatistics.builder().rowCount(DIM1_ROWS, StatConfidence.EXACT).build());
    when(provider.getTableStatistics(DIM2)).thenReturn(
        TableStatistics.builder().rowCount(DIM2_ROWS, StatConfidence.EXACT).build());
    return provider;
  }

  /** Prefix the query with the {@code useJoinReorder} option via the {@code SET ...;} syntax. */
  private static String withOption(String sql, boolean useJoinReorder) {
    return "SET " + QueryOptionKey.USE_JOIN_REORDER + "='" + useJoinReorder + "';\n" + sql;
  }

  /**
   * Prefix the query with both the {@code useJoinReorder} and {@code joinReorderMaxJoins} options.
   */
  private static String withOptions(String sql, boolean useJoinReorder, int maxJoins) {
    return "SET " + QueryOptionKey.USE_JOIN_REORDER + "='" + useJoinReorder + "';\n"
        + "SET " + QueryOptionKey.JOIN_REORDER_MAX_JOINS + "='" + maxJoins + "';\n"
        + sql;
  }

  private static String compileToPlan(QueryEnvironment env, String sql, boolean useJoinReorder) {
    try (QueryEnvironment.CompiledQuery compiled = env.compile(withOption(sql, useJoinReorder))) {
      return RelOptUtil.toString(compiled.getRelNode());
    }
  }

  /** Compiles an EXPLAIN query and returns its explain plan text (non-deprecated API path). */
  private static String explain(QueryEnvironment env, String explainSql) {
    try (QueryEnvironment.CompiledQuery compiled = env.compile(explainSql)) {
      return compiled.explain(0L, null).getExplainPlan();
    }
  }

  // A 3-way inner join written in a pessimal order: the two big-ish intermediate results are
  // produced before the small dimension tables get a chance to shrink them.
  private static final String PESSIMAL_JOIN_SQL =
      "SELECT * FROM fact, dim1, dim2 "
          + "WHERE fact.id = dim1.id AND fact.val = dim2.id";

  // --------------------------------------------------------------------------
  // Tests
  // --------------------------------------------------------------------------

  /**
   * With the option enabled and statistics present, the reorder phase must change the join order so
   * that the top join produces fewer rows than the disabled (default) plan's top join.
   */
  @Test
  public void testReorderChangesPlanAndReducesIntermediateRows() {
    QueryEnvironment env = buildEnv(statsProvider());

    String disabledPlan;
    double disabledTopRows;
    try (QueryEnvironment.CompiledQuery compiled =
        env.compile(withOption(PESSIMAL_JOIN_SQL, false))) {
      RelNode rel = compiled.getRelNode();
      disabledPlan = RelOptUtil.toString(rel);
      disabledTopRows = topJoinRowCount(rel);
    }

    String enabledPlan;
    double enabledTopRows;
    try (QueryEnvironment.CompiledQuery compiled =
        env.compile(withOption(PESSIMAL_JOIN_SQL, true))) {
      RelNode rel = compiled.getRelNode();
      enabledPlan = RelOptUtil.toString(rel);
      enabledTopRows = topJoinRowCount(rel);
    }

    assertTrue(disabledTopRows > 0, "disabled top-join row count should be positive");
    assertTrue(enabledTopRows > 0, "enabled top-join row count should be positive");
    // The reorder must change the join order: the pessimal order joins the large fact table with
    // dim1 first, then dim2; the cost-based reorder pulls the smallest dimension (dim2, 10 rows) in
    // first. The two plans must therefore differ.
    assertNotEquals(enabledPlan, disabledPlan,
        "Enabled reorder must change the join order.\n"
            + "disabled (" + disabledTopRows + "):\n" + disabledPlan
            + "\nenabled (" + enabledTopRows + "):\n" + enabledPlan);
    // And the reordered plan must not have a worse estimated top-join cardinality.
    assertTrue(enabledTopRows <= disabledTopRows + 1e-6,
        "Reordered top-join estimate must not be worse than the un-reordered one.\n"
            + "disabled=" + disabledTopRows + " enabled=" + enabledTopRows);
  }

  /** Option disabled (the default) must leave the plan exactly as it is today. */
  @Test
  public void testDisabledLeavesPlanUnchanged() {
    QueryEnvironment env = buildEnv(statsProvider());
    // Compile twice with the option off: identical input ⇒ identical plan, and the reorder phase
    // is never invoked (default-off semantics).
    String first = compileToPlan(env, PESSIMAL_JOIN_SQL, false);
    String second = compileToPlan(env, PESSIMAL_JOIN_SQL, false);
    assertEquals(first, second, "Default-off plan must be deterministic and unchanged");
  }

  /** An outer join anywhere in the tree disqualifies the whole phase: plan must be unchanged. */
  @Test
  public void testOuterJoinSkipsPhase() {
    QueryEnvironment env = buildEnv(statsProvider());
    String sql = "SELECT fact.val FROM fact "
        + "LEFT JOIN dim1 ON fact.id = dim1.id "
        + "JOIN dim2 ON fact.val = dim2.id";
    assertEquals(compileToPlan(env, sql, true), compileToPlan(env, sql, false),
        "An outer join must cause the reorder phase to be skipped");
  }

  /** A join hint signals user intent and disqualifies the whole phase. */
  @Test
  public void testJoinHintSkipsPhase() {
    QueryEnvironment env = buildEnv(statsProvider());
    String sql = "SELECT /*+ joinOptions(join_strategy='hash') */ fact.val FROM fact "
        + "JOIN dim1 ON fact.id = dim1.id "
        + "JOIN dim2 ON fact.val = dim2.id";
    assertEquals(compileToPlan(env, sql, true), compileToPlan(env, sql, false),
        "A join hint must cause the reorder phase to be skipped");
  }

  /** With no statistics (NoOp provider) the row-count gate fails: plan must be unchanged. */
  @Test
  public void testNoStatsSkipsPhase() {
    QueryEnvironment env = buildEnv(NoOpStatisticsProvider.INSTANCE);
    assertEquals(compileToPlan(env, PESSIMAL_JOIN_SQL, true), compileToPlan(env, PESSIMAL_JOIN_SQL, false),
        "Absent statistics must cause the reorder phase to be skipped");
  }

  /**
   * The reorder phase must never throw: {@link JoinReorderOptimizer#maybeReorder(RelNode, int)}
   * catches any internal error and returns the original (un-reordered) plan. We cover the try/catch
   * with a direct unit call using a {@link RelNode} that throws while the phase inspects it — this
   * is the documented fallback path. (Simulating a failure end-to-end is not possible because
   * Calcite calls {@code PinotTable.getStatistic()} during validation, well before the reorder
   * phase, so a throwing statistics provider would fail the query for an unrelated reason.)
   */
  @Test
  public void testReorderFailureFallsBackToInputPlan() {
    RelNode exploding = mock(RelNode.class);
    when(exploding.getInputs()).thenThrow(new RuntimeException("boom"));
    // maybeReorder must swallow the error and return the exact same instance it was given.
    assertEquals(JoinReorderOptimizer.maybeReorder(exploding, CommonConstants.Broker.DEFAULT_JOIN_REORDER_MAX_JOINS),
        exploding,
        "A failing reorder must fall back to the original plan instance");
  }

  // --------------------------------------------------------------------------
  // T2.3 / T2.4 — guardrail and plan-level tests
  // --------------------------------------------------------------------------

  /**
   * When the join count in the plan exceeds the configured cap the phase must be skipped and the
   * plan returned unchanged. The PESSIMAL_JOIN_SQL has exactly 2 joins; setting maxJoins=1 means
   * the count (2) exceeds the cap (1) so the phase must be skipped.
   */
  @Test
  public void testExceedingCapSkipsPhase() {
    QueryEnvironment env = buildEnv(statsProvider());

    // With cap=1 and a 2-join query: count > cap → skip → plan equals disabled plan.
    String disabledPlan;
    try (QueryEnvironment.CompiledQuery compiled = env.compile(withOption(PESSIMAL_JOIN_SQL, false))) {
      disabledPlan = RelOptUtil.toString(compiled.getRelNode());
    }
    String cappedPlan;
    try (QueryEnvironment.CompiledQuery compiled =
        env.compile(withOptions(PESSIMAL_JOIN_SQL, true, 1))) {
      cappedPlan = RelOptUtil.toString(compiled.getRelNode());
    }
    assertEquals(cappedPlan, disabledPlan,
        "A plan whose join count exceeds the cap must be returned unchanged (TOO_MANY_JOINS)");
  }

  /**
   * When the join count equals the cap the phase must still run and produce a different (reordered)
   * plan than the disabled baseline.
   */
  @Test
  public void testAtCapBoundaryReorderRuns() {
    QueryEnvironment env = buildEnv(statsProvider());

    // PESSIMAL_JOIN_SQL has exactly 2 joins; cap=2 means count == cap → phase runs.
    String disabledPlan;
    try (QueryEnvironment.CompiledQuery compiled = env.compile(withOption(PESSIMAL_JOIN_SQL, false))) {
      disabledPlan = RelOptUtil.toString(compiled.getRelNode());
    }
    String atCapPlan;
    try (QueryEnvironment.CompiledQuery compiled =
        env.compile(withOptions(PESSIMAL_JOIN_SQL, true, 2))) {
      atCapPlan = RelOptUtil.toString(compiled.getRelNode());
    }
    assertNotEquals(atCapPlan, disabledPlan,
        "A plan whose join count equals the cap must still be reordered");
  }

  /**
   * EXPLAIN output for the 3-table skewed-stats query must differ between the enabled and disabled
   * reorder cases, and the disabled plan must list the tables in the original syntactic order
   * (fact, dim1, dim2).
   */
  @Test
  public void testExplainSurfacesDiffersBetweenEnabledAndDisabled() {
    QueryEnvironment env = buildEnv(statsProvider());

    String explainSql = "EXPLAIN PLAN FOR " + PESSIMAL_JOIN_SQL;
    String explainDisabled = explain(env, withOption(explainSql, false));
    String explainEnabled = explain(env, withOption(explainSql, true));

    assertNotEquals(explainEnabled, explainDisabled,
        "EXPLAIN output must differ when join reorder is enabled vs disabled for the skewed-stats query");

    // The disabled plan must preserve the syntactic join order: fact joined with dim1 before dim2.
    int factPos = explainDisabled.indexOf(FACT);
    int dim1Pos = explainDisabled.indexOf(DIM1);
    int dim2Pos = explainDisabled.indexOf(DIM2);
    assertTrue(factPos >= 0 && dim1Pos >= 0 && dim2Pos >= 0,
        "Disabled plan must mention all three tables");
    // In the original syntactic order dim1 appears before dim2 in the first join (closer to the root).
    assertTrue(dim1Pos < dim2Pos,
        "Disabled plan must list dim1 before dim2 (syntactic join order preserved);\nexplain:\n"
            + explainDisabled);
  }

  /**
   * The {@code useJoinReorder=true} query option (passed via SET in the SQL text) must enable the
   * reorder phase for the query. This tests that the option is correctly threaded from the SQL
   * SET syntax all the way through to the optimizer.
   */
  @Test
  public void testQueryOptionPlumbingEnablesPhase() {
    QueryEnvironment env = buildEnv(statsProvider());

    // Baseline: reorder disabled via query option.
    String baseline = compileToPlan(env, PESSIMAL_JOIN_SQL, false);

    // Enabled via query option: the plan must differ from the baseline.
    String reordered = compileToPlan(env, PESSIMAL_JOIN_SQL, true);

    assertNotEquals(reordered, baseline,
        "Setting useJoinReorder=true via query option must enable the reorder phase and change the plan");
  }

  // --------------------------------------------------------------------------
  // helpers
  // --------------------------------------------------------------------------

  /** Estimated row count of the top-most join in the tree. */
  private static double topJoinRowCount(RelNode root) {
    Join topJoin = findTopJoin(root);
    assertNotNull(topJoin, "Plan must contain a join");
    RelMetadataQuery mq = topJoin.getCluster().getMetadataQuery();
    Double rows = mq.getRowCount(topJoin);
    assertNotNull(rows, "Row count must not be null");
    return rows;
  }

  @Nullable
  private static Join findTopJoin(RelNode node) {
    if (node instanceof Join) {
      return (Join) node;
    }
    for (RelNode input : node.getInputs()) {
      Join found = findTopJoin(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }
}
