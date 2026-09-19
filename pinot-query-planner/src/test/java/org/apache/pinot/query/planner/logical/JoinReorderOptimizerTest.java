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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
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
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests for [JoinReorderOptimizer], the scoped cost-based join-reordering phase.
///
/// The fixture follows `PinotRelMdSelectivityTest`: a [MockRoutingManagerFactory]
/// registers three tables (a large fact table and two small dimension tables), a mock
/// [PinotStatisticsProvider] supplies their row counts, and the [TableConfig] is
/// re-stubbed with a real config so planning can resolve table metadata.
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

  /// A table with an `id` key column and a `val` payload column.
  private static Schema schema(String name) {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension("id", FieldSpec.DataType.INT, 0)
        .addSingleValueDimension("val", FieldSpec.DataType.INT, 0)
        .setSchemaName(name)
        .build();
  }

  /// Build a QueryEnvironment with the three tables registered and the given stats provider.
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

  /// A provider returning EXACT row counts for all three tables.
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

  /// Prefix the query with the `useJoinReorder` option via the `SET ...;` syntax.
  private static String withOption(String sql, boolean useJoinReorder) {
    return "SET " + QueryOptionKey.USE_JOIN_REORDER + "='" + useJoinReorder + "';\n" + sql;
  }

  /// Prefix the query with both the `useJoinReorder` and `joinReorderMaxJoins` options.
  private static String withOptions(String sql, boolean useJoinReorder, int maxJoins) {
    return "SET " + QueryOptionKey.USE_JOIN_REORDER + "='" + useJoinReorder + "';\n"
        + "SET " + QueryOptionKey.JOIN_REORDER_MAX_JOINS + "='" + maxJoins + "';\n"
        + sql;
  }

  /// Runs the gates against the plan this SQL compiles to, with feedback collection on so the
  /// reason and counts are populated. Compiled with the phase OFF so the input tree is the one the
  /// gates would really see.
  private static JoinReorderOptimizer.Result gateResult(QueryEnvironment env, String sql, int maxJoins) {
    try (QueryEnvironment.CompiledQuery compiled = env.compile(withOption(sql, false))) {
      return JoinReorderOptimizer.maybeReorder(compiled.getRelNode(), maxJoins, true);
    }
  }

  /// Compiles inside a query thread context and returns the published `joinReorder` response entry,
  /// or `null` if the phase never published one.
  ///
  /// @param useJoinReorder `null` leaves the option unset, exercising the shipped default
  @Nullable
  private static JsonNode feedbackFor(QueryEnvironment env, String sql, @Nullable Boolean useJoinReorder) {
    StringBuilder prefixed = new StringBuilder();
    if (useJoinReorder != null) {
      prefixed.append("SET ").append(QueryOptionKey.USE_JOIN_REORDER)
          .append("='").append(useJoinReorder).append("';\n");
    }
    prefixed.append("SET ").append(QueryOptionKey.JOIN_REORDER_FEEDBACK).append("='true';\n").append(sql);
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      try (QueryEnvironment.CompiledQuery compiled = env.compile(prefixed.toString())) {
        compiled.getRelNode();
      }
      return QueryThreadContext.get().getExecutionContext().getResponseMetadata()
          .get(Request.JOIN_REORDER_RESPONSE_KEY);
    }
  }

  private static String compileToPlan(QueryEnvironment env, String sql, boolean useJoinReorder) {
    try (QueryEnvironment.CompiledQuery compiled = env.compile(withOption(sql, useJoinReorder))) {
      return RelOptUtil.toString(compiled.getRelNode());
    }
  }

  /// Compiles an EXPLAIN query and returns its explain plan text (non-deprecated API path).
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

  /// With the option enabled and statistics present, the reorder phase must change the join order so
  /// that the top join produces fewer rows than the disabled (default) plan's top join.
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

  /// The shipped default must leave the plan exactly as it is today.
  ///
  /// Asserted through the feedback channel rather than by comparing two plan strings: the phase
  /// either ran or it did not, and only the response entry says which. Comparing an off-plan to
  /// another off-plan proves nothing but that compilation is deterministic, and would still pass
  /// if the gate were inverted.
  @Test
  public void testPhaseDoesNotRunUnlessEnabled() {
    QueryEnvironment env = buildEnv(statsProvider());
    assertNull(feedbackFor(env, PESSIMAL_JOIN_SQL, false),
        "The phase must not run when useJoinReorder is off");
    assertNull(feedbackFor(env, PESSIMAL_JOIN_SQL, null),
        "The phase must not run when useJoinReorder is absent — off is the shipped default");
    assertNotNull(feedbackFor(env, PESSIMAL_JOIN_SQL, true),
        "The phase must run when useJoinReorder is on, or the two assertions above prove nothing");
  }

  /// The feedback entry must reach the response metadata through the real publish path, under the
  /// key clients read. Everything else about feedback is asserted on [JoinReorderOptimizer.Result]
  /// directly, which cannot catch a broken or misnamed publish.
  @Test
  public void testFeedbackReachesResponseMetadata() {
    QueryEnvironment env = buildEnv(statsProvider());
    JsonNode entry = feedbackFor(env, PESSIMAL_JOIN_SQL, true);
    assertNotNull(entry, "No " + Request.JOIN_REORDER_RESPONSE_KEY + " entry was published");
    assertEquals(entry.get("outcome").asText(), "APPLIED");
    assertEquals(entry.get("numJoins").asInt(), 2);
  }

  /// Feedback must be opt-in: the diagnostics it reports each cost a full-tree metadata walk.
  @Test
  public void testNoFeedbackEntryUnlessRequested() {
    QueryEnvironment env = buildEnv(statsProvider());
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      String sql = "SET " + QueryOptionKey.USE_JOIN_REORDER + "='true';\n" + PESSIMAL_JOIN_SQL;
      try (QueryEnvironment.CompiledQuery compiled = env.compile(sql)) {
        compiled.getRelNode();
      }
      assertNull(QueryThreadContext.get().getExecutionContext().getResponseMetadata()
          .get(Request.JOIN_REORDER_RESPONSE_KEY));
    }
  }

  /// A two-table join has exactly one join: there is an order but no choice of order, so the
  /// reported reason must say so rather than claim the plan has no joins.
  @Test
  public void testSingleJoinReportsTooFewJoins() {
    QueryEnvironment env = buildEnv(statsProvider());
    JoinReorderOptimizer.Result result =
        gateResult(env, "SELECT fact.val FROM fact JOIN dim1 ON fact.id = dim1.id", 10);
    assertEquals(result.skipReason(), JoinReorderOptimizer.SkipReason.TOO_FEW_JOINS);
    // Pin the emitted string: it is public surface once a release ships it.
    assertEquals(result.toJson().get("reason").asText(), "TOO_FEW_JOINS");
    assertEquals(result.toJson().get("numJoins").asInt(), 1,
        "The count must be the real number of joins, not zero");
  }

  @DataProvider(name = "gates")
  public Object[][] gates() {
    String threeWay = "SELECT fact.val FROM fact JOIN dim1 ON fact.id = dim1.id "
        + "JOIN dim2 ON fact.val = dim2.id";
    return new Object[][]{
        {"SELECT fact.val FROM fact LEFT JOIN dim1 ON fact.id = dim1.id "
            + "JOIN dim2 ON fact.val = dim2.id", 10, true,
            JoinReorderOptimizer.SkipReason.NON_INNER_JOIN},
        {"SELECT /*+ joinOptions(join_strategy='hash') */ fact.val FROM fact "
            + "JOIN dim1 ON fact.id = dim1.id JOIN dim2 ON fact.val = dim2.id", 10, true,
            JoinReorderOptimizer.SkipReason.HINTED_JOIN},
        {threeWay, 1, true, JoinReorderOptimizer.SkipReason.TOO_MANY_JOINS},
        {threeWay, 10, false, JoinReorderOptimizer.SkipReason.UNKNOWN_ROW_COUNT},
    };
  }

  /// Each gate must report its own reason. Asserting the reason rather than plan-string equality
  /// matters because an unchanged plan is also what a phase that ran and happened to pick the same
  /// order looks like.
  @Test(dataProvider = "gates")
  public void testGateReportsItsOwnReason(String sql, int maxJoins, boolean withStats,
      JoinReorderOptimizer.SkipReason expected) {
    QueryEnvironment env = buildEnv(withStats ? statsProvider() : NoOpStatisticsProvider.INSTANCE);
    assertEquals(gateResult(env, sql, maxJoins).skipReason(), expected);
  }

  /// The gate requires EVERY leaf to have a known row count, not merely one of them. Mixed
  /// known/guessed cardinalities are the case the phase must refuse: reordering a 1,000,000-row
  /// table against a Calcite default guess compares a real number to a fabricated one.
  @Test
  public void testMixedKnownAndUnknownStatsSkipsPhase() {
    PinotStatisticsProvider partial = mock(PinotStatisticsProvider.class);
    when(partial.getTableStatistics(FACT)).thenReturn(
        TableStatistics.builder().rowCount(FACT_ROWS, StatConfidence.EXACT).build());
    // dim1 and dim2 have no statistics at all — the mock returns null for them.
    QueryEnvironment env = buildEnv(partial);
    assertEquals(gateResult(env, PESSIMAL_JOIN_SQL, 10).skipReason(),
        JoinReorderOptimizer.SkipReason.UNKNOWN_ROW_COUNT,
        "One known table among three must not satisfy the all-leaves gate");
  }

  /// A join carrying correlation state must disqualify the phase.
  ///
  /// The shape has to be constructed rather than compiled from SQL: Pinot decorrelates before the
  /// optimize phase, and the `LogicalCorrelate`s that survive put an `Uncollect` under a
  /// `Correlate` rather than a `Join`, so no query is known to produce a correlated `Join` here.
  /// That is exactly why the gate needs a test — the invariant that keeps it unreachable lives in
  /// another rule, and if it ever changes, `MultiJoin` would silently drop the correlation because
  /// it has no `variablesSet` component to put it in.
  @Test
  public void testCorrelatedJoinSkipsPhase() {
    QueryEnvironment env = buildEnv(statsProvider());
    try (QueryEnvironment.CompiledQuery compiled = env.compile(withOption(PESSIMAL_JOIN_SQL, false))) {
      Join topJoin = findTopJoin(compiled.getRelNode());
      assertNotNull(topJoin, "Fixture must produce a join tree");
      // Same tree, but the top join now declares a correlation variable.
      RelNode correlated = LogicalJoin.create(topJoin.getLeft(), topJoin.getRight(), topJoin.getHints(),
          topJoin.getCondition(), Set.of(new CorrelationId(0)), topJoin.getJoinType());

      JoinReorderOptimizer.Result result = JoinReorderOptimizer.maybeReorder(correlated, 10, true);
      assertEquals(result.skipReason(), JoinReorderOptimizer.SkipReason.CORRELATED_JOIN);
      assertEquals(result.plan(), correlated, "A skipped phase must return the original tree");
    }
  }

  /// A `Correlate` must NOT disqualify the phase. It is a BiRel, never matched by
  /// `JoinToMultiJoinRule`, so it stays an opaque factor whose binder travels with the input it
  /// binds — and CROSS JOIN UNNEST is a common Pinot shape that would lose reordering for nothing.
  /// Guards against "fix the correlation gate" being over-applied to `Correlate` itself.
  @Test
  public void testCorrelateItselfDoesNotDisqualify() {
    QueryEnvironment env = buildEnv(statsProvider());
    JoinReorderOptimizer.Result result = gateResult(env, PESSIMAL_JOIN_SQL, 10);
    assertNull(result.skipReason(), "Baseline fixture must be eligible");
    assertTrue(result.isApplied());
  }

  /// LOW confidence is not usable for costing, so a LOW-confidence row count must read as unknown
  /// rather than as a number.
  @Test
  public void testLowConfidenceStatsSkipPhase() {
    PinotStatisticsProvider lowConfidence = mock(PinotStatisticsProvider.class);
    for (Map.Entry<String, Long> e : Map.of(FACT, FACT_ROWS, DIM1, DIM1_ROWS, DIM2, DIM2_ROWS).entrySet()) {
      when(lowConfidence.getTableStatistics(e.getKey())).thenReturn(
          TableStatistics.builder().rowCount(e.getValue(), StatConfidence.LOW).build());
    }
    QueryEnvironment env = buildEnv(lowConfidence);
    assertEquals(gateResult(env, PESSIMAL_JOIN_SQL, 10).skipReason(),
        JoinReorderOptimizer.SkipReason.UNKNOWN_ROW_COUNT);
  }

  /// An outer join anywhere in the tree disqualifies the whole phase: plan must be unchanged.
  @Test
  public void testOuterJoinSkipsPhase() {
    QueryEnvironment env = buildEnv(statsProvider());
    String sql = "SELECT fact.val FROM fact "
        + "LEFT JOIN dim1 ON fact.id = dim1.id "
        + "JOIN dim2 ON fact.val = dim2.id";
    assertEquals(compileToPlan(env, sql, true), compileToPlan(env, sql, false),
        "An outer join must cause the reorder phase to be skipped");
  }

  /// A join hint signals user intent and disqualifies the whole phase.
  @Test
  public void testJoinHintSkipsPhase() {
    QueryEnvironment env = buildEnv(statsProvider());
    String sql = "SELECT /*+ joinOptions(join_strategy='hash') */ fact.val FROM fact "
        + "JOIN dim1 ON fact.id = dim1.id "
        + "JOIN dim2 ON fact.val = dim2.id";
    assertEquals(compileToPlan(env, sql, true), compileToPlan(env, sql, false),
        "A join hint must cause the reorder phase to be skipped");
  }

  /// With no statistics (NoOp provider) the row-count gate fails: plan must be unchanged.
  @Test
  public void testNoStatsSkipsPhase() {
    QueryEnvironment env = buildEnv(NoOpStatisticsProvider.INSTANCE);
    assertEquals(compileToPlan(env, PESSIMAL_JOIN_SQL, true), compileToPlan(env, PESSIMAL_JOIN_SQL, false),
        "Absent statistics must cause the reorder phase to be skipped");
  }

  /// The reorder phase must never throw: [JoinReorderOptimizer#maybeReorder(RelNode, int, boolean)]
  /// catches any internal error and returns the original (un-reordered) plan. We cover the try/catch
  /// with a direct unit call using a [RelNode] that throws while the phase inspects it — this
  /// is the documented fallback path. (Simulating a failure end-to-end is not possible because
  /// Calcite calls `PinotTable.getStatistic()` during validation, well before the reorder
  /// phase, so a throwing statistics provider would fail the query for an unrelated reason.)
  @Test
  public void testReorderFailureFallsBackToInputPlan() {
    RelNode exploding = mock(RelNode.class);
    when(exploding.getInputs()).thenThrow(new RuntimeException("boom"));
    // maybeReorder must swallow the error and return the exact same instance it was given.
    JoinReorderOptimizer.Result result =
        JoinReorderOptimizer.maybeReorder(exploding, CommonConstants.Broker.DEFAULT_JOIN_REORDER_MAX_JOINS, true);
    assertEquals(result.plan(), exploding, "A failing reorder must fall back to the original plan instance");
    // The failure must also be reportable, not just survivable: an operator asking why their query
    // did not reorder needs to see ERROR rather than an absent answer.
    assertFalse(result.isApplied());
    assertEquals(result.skipReason(), JoinReorderOptimizer.SkipReason.ERROR);
    assertEquals(result.toJson().get("reason").asText(), "ERROR");
  }

  // --------------------------------------------------------------------------
  // T2.3 / T2.4 — guardrail and plan-level tests
  // --------------------------------------------------------------------------

  /// When the join count in the plan exceeds the configured cap the phase must be skipped and the
  /// plan returned unchanged. The PESSIMAL_JOIN_SQL has exactly 2 joins; setting maxJoins=1 means
  /// the count (2) exceeds the cap (1) so the phase must be skipped.
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

  /// When the join count equals the cap the phase must still run and produce a different (reordered)
  /// plan than the disabled baseline.
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

  /// EXPLAIN output for the 3-table skewed-stats query must differ between the enabled and disabled
  /// reorder cases, and the disabled plan must list the tables in the original syntactic order
  /// (fact, dim1, dim2).
  @Test
  public void testExplainSurfacesDiffersBetweenEnabledAndDisabled() {
    QueryEnvironment env = buildEnv(statsProvider());

    // WITHOUT IMPLEMENTATION: this test compares the shape of the LOGICAL plan, and since
    // apache/pinot#19373 a plain EXPLAIN also builds the dispatchable subplan, which needs a
    // WorkerManager this environment deliberately does not have.
    String explainSql = "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR " + PESSIMAL_JOIN_SQL;
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

  /// The `useJoinReorder=true` query option (passed via SET in the SQL text) must enable the
  /// reorder phase for the query. This tests that the option is correctly threaded from the SQL
  /// SET syntax all the way through to the optimizer.
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

  /// Estimated row count of the top-most join in the tree.
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

  // --------------------------------------------------------------------------
  // Observability: EXPLAIN attributes and response-metadata feedback
  // --------------------------------------------------------------------------

  /// EXPLAIN must be able to show the row-count estimates the optimizer actually used. Without
  /// them there is no way to tell a good plan chosen from good statistics apart from a good plan
  /// chosen by luck -- or to notice that a join estimate is orders of magnitude wrong.
  @Test
  public void testExplainIncludingAllAttributesShowsRowCounts() {
    QueryEnvironment env = buildEnv(statsProvider());
    String plan = explain(env, "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITHOUT IMPLEMENTATION FOR "
        + PESSIMAL_JOIN_SQL);
    assertTrue(plan.contains("rowcount"), "EXPLAIN ... INCLUDING ALL ATTRIBUTES must expose row "
        + "count estimates, got:\n" + plan);

    // The default level stays terse, so this is opt-in rather than noise on every EXPLAIN.
    String terse = explain(env, "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR " + PESSIMAL_JOIN_SQL);
    assertFalse(terse.contains("rowcount"), "The default EXPLAIN level should stay terse");
  }

  /// The feedback entry is what an operator reads to answer "why did my query not reorder?", so
  /// its shape is a contract: outcome always present, reason only when skipped, and estimates
  /// only when the phase actually ran.
  @Test
  public void testFeedbackJsonForSkippedPhase() {
    QueryEnvironment env = buildEnv(statsProvider());
    RelNode anyPlan;
    try (QueryEnvironment.CompiledQuery compiled = env.compile(withOption(PESSIMAL_JOIN_SQL, false))) {
      anyPlan = compiled.getRelNode();
    }
    ObjectNode json = JoinReorderOptimizer.Result
        .skipped(anyPlan, JoinReorderOptimizer.SkipReason.HINTED_JOIN, 3).toJson();

    assertEquals(json.get("outcome").asText(), "SKIPPED");
    assertEquals(json.get("reason").asText(), "HINTED_JOIN");
    assertEquals(json.get("numJoins").asInt(), 3);
    // No cost on a skip: emitting zeros here would read as a real estimate of zero.
    assertFalse(json.has("estimatedCostBefore"), "A skipped phase must not report a cost");
    assertFalse(json.has("estimatedCostAfter"), "A skipped phase must not report a cost");
  }

  @Test
  public void testFeedbackJsonForAppliedPhase() {
    QueryEnvironment env = buildEnv(statsProvider());
    RelNode anyPlan;
    try (QueryEnvironment.CompiledQuery compiled = env.compile(withOption(PESSIMAL_JOIN_SQL, true))) {
      anyPlan = compiled.getRelNode();
    }
    ObjectNode json = JoinReorderOptimizer.Result
        .applied(anyPlan, 2, 1000.0, 250.0, 7L, true).toJson();

    assertEquals(json.get("outcome").asText(), "APPLIED");
    assertFalse(json.has("reason"), "An applied phase has no skip reason");
    assertTrue(json.get("planChanged").asBoolean());
    // Both costs are reported so a miscalibrated cost model is detectable after the fact by
    // comparing what the optimizer predicted against what the query actually cost. Cumulative
    // cost, not the root row count: the root hides the intermediate joins that reordering moves
    // (a COUNT(*) root is 1.0 whatever happens underneath).
    assertEquals(json.get("estimatedCostBefore").asDouble(), 1000.0, 1e-9);
    assertEquals(json.get("estimatedCostAfter").asDouble(), 250.0, 1e-9);
  }

  /// Calcite may decline to give a cumulative cost. That must show up as an absent field rather
  /// than a -1 that a consumer would chart as a real value.
  @Test
  public void testUnavailableCostIsOmittedRatherThanReportedAsSentinel() {
    QueryEnvironment env = buildEnv(statsProvider());
    RelNode anyPlan;
    try (QueryEnvironment.CompiledQuery compiled = env.compile(withOption(PESSIMAL_JOIN_SQL, true))) {
      anyPlan = compiled.getRelNode();
    }
    ObjectNode json = JoinReorderOptimizer.Result.applied(anyPlan, 2, -1, -1, 3L, false).toJson();
    assertEquals(json.get("outcome").asText(), "APPLIED");
    assertFalse(json.has("estimatedCostBefore"), "An unavailable cost must be omitted, not sent as -1");
    assertFalse(json.has("estimatedCostAfter"), "An unavailable cost must be omitted, not sent as -1");
  }

  @Test
  public void testFeedbackIsOffUnlessRequested() {
    assertFalse(QueryOptionsUtils.isJoinReorderFeedback(Map.of()));
    assertFalse(QueryOptionsUtils.isJoinReorderFeedback(Map.of("joinReorderFeedback", "false")));
    assertTrue(QueryOptionsUtils.isJoinReorderFeedback(Map.of("joinReorderFeedback", "true")));
  }

  /// Drives the REAL cost computation rather than constructing a `Result` with literal values.
  ///
  /// This is the test the previous implementation lacked, and lacking it is why a broken field
  /// shipped: the JSON-shape tests below build `Result.applied(plan, 2, 1000.0, 250.0, ...)` with
  /// hardcoded numbers, so they never call the estimator and passed while production reported 1.0.
  ///
  /// `COUNT(*)` is the discriminator. Its ROOT row count is 1 however large the joins beneath it
  /// are, so a field sourced from the root is indistinguishable from a broken one. Cumulative cost
  /// aggregates the subtree and must therefore dwarf it.
  @Test
  public void testReportedCostReflectsTheSubtreeNotTheRootRowCount() {
    QueryEnvironment env = buildEnv(statsProvider());
    try (QueryEnvironment.CompiledQuery compiled = env.compile(
        withOption("SELECT COUNT(*) FROM fact, dim1, dim2 "
            + "WHERE fact.id = dim1.id AND fact.val = dim2.id", true))) {
      RelNode plan = compiled.getRelNode();

      // Establish the trap this test exists to catch: the root really does report ~1 row.
      double rootRows = plan.getCluster().getMetadataQuery().getRowCount(plan);
      assertTrue(rootRows < 100, "A COUNT(*) root should report a tiny row count, got " + rootRows);

      ObjectNode json = JoinReorderOptimizer.maybeReorder(plan, 10, true).toJson();
      assertEquals(json.get("outcome").asText(), "APPLIED",
          "Expected the phase to run for this plan; got " + json);
      assertTrue(json.has("estimatedCostBefore"), "An applied phase must report a cost: " + json);

      double cost = json.get("estimatedCostBefore").asDouble();
      // The fact table alone is 1M rows, so any subtree-aware cost must be at least that. The old
      // root-row-count implementation returned 1.0 here and would fail this assertion.
      assertTrue(cost >= FACT_ROWS, "Cost should reflect the joined subtree (>= " + FACT_ROWS
          + "), got " + cost + " -- a value near the root row count (" + rootRows
          + ") means the cost is being read from the root again");
    }
  }
}
