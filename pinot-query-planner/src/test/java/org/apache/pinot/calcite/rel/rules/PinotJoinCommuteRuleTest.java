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
package org.apache.pinot.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions.JoinHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.query.catalog.PinotTable;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link PinotJoinCommuteRule}.
 *
 * <p>Each test constructs a {@link LogicalJoin} backed by mock {@link RelOptTable} instances that wrap a real
 * {@link PinotTable} with the desired {@code isDimTable} flag. The HepPlanner is configured with only the rule
 * under test so that observed transformations are unambiguous.
 *
 * <p>The shared assertion shape after a successful commute is a {@link LogicalProject} whose input is the swapped
 * {@link LogicalJoin}. The Project is inserted by {@link org.apache.calcite.rel.rules.JoinCommuteRule#swap} to
 * preserve the original column order of the join output; we treat it as part of the contract and verify the
 * swapped join via {@code project.getInput(0)}.
 */
public class PinotJoinCommuteRuleTest {
  private static final TypeFactory TYPE_FACTORY = TypeFactory.INSTANCE;
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  private static final RelDataType ROW_TYPE = TYPE_FACTORY.builder()
      .add("k", SqlTypeName.INTEGER)
      .add("v", SqlTypeName.INTEGER)
      .build();

  private RelOptCluster _cluster;
  private HepPlanner _planner;

  @BeforeMethod
  public void setUp() {
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    programBuilder.addRuleInstance(PinotJoinCommuteRule.INSTANCE);
    _planner = new HepPlanner(programBuilder.build());
    _cluster = RelOptCluster.create(_planner, REX_BUILDER);
    _cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
    // Required so Calcite's hint validation accepts the {@code joinOptions} hint name when a rule that fires on
    // a hinted join (e.g. {@link #lookupHintWithDimOnLeftSwaps}) walks through the swap path.
    _cluster.setHintStrategies(PinotHintStrategyTable.PINOT_HINT_STRATEGY_TABLE);
  }

  // -------- Positive cases: rule fires --------

  @Test
  public void dimOnLeftInnerJoinSwaps() {
    LogicalTableScan dim = scan("dim", /*isDim=*/ true);
    LogicalTableScan fact = scan("fact", /*isDim=*/ false);
    LogicalJoin join = innerJoin(dim, fact);

    LogicalJoin commuted = runRuleAndExpectSwap(join);
    assertSame(unwrapTable(commuted.getLeft()), unwrapTable(fact), "fact should be on left after commute");
    assertSame(unwrapTable(commuted.getRight()), unwrapTable(dim), "dim should be on right after commute");
    assertEquals(commuted.getJoinType(), JoinRelType.INNER);
  }

  @Test
  public void dimOnLeftLeftJoinSwapsAndFlipsToRight() {
    LogicalTableScan dim = scan("dim", true);
    LogicalTableScan fact = scan("fact", false);
    LogicalJoin join = joinOf(dim, fact, JoinRelType.LEFT);

    LogicalJoin commuted = runRuleAndExpectSwap(join);
    // LEFT swaps to RIGHT — fact (originally right) is now left, dim (originally left) is now right.
    assertEquals(commuted.getJoinType(), JoinRelType.RIGHT);
    assertSame(unwrapTable(commuted.getLeft()), unwrapTable(fact));
    assertSame(unwrapTable(commuted.getRight()), unwrapTable(dim));
  }

  @Test
  public void dimOnLeftRightJoinSwapsAndFlipsToLeft() {
    LogicalTableScan dim = scan("dim", true);
    LogicalTableScan fact = scan("fact", false);
    LogicalJoin join = joinOf(dim, fact, JoinRelType.RIGHT);

    LogicalJoin commuted = runRuleAndExpectSwap(join);
    assertEquals(commuted.getJoinType(), JoinRelType.LEFT);
  }

  @Test
  public void dimOnLeftFullJoinSwaps() {
    LogicalTableScan dim = scan("dim", true);
    LogicalTableScan fact = scan("fact", false);
    LogicalJoin join = joinOf(dim, fact, JoinRelType.FULL);

    LogicalJoin commuted = runRuleAndExpectSwap(join);
    assertEquals(commuted.getJoinType(), JoinRelType.FULL);
  }

  @Test
  public void filterOnDimSwaps() {
    // Project/Filter chains on top of the dim scan still resolve to a dim-shaped input.
    LogicalTableScan dim = scan("dim", true);
    LogicalFilter filteredDim = LogicalFilter.create(dim, filterEqual(dim, 0, 42));
    LogicalTableScan fact = scan("fact", false);
    LogicalJoin join = innerJoin(filteredDim, fact);

    runRuleAndExpectSwap(join);
  }

  @Test
  public void projectOnDimSwaps() {
    LogicalTableScan dim = scan("dim", true);
    LogicalProject projectedDim = LogicalProject.create(dim, Collections.emptyList(),
        List.of(REX_BUILDER.makeInputRef(ROW_TYPE.getFieldList().get(0).getType(), 0),
            REX_BUILDER.makeInputRef(ROW_TYPE.getFieldList().get(1).getType(), 1)),
        List.of("k", "v"));
    LogicalTableScan fact = scan("fact", false);
    LogicalJoin join = innerJoin(projectedDim, fact);

    runRuleAndExpectSwap(join);
  }

  // -------- Negative cases: rule does not fire --------

  @Test
  public void dimOnRightInnerJoinDoesNotSwap() {
    LogicalTableScan dim = scan("dim", true);
    LogicalTableScan fact = scan("fact", false);
    LogicalJoin join = innerJoin(fact, dim); // already canonical
    runRuleAndExpectNoChange(join);
  }

  @Test
  public void bothDimDoesNotSwap() {
    LogicalTableScan dim1 = scan("dim1", true);
    LogicalTableScan dim2 = scan("dim2", true);
    runRuleAndExpectNoChange(innerJoin(dim1, dim2));
  }

  @Test
  public void neitherDimDoesNotSwap() {
    LogicalTableScan fact1 = scan("fact1", false);
    LogicalTableScan fact2 = scan("fact2", false);
    runRuleAndExpectNoChange(innerJoin(fact1, fact2));
  }

  @Test
  public void semiJoinWithDimOnLeftDoesNotSwap() {
    runRuleAndExpectNoChange(joinOf(scan("dim", true), scan("fact", false), JoinRelType.SEMI));
  }

  @Test
  public void antiJoinWithDimOnLeftDoesNotSwap() {
    runRuleAndExpectNoChange(joinOf(scan("dim", true), scan("fact", false), JoinRelType.ANTI));
  }

  /// ASOF anchors the temporal scan on the left side; commuting it would invert the ASOF semantics. The rule
  /// short-circuits on {@code instanceof LogicalAsofJoin} before the dim-detection walk. Otherwise this join
  /// — dim on left, fact on right, one equi-key — would meet every other predicate.
  @Test
  public void asofJoinWithDimOnLeftDoesNotSwap() {
    LogicalTableScan dim = scan("dim", true);
    LogicalTableScan fact = scan("fact", false);
    int leftFieldCount = dim.getRowType().getFieldCount();
    RexNode condition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(dim, 0),
        REX_BUILDER.makeInputRef(fact, 0).accept(new InputRefShift(leftFieldCount)));
    RexNode matchCondition = REX_BUILDER.makeLiteral(true);
    LogicalAsofJoin asofJoin = LogicalAsofJoin.create(dim, fact, ImmutableList.of(), condition, matchCondition,
        JoinRelType.LEFT_ASOF, ImmutableList.of());
    runRuleAndExpectNoChange(asofJoin);
  }

  @Test
  public void cartesianJoinWithDimOnLeftDoesNotSwap() {
    // No equi-key — even though dim is on the left, the rule skips because the build/broadcast policy for
    // NonEquiJoinOperator is out of scope for v1.
    LogicalTableScan dim = scan("dim", true);
    LogicalTableScan fact = scan("fact", false);
    RexNode alwaysTrue = REX_BUILDER.makeLiteral(true);
    LogicalJoin join = LogicalJoin.create(dim, fact, ImmutableList.of(), alwaysTrue, Collections.emptySet(),
        JoinRelType.INNER);
    runRuleAndExpectNoChange(join);
  }

  @DataProvider(name = "nonLookupJoinStrategies")
  public Object[][] nonLookupJoinStrategies() {
    return new Object[][] {
        {JoinHintOptions.HASH_JOIN_STRATEGY},
        {JoinHintOptions.DYNAMIC_BROADCAST_JOIN_STRATEGY},
    };
  }

  /**
   * {@code join_strategy='hash'} and {@code join_strategy='dynamic_broadcast'} are deliberate user intent that
   * the rule must not silently invert. (LOOKUP is the special case — see {@link #lookupHintWithDimOnLeftSwaps}.)
   * Note that a single HepPlanner instance must not be reused across iterations of this verification because it
   * remembers equivalent sets across {@code setRoot} calls; using a {@code @DataProvider} gives each invocation
   * a fresh planner from {@link #setUp()}.
   */
  @Test(dataProvider = "nonLookupJoinStrategies")
  public void nonLookupJoinStrategyHintDoesNotSwap(String strategy) {
    LogicalJoin join = innerJoinWithJoinOptionHint(JoinHintOptions.JOIN_STRATEGY, strategy);
    runRuleAndExpectNoChange(join);
  }

  /**
   * {@code join_strategy='lookup'} with dim-on-left is the one hint case where the user's intent is unsatisfiable
   * as written — {@code LookupJoinOperator} requires dim on the right. The rule auto-corrects by committing to
   * commute, so the downstream lookup-join planning pass sees a satisfiable plan. The lookup hint is preserved
   * on the swapped join via Calcite's {@code Join.copy()}.
   */
  @Test
  public void lookupHintWithDimOnLeftSwaps() {
    LogicalJoin join = innerJoinWithJoinOptionHint(JoinHintOptions.JOIN_STRATEGY, JoinHintOptions.LOOKUP_JOIN_STRATEGY);
    LogicalJoin commuted = runRuleAndExpectSwap(join);
    // Confirm tables are swapped: the original left was dim, after commute right must be dim.
    PinotTable rightAfter = unwrapTable(commuted.getRight());
    assertTrue(rightAfter.isDimTable(), "Right side after commute must be the dim table");
    // The lookup hint must travel with the swapped join so LookupJoinRule still recognizes it.
    assertEquals(JoinHintOptions.getJoinStrategyHint(commuted), JoinHintOptions.LOOKUP_JOIN_STRATEGY,
        "Lookup hint must be preserved on the commuted join so downstream rules still see it");
  }

  @DataProvider(name = "pinnedSideHints")
  public Object[][] pinnedSideHints() {
    return new Object[][] {
        {JoinHintOptions.LEFT_DISTRIBUTION_TYPE, PinotHintOptions.DistributionType.BROADCAST_HINT},
        {JoinHintOptions.RIGHT_DISTRIBUTION_TYPE, PinotHintOptions.DistributionType.HASH_HINT},
        {JoinHintOptions.IS_COLOCATED_BY_JOIN_KEYS, "true"},
    };
  }

  /// Any distribution/colocation hint signals explicit user intent about side roles. Each key path through
  /// {@code hasUserPinnedSideHint} must short-circuit; if a future refactor accidentally drops a key from the
  /// OR clause this parameterized run catches it.
  @Test(dataProvider = "pinnedSideHints")
  public void pinnedSideHintDoesNotSwap(String hintKey, String hintValue) {
    runRuleAndExpectNoChange(innerJoinWithJoinOptionHint(hintKey, hintValue));
  }

  @Test
  public void aggregateAboveDimScanDoesNotSwap() {
    // Aggregate changes cardinality semantics: an aggregate over a dim table is not itself dim-sized in general
    // (and our walker conservatively rejects multi-input / cardinality-changing nodes).
    LogicalTableScan dim = scan("dim", true);
    LogicalAggregate aggregate = LogicalAggregate.create(dim, ImmutableList.of(), ImmutableBitSet.of(0),
        ImmutableList.of(), ImmutableList.<AggregateCall>of());
    LogicalTableScan fact = scan("fact", false);
    // The aggregate has one output column (the group-by key); join its col 0 against fact's col 0.
    int aggFieldCount = aggregate.getRowType().getFieldCount();
    RexNode cond = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(aggregate, 0),
        new RexInputRef(aggFieldCount, fact.getRowType().getFieldList().get(0).getType()));
    LogicalJoin join = LogicalJoin.create(aggregate, fact, ImmutableList.of(), cond, Collections.emptySet(),
        JoinRelType.INNER);
    runRuleAndExpectNoChange(join);
  }

  @Test
  public void joinAboveDimScanDoesNotSwap() {
    // An inner join below the candidate join is multi-input; our walker bails at Join, so the outer join's left
    // input is correctly classified as not-dim-shaped even though every leaf is a dim table.
    LogicalTableScan dimA = scan("dim_a", true);
    LogicalTableScan dimB = scan("dim_b", true);
    LogicalJoin nestedDimJoin = innerJoin(dimA, dimB);
    LogicalTableScan fact = scan("fact", false);
    int nestedFieldCount = nestedDimJoin.getRowType().getFieldCount();
    RexNode cond = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(nestedDimJoin, 0),
        new RexInputRef(nestedFieldCount, fact.getRowType().getFieldList().get(0).getType()));
    LogicalJoin join = LogicalJoin.create(nestedDimJoin, fact, ImmutableList.of(), cond, Collections.emptySet(),
        JoinRelType.INNER);
    runRuleAndExpectNoChange(join);
  }

  // -------- Idempotency --------

  @Test
  public void idempotentAppliesOnlyOnce() {
    LogicalTableScan dim = scan("dim", true);
    LogicalTableScan fact = scan("fact", false);
    LogicalJoin join = innerJoin(dim, fact);

    _planner.setRoot(join);
    RelNode firstRun = _planner.findBestExp();
    LogicalJoin commuted = extractJoinFromResult(firstRun);
    assertSame(unwrapTable(commuted.getRight()), unwrapTable(dim), "first run should put dim on right");

    // Re-run the planner on the commuted result. Since the predicate now sees dim on the right, the rule
    // must NOT fire again.
    _planner.setRoot(firstRun);
    RelNode secondRun = _planner.findBestExp();
    LogicalJoin secondCommuted = extractJoinFromResult(secondRun);
    assertSame(unwrapTable(secondCommuted.getRight()), unwrapTable(dim), "dim must remain on right after re-run");
    assertSame(unwrapTable(secondCommuted.getLeft()), unwrapTable(fact), "fact must remain on left after re-run");
  }

  // -------- Helpers --------

  private LogicalJoin runRuleAndExpectSwap(LogicalJoin original) {
    _planner.setRoot(original);
    RelNode result = _planner.findBestExp();
    assertTrue(result instanceof LogicalProject,
        "Expected a Project (inserted by JoinCommuteRule.swap) wrapping the swapped Join; got " + result.getClass());
    return extractJoinFromResult(result);
  }

  /**
   * Verifies that the rule did not commute the join. HepPlanner re-numbers nodes even when no rule fires, so
   * we cannot rely on instance identity ({@code assertSame}). The smoking gun for a successful commute is
   * (a) a Project wrapping the join and (b) the underlying tables swapped. Absence of (a) plus matching table
   * identities under (b) = no commute happened.
   *
   * <p>Accepts the abstract {@link Join} (not just {@link LogicalJoin}) so the same assertion can verify the
   * {@link LogicalAsofJoin} early-bail path — Calcite's commute returns a different concrete class, so we also
   * check that the result class matches the input class.
   */
  private void runRuleAndExpectNoChange(Join original) {
    PinotTable expectedLeftTable = tryUnwrapTable(original.getLeft());
    PinotTable expectedRightTable = tryUnwrapTable(original.getRight());

    _planner.setRoot(original);
    RelNode result = _planner.findBestExp();

    assertTrue(result instanceof Join,
        "Expected the Join to be returned unchanged (no Project wrap from commute); got " + result.getClass());
    Join returned = (Join) result;
    assertEquals(returned.getClass(), original.getClass(),
        "Concrete join class must be unchanged (e.g. LogicalAsofJoin must not become LogicalJoin)");
    assertEquals(returned.getJoinType(), original.getJoinType(), "Join type must be unchanged");

    PinotTable returnedLeft = tryUnwrapTable(returned.getLeft());
    PinotTable returnedRight = tryUnwrapTable(returned.getRight());
    if (expectedLeftTable != null) {
      assertSame(returnedLeft, expectedLeftTable, "Left input table must be unchanged");
    }
    if (expectedRightTable != null) {
      assertSame(returnedRight, expectedRightTable, "Right input table must be unchanged");
    }
  }

  /**
   * Walks through {@link LogicalProject}/{@link LogicalFilter} to find the underlying {@link PinotTable}.
   * Returns {@code null} if the input is not a simple scan-chain (e.g. it sits below an Aggregate or Join);
   * callers treat {@code null} as "no table identity to compare against".
   */
  private static PinotTable tryUnwrapTable(RelNode rel) {
    RelNode current = rel;
    while (current instanceof LogicalProject || current instanceof LogicalFilter) {
      current = current.getInput(0);
    }
    if (current instanceof LogicalTableScan) {
      return ((LogicalTableScan) current).getTable().unwrap(PinotTable.class);
    }
    return null;
  }

  private static LogicalJoin extractJoinFromResult(RelNode root) {
    if (root instanceof LogicalJoin) {
      return (LogicalJoin) root;
    }
    if (root instanceof LogicalProject) {
      RelNode child = root.getInput(0);
      assertTrue(child instanceof LogicalJoin, "Project's child should be a Join; got " + child.getClass());
      return (LogicalJoin) child;
    }
    throw new AssertionError("Expected Join or Project(Join); got " + root.getClass());
  }

  private LogicalTableScan scan(String name, boolean isDim) {
    PinotTable pinotTable = new PinotTable(stubPinotSchema(name), false, isDim);
    RelOptTable mockTable = Mockito.mock(RelOptTable.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(mockTable.unwrap(PinotTable.class)).thenReturn(pinotTable);
    Mockito.when(mockTable.getRowType()).thenReturn(ROW_TYPE);
    Mockito.when(mockTable.getQualifiedName()).thenReturn(List.of("default", name));
    Mockito.when(mockTable.getRowCount()).thenReturn(100.0);
    Mockito.when(mockTable.getCollationList()).thenReturn(ImmutableList.of());
    Mockito.when(mockTable.getDistribution()).thenReturn(RelDistributions.ANY);
    return LogicalTableScan.create(_cluster, mockTable, ImmutableList.of());
  }

  private static Schema stubPinotSchema(String name) {
    return new Schema.SchemaBuilder()
        .setSchemaName(name)
        .addSingleValueDimension("k", FieldSpec.DataType.INT)
        .addSingleValueDimension("v", FieldSpec.DataType.INT)
        .build();
  }

  private LogicalJoin innerJoin(RelNode left, RelNode right) {
    return joinOf(left, right, JoinRelType.INNER);
  }

  private LogicalJoin joinOf(RelNode left, RelNode right, JoinRelType joinType) {
    int leftFieldCount = left.getRowType().getFieldCount();
    RexNode condition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(left, 0),
        REX_BUILDER.makeInputRef(right, 0).accept(new InputRefShift(leftFieldCount)));
    return LogicalJoin.create(left, right, ImmutableList.of(), condition, Collections.emptySet(), joinType);
  }

  /**
   * Builds an INNER join with a dim table on the left, a fact table on the right, and a single key/value entry in
   * the {@code joinOptions} hint. Used by negative tests that verify the rule honors explicit user hints.
   */
  private LogicalJoin innerJoinWithJoinOptionHint(String hintKey, String hintValue) {
    LogicalTableScan dim = scan("dim", true);
    LogicalTableScan fact = scan("fact", false);
    int leftFieldCount = dim.getRowType().getFieldCount();
    RexNode condition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(dim, 0),
        REX_BUILDER.makeInputRef(fact, 0).accept(new InputRefShift(leftFieldCount)));
    RelHint hint = RelHint.builder(PinotHintOptions.JOIN_HINT_OPTIONS).hintOption(hintKey, hintValue).build();
    return LogicalJoin.create(dim, fact, ImmutableList.of(hint), condition, Collections.emptySet(),
        JoinRelType.INNER);
  }

  private RexNode filterEqual(RelNode child, int columnIndex, int literalValue) {
    return REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(child, columnIndex),
        REX_BUILDER.makeLiteral(literalValue, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
  }

  private static PinotTable unwrapTable(RelNode rel) {
    RelNode current = rel;
    while (current instanceof LogicalProject || current instanceof LogicalFilter) {
      current = current.getInput(0);
    }
    assertTrue(current instanceof LogicalTableScan, "Expected to reach a TableScan, got " + current.getClass());
    PinotTable pinotTable = ((LogicalTableScan) current).getTable().unwrap(PinotTable.class);
    assertNotNull(pinotTable, "TableScan should be backed by a PinotTable");
    return pinotTable;
  }

  /**
   * RexShuttle helper to shift input-ref indexes by a fixed amount. Used to build cross-input join conditions
   * where the right-side reference must be offset by the left-side field count.
   */
  private static final class InputRefShift extends RexShuttle {
    private final int _shift;

    InputRefShift(int shift) {
      _shift = shift;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      return new RexInputRef(inputRef.getIndex() + _shift, inputRef.getType());
    }
  }
}
