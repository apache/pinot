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

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.calcite.rex.SearchSealer;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Unit tests for [PinotMvAggregatePreFilterRule].
public class PinotMvAggregatePreFilterRuleTest {
  private final RelDataTypeFactory _typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder _rexBuilder = new RexBuilder(_typeFactory);
  private final RelDataType _varcharType = _typeFactory.createSqlType(SqlTypeName.VARCHAR);
  private final RelDataType _intType = _typeFactory.createSqlType(SqlTypeName.INTEGER);
  private final RelDataType _bigintType = _typeFactory.createSqlType(SqlTypeName.BIGINT);
  private final RelDataType _varcharArrayType = _typeFactory.createArrayType(_varcharType, -1);
  private final RelDataType _intArrayType = _typeFactory.createArrayType(_intType, -1);

  private RelOptCluster newCluster() {
    return RelOptCluster.create(new HepPlanner(HepProgram.builder().build()), _rexBuilder);
  }

  private TableScan createTableScan(RelOptCluster cluster) {
    // Table t(sv VARCHAR, tags VARCHAR ARRAY, nums INT ARRAY)
    RelDataType rowType = _typeFactory.builder()
        .add("sv", _varcharType)
        .add("tags", _varcharArrayType)
        .add("nums", _intArrayType)
        .build();
    RelOptTable table = mock(RelOptTable.class);
    when(table.getRowType()).thenReturn(rowType);
    when(table.getQualifiedName()).thenReturn(List.of("default", "t"));
    return LogicalTableScan.create(cluster, table, List.of());
  }

  private RexNode arrayToMv(RexNode operand) {
    return _rexBuilder.makeCall(PinotOperatorTable.ARRAY_TO_MV, operand);
  }

  private AggregateCall countStar(RelNode input) {
    return AggregateCall.create(SqlParserPos.ZERO, SqlStdOperatorTable.COUNT, false, false, false, List.of(),
        List.of(), -1, null, RelCollations.EMPTY, false, input, _bigintType, "cnt");
  }

  private NlsString nls(String value) {
    return _rexBuilder.makeLiteral(value).getValueAs(NlsString.class);
  }

  private RelDataType literalVarcharType() {
    return _rexBuilder.makeLiteral("a").getType();
  }

  private RelNode runRule(RelNode root) {
    HepProgram program = HepProgram.builder()
        .addRuleInstance(PinotMvAggregatePreFilterRule.INSTANCE)
        .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(root);
    return planner.findBestExp();
  }

  /// Builds `Filter(topCond) -> Aggregate(group={0}, COUNT()) -> Project(ARRAY_TO_MV(tags)) -> TableScan(t)`.
  private Filter buildTagsScanPlan(RelOptCluster cluster, Function<Aggregate, RexNode> condFn) {
    TableScan scan = createTableScan(cluster);
    Project project = LogicalProject.create(scan, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(scan, 1))), List.of("k"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0), null,
        List.of(countStar(project)));
    return LogicalFilter.create(agg, condFn.apply(agg));
  }

  private void assertPreFilterInserted(RelNode result, String expectedLeafSubstr) {
    assertTrue(result instanceof Filter, "Top node must remain Filter");
    Aggregate agg = (Aggregate) ((Filter) result).getInput();
    Project project = (Project) agg.getInput();
    assertTrue(project.getInput() instanceof Filter,
        "Expected leaf pre-filter below Project, got: " + project.getInput());
    Filter leafFilter = (Filter) project.getInput();
    assertTrue(leafFilter.getInput() instanceof TableScan);
    String leafCond = leafFilter.getCondition().toString();
    assertTrue(leafCond.contains("ARRAY_TO_MV($1)"), leafCond);
    assertTrue(leafCond.contains(expectedLeafSubstr), leafCond);
  }

  private void assertNoPreFilterInserted(RelNode result) {
    assertTrue(result instanceof Filter, "Top node must remain Filter");
    Aggregate agg = (Aggregate) ((Filter) result).getInput();
    RelNode aggInput = agg.getInput();
    if (aggInput instanceof Project) {
      assertFalse(((Project) aggInput).getInput() instanceof Filter,
          "Expected no leaf pre-filter below Project, got: " + ((Project) aggInput).getInput());
    }
  }

  @Test
  public void testPositiveEqualsAndIdempotency() {
    RelOptCluster cluster = newCluster();
    Filter root = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeLiteral("vip")));

    RelNode firstRun = runRule(root);
    assertPreFilterInserted(firstRun, "vip");
    // Top filter must remain intact.
    assertEquals(((Filter) firstRun).getCondition(), root.getCondition());

    // Running again must be a strict no-op (idempotent).
    RelNode secondRun = runRule(firstRun);
    assertEquals(RelOptUtil.toString(secondRun), RelOptUtil.toString(firstRun));
  }

  @Test
  public void testReversedLiteralComparisonIsCanonicalized() {
    RelOptCluster cluster = newCluster();
    Filter root = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, _rexBuilder.makeLiteral("a"),
            _rexBuilder.makeInputRef(agg, 0)));

    RelNode result = runRule(root);
    assertPreFilterInserted(result, ">(");
    Filter leafFilter = (Filter) ((Project) ((Aggregate) ((Filter) result).getInput()).getInput()).getInput();
    assertTrue(leafFilter.getCondition().toString().contains(">(ARRAY_TO_MV($1)"),
        leafFilter.getCondition().toString());
  }

  @Test
  public void testRangeComparisonsAndBetween() {
    RelOptCluster cluster = newCluster();
    Filter root = buildTagsScanPlan(cluster, agg -> {
      RexNode ref = _rexBuilder.makeInputRef(agg, 0);
      return _rexBuilder.makeCall(SqlStdOperatorTable.AND,
          _rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ref, _rexBuilder.makeLiteral("b")),
          _rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, ref, _rexBuilder.makeLiteral("d")));
    });

    RelNode result = runRule(root);
    assertPreFilterInserted(result, ">=(ARRAY_TO_MV($1)");
    Filter leafFilter = (Filter) ((Project) ((Aggregate) ((Filter) result).getInput()).getInput()).getInput();
    assertTrue(leafFilter.getCondition().toString().contains("<=(ARRAY_TO_MV($1)"),
        leafFilter.getCondition().toString());

    // Pinot preserves BETWEEN ASYMMETRIC as a 3-operand call for MV columns.
    Filter betweenRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.BETWEEN, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeLiteral("b"), _rexBuilder.makeLiteral("d")));
    assertPreFilterInserted(runRule(betweenRoot), "BETWEEN");
  }

  @Test
  public void testPositiveSearchInListAndSealedSearch() {
    RelOptCluster cluster = newCluster();
    ImmutableRangeSet<NlsString> points = ImmutableRangeSet.<NlsString>builder()
        .add(Range.singleton(nls("a")))
        .add(Range.singleton(nls("b")))
        .build();
    Sarg<NlsString> sarg = Sarg.of(RexUnknownAs.UNKNOWN, points);
    RexLiteral sargLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg, literalVarcharType());

    Filter root = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, _rexBuilder.makeInputRef(agg, 0), sargLiteral));

    RelNode result = runRule(root);
    assertPreFilterInserted(result, "SEARCH(ARRAY_TO_MV($1)");

    // Sealed search with PinotSealedSearchOperator is also supported and stays sealed.
    SearchSealer sealer = new SearchSealer(2);
    RelNode sealedRoot = sealer.seal(root);
    RelNode sealedResult = runRule(sealedRoot);
    assertPreFilterInserted(sealedResult, "$SEARCH#");

    // SEARCH with nullAs = FALSE (still excludes NULL) on a point set is also sound and copied.
    Sarg<NlsString> nullFalseSarg = Sarg.of(RexUnknownAs.FALSE, points);
    RexLiteral nullFalseSargLiteral = _rexBuilder.makeSearchArgumentLiteral(nullFalseSarg, literalVarcharType());
    Filter nullFalseRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, _rexBuilder.makeInputRef(agg, 0),
            nullFalseSargLiteral));
    assertPreFilterInserted(runRule(nullFalseRoot), "SEARCH(ARRAY_TO_MV($1)");
  }

  @Test
  public void testPositiveOrOnSameMvKey() {
    RelOptCluster cluster = newCluster();
    Filter root = buildTagsScanPlan(cluster, agg -> {
      RexNode ref = _rexBuilder.makeInputRef(agg, 0);
      return _rexBuilder.makeCall(SqlStdOperatorTable.OR,
          _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, _rexBuilder.makeLiteral("a")),
          _rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ref, _rexBuilder.makeLiteral("x")));
    });

    RelNode result = runRule(root);
    assertPreFilterInserted(result, "OR(");
  }

  @Test
  public void testMergesWithExistingLeafWhereFilter() {
    RelOptCluster cluster = newCluster();
    TableScan scan = createTableScan(cluster);
    Filter existingLeaf = LogicalFilter.create(scan,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(scan, 0),
            _rexBuilder.makeLiteral("US")));
    Project project = LogicalProject.create(existingLeaf, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(existingLeaf, 1))), List.of("k"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0), null,
        List.of(countStar(project)));
    Filter topFilter = LogicalFilter.create(agg,
        _rexBuilder.makeCall(SqlStdOperatorTable.AND,
            _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
                _rexBuilder.makeLiteral("vip")),
            _rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, _rexBuilder.makeInputRef(agg, 1),
                _rexBuilder.makeExactLiteral(BigDecimal.valueOf(5)))));

    RelNode result = runRule(topFilter);
    assertPreFilterInserted(result, "vip");
    Filter leafFilter = (Filter) ((Project) ((Aggregate) ((Filter) result).getInput()).getInput()).getInput();
    String leafCond = leafFilter.getCondition().toString();
    // Merged into a single LogicalFilter over TableScan containing both US and vip, without cnt > 5.
    assertTrue(leafCond.contains("US"), leafCond);
    assertTrue(leafCond.contains("vip"), leafCond);
    assertFalse(leafCond.contains("5"), leafCond);
  }

  @Test
  public void testNegativePredicatesAreNotCopied() {
    RelOptCluster cluster = newCluster();

    // 1. NOT_EQUALS (<>)
    Filter neqRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeLiteral("a")));
    assertNoPreFilterInserted(runRule(neqRoot));

    // 2. NOT (k = 'a')
    Filter notRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.NOT,
            _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
                _rexBuilder.makeLiteral("a"))));
    assertNoPreFilterInserted(runRule(notRoot));

    // 3. IS NULL / IS NOT NULL
    Filter isNullRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, _rexBuilder.makeInputRef(agg, 0)));
    assertNoPreFilterInserted(runRule(isNullRoot));

    Filter isNotNullRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, _rexBuilder.makeInputRef(agg, 0)));
    assertNoPreFilterInserted(runRule(isNotNullRoot));

    // 4. Complemented points SEARCH (NOT IN)
    ImmutableRangeSet<NlsString> notInRanges = ImmutableRangeSet.<NlsString>builder()
        .add(Range.singleton(nls("a")))
        .add(Range.singleton(nls("b")))
        .build()
        .complement();
    RexLiteral notInSarg = _rexBuilder.makeSearchArgumentLiteral(Sarg.of(RexUnknownAs.UNKNOWN, notInRanges),
        literalVarcharType());
    Filter notInRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, _rexBuilder.makeInputRef(agg, 0), notInSarg));
    assertNoPreFilterInserted(runRule(notInRoot));

    // 5. SEARCH with nullAs = TRUE (includes NULL)
    ImmutableRangeSet<NlsString> points = ImmutableRangeSet.of(Range.singleton(nls("a")));
    RexLiteral nullTrueSarg = _rexBuilder.makeSearchArgumentLiteral(Sarg.of(RexUnknownAs.TRUE, points),
        literalVarcharType());
    Filter nullTrueRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, _rexBuilder.makeInputRef(agg, 0), nullTrueSarg));
    assertNoPreFilterInserted(runRule(nullTrueRoot));

    // 6. SEARCH representing IS NOT NULL (Range.all(), nullAs = FALSE)
    RexLiteral allSarg = _rexBuilder.makeSearchArgumentLiteral(
        Sarg.of(RexUnknownAs.FALSE, ImmutableRangeSet.<NlsString>of(Range.all())), literalVarcharType());
    Filter allSargRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, _rexBuilder.makeInputRef(agg, 0), allSarg));
    assertNoPreFilterInserted(runRule(allSargRoot));

    // 7. SEARCH whose complement contains a single-point range (e.g. k > 'a' AND k <> 'c')
    ImmutableRangeSet<NlsString> punctured = ImmutableRangeSet.<NlsString>builder()
        .add(Range.open(nls("a"), nls("c")))
        .add(Range.greaterThan(nls("c")))
        .build();
    RexLiteral puncturedSarg = _rexBuilder.makeSearchArgumentLiteral(Sarg.of(RexUnknownAs.UNKNOWN, punctured),
        literalVarcharType());
    Filter puncturedRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, _rexBuilder.makeInputRef(agg, 0), puncturedSarg));
    assertNoPreFilterInserted(runRule(puncturedRoot));

    // 8. Negated NOT BETWEEN and SYMMETRIC BETWEEN
    Filter notBetweenRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.NOT_BETWEEN, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeLiteral("b"), _rexBuilder.makeLiteral("d")));
    assertNoPreFilterInserted(runRule(notBetweenRoot));

    Filter symBetweenRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.SYMMETRIC_BETWEEN, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeLiteral("b"), _rexBuilder.makeLiteral("d")));
    assertNoPreFilterInserted(runRule(symBetweenRoot));

    // 9. Wrapped transform in HAVING: LOWER(k) = 'a'
    Filter wrappedHavingRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            _rexBuilder.makeCall(SqlStdOperatorTable.LOWER, _rexBuilder.makeInputRef(agg, 0)),
            _rexBuilder.makeLiteral("a")));
    assertNoPreFilterInserted(runRule(wrappedHavingRoot));

    // 10. OR mixing sound predicate with aggregate call
    Filter mixedOrRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.OR,
            _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
                _rexBuilder.makeLiteral("a")),
            _rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, _rexBuilder.makeInputRef(agg, 1),
                _rexBuilder.makeExactLiteral(BigDecimal.ONE))));
    assertNoPreFilterInserted(runRule(mixedOrRoot));

    // 11. Comparison with NULL literal
    Filter nullLitRoot = buildTagsScanPlan(cluster,
        agg -> _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeNullLiteral(_varcharType)));
    assertNoPreFilterInserted(runRule(nullLitRoot));
  }

  @Test
  public void testWrappedGroupKeyColumnToColumnAndJoinAreNotCopied() {
    RelOptCluster cluster = newCluster();

    // 1. Wrapped group key: GROUP BY LOWER(ARRAY_TO_MV(tags))
    TableScan scan = createTableScan(cluster);
    Project wrappedProject = LogicalProject.create(scan, List.of(),
        List.of(_rexBuilder.makeCall(SqlStdOperatorTable.LOWER, arrayToMv(_rexBuilder.makeInputRef(scan, 1)))),
        List.of("k"), Set.of());
    Aggregate wrappedAgg = LogicalAggregate.create(wrappedProject, List.of(), ImmutableBitSet.of(0), null,
        List.of(countStar(wrappedProject)));
    Filter wrappedRoot = LogicalFilter.create(wrappedAgg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(wrappedAgg, 0),
            _rexBuilder.makeLiteral("a")));
    assertNoPreFilterInserted(runRule(wrappedRoot));

    // 2. Column-to-column comparison: ARRAY_TO_MV(tags) = sv
    Project twoColProject = LogicalProject.create(scan, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(scan, 1)), _rexBuilder.makeInputRef(scan, 0)),
        List.of("k", "sv"), Set.of());
    Aggregate twoColAgg = LogicalAggregate.create(twoColProject, List.of(), ImmutableBitSet.of(0, 1), null,
        List.of(countStar(twoColProject)));
    Filter colToColRoot = LogicalFilter.create(twoColAgg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(twoColAgg, 0),
            _rexBuilder.makeInputRef(twoColAgg, 1)));
    assertNoPreFilterInserted(runRule(colToColRoot));

    // 3. Join underneath Aggregate is not copied.
    TableScan left = createTableScan(cluster);
    TableScan right = createTableScan(cluster);
    RelNode join = LogicalJoin.create(left, right, List.of(),
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(left, 0),
            _rexBuilder.makeInputRef(right.getRowType().getFieldList().get(0).getType(), 3)),
        Set.of(), JoinRelType.INNER);
    Project joinProject = LogicalProject.create(join, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(join, 1))), List.of("k"), Set.of());
    Aggregate joinAgg = LogicalAggregate.create(joinProject, List.of(), ImmutableBitSet.of(0), null,
        List.of(countStar(joinProject)));
    Filter joinRoot = LogicalFilter.create(joinAgg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(joinAgg, 0),
            _rexBuilder.makeLiteral("vip")));
    RelNode joinResult = runRule(joinRoot);
    assertEquals(RelOptUtil.toString(joinResult), RelOptUtil.toString(joinRoot));

    // 4. GROUPING SETS is not copied.
    Project gsProject = LogicalProject.create(scan, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(scan, 1))), List.of("k"), Set.of());
    Aggregate gsAgg = LogicalAggregate.create(gsProject, List.of(), ImmutableBitSet.of(0),
        List.of(ImmutableBitSet.of(0), ImmutableBitSet.of()), List.of(countStar(gsProject)));
    Filter gsRoot = LogicalFilter.create(gsAgg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(gsAgg, 0),
            _rexBuilder.makeLiteral("vip")));
    assertNoPreFilterInserted(runRule(gsRoot));
  }

  @Test
  public void testFilterAboveProjectDoesNotCopyPreFilter() {
    // Aggregate -> Filter -> Project(ARRAY_TO_MV) -> TableScan must bail out rather than inserting a second
    // Filter below Project (which FilterMergeRule could not merge).
    RelOptCluster cluster = newCluster();
    TableScan scan = createTableScan(cluster);
    Project project = LogicalProject.create(scan, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(scan, 1)), _rexBuilder.makeInputRef(scan, 0)),
        List.of("k", "sv"), Set.of());
    Filter midFilter = LogicalFilter.create(project,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(project, 1),
            _rexBuilder.makeLiteral("US")));
    Aggregate agg = LogicalAggregate.create(midFilter, List.of(), ImmutableBitSet.of(0), null,
        List.of(countStar(midFilter)));
    Filter topFilter = LogicalFilter.create(agg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeLiteral("vip")));

    RelNode result = runRule(topFilter);
    assertEquals(RelOptUtil.toString(result), RelOptUtil.toString(topFilter));
  }

  @Test
  public void testAlwaysFalseLeafFilterIsNoOp() {
    RelOptCluster cluster = newCluster();
    TableScan scan = createTableScan(cluster);
    Filter falseLeaf = LogicalFilter.create(scan, _rexBuilder.makeLiteral(false));
    Project project = LogicalProject.create(falseLeaf, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(falseLeaf, 1))), List.of("k"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0), null,
        List.of(countStar(project)));
    Filter topFilter = LogicalFilter.create(agg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeLiteral("vip")));

    RelNode result = runRule(topFilter);
    assertEquals(RelOptUtil.toString(result), RelOptUtil.toString(topFilter));
  }

  @Test
  public void testTwoMvKeysInAndCopiesBothInOrCopiesNeither() {
    RelOptCluster cluster = newCluster();
    TableScan scan = createTableScan(cluster);
    Project project = LogicalProject.create(scan, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(scan, 1)), arrayToMv(_rexBuilder.makeInputRef(scan, 2))),
        List.of("k1", "k2"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0, 1), null,
        List.of(countStar(project)));
    RexNode cond1 = _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
        _rexBuilder.makeLiteral("vip"));
    RexNode cond2 = _rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, _rexBuilder.makeInputRef(agg, 1),
        _rexBuilder.makeExactLiteral(BigDecimal.TEN));

    // AND across two MV keys copies both conjuncts into the leaf pre-filter.
    Filter andRoot = LogicalFilter.create(agg, _rexBuilder.makeCall(SqlStdOperatorTable.AND, cond1, cond2));
    RelNode andResult = runRule(andRoot);
    assertPreFilterInserted(andResult, "ARRAY_TO_MV($1)");
    Filter leafFilter = (Filter) ((Project) ((Aggregate) ((Filter) andResult).getInput()).getInput()).getInput();
    String leafCond = leafFilter.getCondition().toString();
    assertTrue(leafCond.contains("ARRAY_TO_MV($2)"), leafCond);
    assertTrue(leafCond.contains("10"), leafCond);

    // OR across two different MV keys copies neither.
    Filter orRoot = LogicalFilter.create(agg, _rexBuilder.makeCall(SqlStdOperatorTable.OR, cond1, cond2));
    assertNoPreFilterInserted(runRule(orRoot));
  }
}
