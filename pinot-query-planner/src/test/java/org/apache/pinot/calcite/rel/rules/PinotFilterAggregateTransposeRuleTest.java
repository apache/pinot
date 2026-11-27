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

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;
import org.apache.pinot.spi.utils.CommonConstants.Broker.PlannerRuleNames;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Unit tests for [PinotFilterAggregateTransposeRule].
public class PinotFilterAggregateTransposeRuleTest {
  private final RelDataTypeFactory _typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder _rexBuilder = new RexBuilder(_typeFactory);
  private final RelDataType _varcharType = _typeFactory.createSqlType(SqlTypeName.VARCHAR);
  private final RelDataType _intType = _typeFactory.createSqlType(SqlTypeName.INTEGER);
  private final RelDataType _bigintType = _typeFactory.createSqlType(SqlTypeName.BIGINT);
  private final RelDataType _varcharArrayType = _typeFactory.createArrayType(_varcharType, -1);
  private final RelDataType _intArrayType = _typeFactory.createArrayType(_intType, -1);

  private RelNode runRule(RelNode root) {
    HepProgram program = HepProgram.builder()
        .addRuleInstance(PinotFilterAggregateTransposeRule.instanceWithDescription(
            PlannerRuleNames.FILTER_AGGREGATE_TRANSPOSE))
        .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(root);
    return planner.findBestExp();
  }

  private RelOptCluster newCluster() {
    return RelOptCluster.create(new HepPlanner(HepProgram.builder().build()), _rexBuilder);
  }

  private RelNode createInput(RelOptCluster cluster) {
    // (sv VARCHAR, tags VARCHAR ARRAY, nums INT ARRAY, metric INT)
    RelDataType rowType = _typeFactory.builder()
        .add("sv", _varcharType)
        .add("tags", _varcharArrayType)
        .add("nums", _intArrayType)
        .add("metric", _intType)
        .build();
    return LogicalValues.createEmpty(cluster, rowType);
  }

  private RexNode arrayToMv(RexNode operand) {
    return _rexBuilder.makeCall(PinotOperatorTable.ARRAY_TO_MV, operand);
  }

  private AggregateCall countStar(RelNode input) {
    return AggregateCall.create(SqlParserPos.ZERO, SqlStdOperatorTable.COUNT, false, false, false, List.of(),
        List.of(), -1, null, RelCollations.EMPTY, false, input, _bigintType, "cnt");
  }

  @Test
  public void testNestedLowerArrayToMvKeyStaysAboveAggregate() {
    RelOptCluster cluster = newCluster();
    RelNode input = createInput(cluster);
    RexNode lowerMv = _rexBuilder.makeCall(SqlStdOperatorTable.LOWER, arrayToMv(_rexBuilder.makeInputRef(input, 1)));
    Project project = LogicalProject.create(input, List.of(), List.of(lowerMv), List.of("k"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0), null,
        List.of(countStar(project)));
    Filter filter = LogicalFilter.create(agg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeLiteral("vip")));

    RelNode result = runRule(filter);
    // Filter must stay above Aggregate and not be pushed below.
    assertTrue(result instanceof Filter, "Expected Filter on top, got: " + result);
    RelNode aggNode = ((Filter) result).getInput();
    assertTrue(aggNode instanceof Aggregate, "Expected Aggregate under Filter, got: " + aggNode);
    assertTrue(((Aggregate) aggNode).getInput() instanceof Project,
        "Expected no Filter pushed under Aggregate, got: " + ((Aggregate) aggNode).getInput());
  }

  @Test
  public void testNestedCastArrayToMvKeyStaysAboveAggregate() {
    RelOptCluster cluster = newCluster();
    RelNode input = createInput(cluster);
    RexNode castMv = _rexBuilder.makeCast(_varcharType, arrayToMv(_rexBuilder.makeInputRef(input, 2)));
    Project project = LogicalProject.create(input, List.of(), List.of(castMv), List.of("k"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0), null,
        List.of(countStar(project)));
    Filter filter = LogicalFilter.create(agg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeLiteral("1")));

    RelNode result = runRule(filter);
    assertTrue(result instanceof Filter);
    assertTrue(((Filter) result).getInput() instanceof Aggregate);
    assertTrue(((Aggregate) ((Filter) result).getInput()).getInput() instanceof Project);
  }

  @Test
  public void testMixedMvAndSvConjunctsSplitCorrectly() {
    RelOptCluster cluster = newCluster();
    RelNode input = createInput(cluster);
    // Project: ($0 = ARRAY_TO_MV(tags), $1 = sv)
    Project project = LogicalProject.create(input, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(input, 1)), _rexBuilder.makeInputRef(input, 0)),
        List.of("mvKey", "svKey"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0, 1), null,
        List.of(countStar(project)));
    // HAVING mvKey = 'vip' AND svKey = 'US' AND cnt > 5
    RexNode mvCond = _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
        _rexBuilder.makeLiteral("vip"));
    RexNode svCond = _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 1),
        _rexBuilder.makeLiteral("US"));
    RexNode aggCond = _rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, _rexBuilder.makeInputRef(agg, 2),
        _rexBuilder.makeExactLiteral(BigDecimal.valueOf(5)));
    Filter filter = LogicalFilter.create(agg,
        _rexBuilder.makeCall(SqlStdOperatorTable.AND, mvCond, svCond, aggCond));

    RelNode result = runRule(filter);
    // Top Filter retains mvKey = 'vip' AND cnt > 5; svKey = 'US' is pushed below Aggregate.
    assertTrue(result instanceof Filter);
    Filter topFilter = (Filter) result;
    String topCondStr = topFilter.getCondition().toString();
    assertTrue(topCondStr.contains("vip"), topCondStr);
    assertTrue(topCondStr.contains("5"), topCondStr);
    assertFalse(topCondStr.contains("US"), topCondStr);

    assertTrue(topFilter.getInput() instanceof Aggregate);
    Aggregate newAgg = (Aggregate) topFilter.getInput();
    assertTrue(newAgg.getInput() instanceof Filter, "Expected pushed Filter under Aggregate");
    Filter pushedFilter = (Filter) newAgg.getInput();
    String pushedCondStr = pushedFilter.getCondition().toString();
    assertTrue(pushedCondStr.contains("US"), pushedCondStr);
    assertFalse(pushedCondStr.contains("vip"), pushedCondStr);
  }

  @Test
  public void testNonZeroBasedGroupSetSplitsAndRemapsCorrectly() {
    RelOptCluster cluster = newCluster();
    RelNode input = createInput(cluster);
    // Project: ($0 = metric, $1 = ARRAY_TO_MV(tags), $2 = sv)
    // Aggregate groups by {1, 2}, so output $0 -> input $1 (MV), output $1 -> input $2 (SV).
    Project project = LogicalProject.create(input, List.of(),
        List.of(_rexBuilder.makeInputRef(input, 3), arrayToMv(_rexBuilder.makeInputRef(input, 1)),
            _rexBuilder.makeInputRef(input, 0)),
        List.of("m", "mvKey", "svKey"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(1, 2), null,
        List.of(countStar(project)));
    RexNode mvCond = _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
        _rexBuilder.makeLiteral("vip"));
    RexNode svCond = _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 1),
        _rexBuilder.makeLiteral("US"));
    Filter filter = LogicalFilter.create(agg, _rexBuilder.makeCall(SqlStdOperatorTable.AND, mvCond, svCond));

    RelNode result = runRule(filter);
    assertTrue(result instanceof Filter);
    Filter topFilter = (Filter) result;
    assertEquals(topFilter.getCondition(), mvCond);

    Aggregate newAgg = (Aggregate) topFilter.getInput();
    assertTrue(newAgg.getInput() instanceof Filter);
    Filter pushedFilter = (Filter) newAgg.getInput();
    // Output col 1 must be remapped to input col 2 (svKey), not col 1!
    assertEquals(pushedFilter.getCondition(),
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(project, 2),
            _rexBuilder.makeLiteral("US")));
  }

  @Test
  public void testPureSvFastPathPushesFilterBelowAggregate() {
    RelOptCluster cluster = newCluster();
    RelNode input = createInput(cluster);
    Project project = LogicalProject.create(input, List.of(),
        List.of(_rexBuilder.makeInputRef(input, 0)), List.of("svKey"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0), null,
        List.of(countStar(project)));
    Filter filter = LogicalFilter.create(agg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeLiteral("US")));

    RelNode result = runRule(filter);
    // Entire filter pushed below Aggregate; root is now Aggregate.
    assertTrue(result instanceof Aggregate, "Expected Aggregate on top, got: " + result);
    assertTrue(((Aggregate) result).getInput() instanceof Filter);
  }

  @Test
  public void testGroupingSetsDoesNotPushKeyAbsentFromAnyGroupingSet() {
    RelOptCluster cluster = newCluster();
    RelNode input = createInput(cluster);
    // Project: ($0 = metric, $1 = ARRAY_TO_MV(tags), $2 = sv)
    // GROUPING SETS (({1, 2}), ({1})): input col 2 (sv) is absent from grouping set {1}, so sv = 'US' cannot be pushed!
    Project project = LogicalProject.create(input, List.of(),
        List.of(_rexBuilder.makeInputRef(input, 3), arrayToMv(_rexBuilder.makeInputRef(input, 1)),
            _rexBuilder.makeInputRef(input, 0)),
        List.of("m", "mvKey", "svKey"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(1, 2),
        List.of(ImmutableBitSet.of(1, 2), ImmutableBitSet.of(1)), List.of(countStar(project)));
    RexNode mvCond = _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
        _rexBuilder.makeLiteral("vip"));
    RexNode svCond = _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 1),
        _rexBuilder.makeLiteral("US"));
    Filter filter = LogicalFilter.create(agg, _rexBuilder.makeCall(SqlStdOperatorTable.AND, mvCond, svCond));

    RelNode result = runRule(filter);
    // Neither conjunct can be pushed.
    assertTrue(result instanceof Filter);
    assertTrue(((Aggregate) ((Filter) result).getInput()).getInput() instanceof Project);
  }

  @Test
  public void testMvKeyPresentWithPureSvHavingPushesEntireFilter() {
    RelOptCluster cluster = newCluster();
    RelNode input = createInput(cluster);
    Project project = LogicalProject.create(input, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(input, 1)), _rexBuilder.makeInputRef(input, 0)),
        List.of("mvKey", "svKey"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0, 1), null,
        List.of(countStar(project)));
    // Filter references only the SV key even though an MV key is in GROUP BY.
    Filter filter = LogicalFilter.create(agg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 1),
            _rexBuilder.makeLiteral("US")));

    RelNode result = runRule(filter);
    // All conjuncts pushed below Aggregate; no Filter remains above Aggregate.
    assertTrue(result instanceof Aggregate, "Expected Aggregate on top, got: " + result);
    RelNode pushed = ((Aggregate) result).getInput();
    assertTrue(pushed instanceof Filter, "Expected pushed Filter under Aggregate, got: " + pushed);
    assertTrue(((Filter) pushed).getCondition().toString().contains("US"));
  }

  @Test
  public void testCrossColumnMvAndSvConjunctStaysAboveAggregate() {
    RelOptCluster cluster = newCluster();
    RelNode input = createInput(cluster);
    Project project = LogicalProject.create(input, List.of(),
        List.of(arrayToMv(_rexBuilder.makeInputRef(input, 1)), _rexBuilder.makeInputRef(input, 0)),
        List.of("mvKey", "svKey"), Set.of());
    Aggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0, 1), null,
        List.of(countStar(project)));
    // mvKey = svKey references the MV key, so it must stay above Aggregate.
    Filter filter = LogicalFilter.create(agg,
        _rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, _rexBuilder.makeInputRef(agg, 0),
            _rexBuilder.makeInputRef(agg, 1)));

    RelNode result = runRule(filter);
    assertTrue(result instanceof Filter, "Expected Filter to remain above Aggregate, got: " + result);
    assertTrue(((Aggregate) ((Filter) result).getInput()).getInput() instanceof Project);
  }
}
