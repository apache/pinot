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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.calcite.rel.logical.PinotLogicalEnrichedJoin;
import org.apache.pinot.query.type.TypeFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class PinotEnrichedJoinRuleTest {
  private static final TypeFactory TYPE_FACTORY = TypeFactory.INSTANCE;
  private static final RexBuilder REX_BUILDER = RexBuilder.DEFAULT;

  private AutoCloseable _mocks;

  @Mock
  private RelOptRuleCall _call;
  @Mock
  private RelNode _input;
  @Mock
  private RelOptCluster _cluster;
  @Mock
  private RelMetadataQuery _query;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    RelTraitSet traits = RelTraitSet.createEmpty();
    when(_cluster.traitSetOf(Convention.NONE)).thenReturn(traits);
    when(_cluster.getHintStrategies()).thenReturn(HintStrategyTable.EMPTY);
    when(_cluster.getMetadataQuery()).thenReturn(RelMetadataQuery.instance());
    when(_cluster.traitSet()).thenReturn(traits);
    when(_cluster.getTypeFactory()).thenReturn(TYPE_FACTORY);
    when(_cluster.getRexBuilder()).thenReturn(REX_BUILDER);

    when(_input.getTraitSet()).thenReturn(traits);
    when(_input.getCluster()).thenReturn(_cluster);
    when(_call.getMetadataQuery()).thenReturn(_query);
    when(_query.getMaxRowCount(Mockito.any())).thenReturn(null);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  // TODO: test
  @Test
  public void testOnMatchFilterJoinCase() {
    // Create a RelOptRuleCall for a filter above join
    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();
    when(_input.getRowType()).thenReturn(intType);
    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(
        SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(),
            joinCondition, Collections.emptySet(), JoinRelType.INNER);
    // filter condition col2 = 1
    RexNode filterCondition =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 2),
            REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    LogicalFilter originalFilter = LogicalFilter.create(originalJoin, filterCondition);
    Mockito.when(_call.rel(0)).thenReturn(originalFilter);
    Mockito.when(_call.rel(1)).thenReturn(originalJoin);
    PinotEnrichedJoinRule rule = PinotEnrichedJoinRule.Config.FILTER_JOIN.toRule();
    Mockito.when(_call.getRule()).thenReturn(rule);
    rule.onMatch(_call);

    ArgumentCaptor<RelNode> logicalEnrichedNodeCapture = ArgumentCaptor.forClass(PinotLogicalEnrichedJoin.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(logicalEnrichedNodeCapture.capture());
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) logicalEnrichedNodeCapture.getValue();
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getFilter(), filterCondition);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
  }

  @Test
  public void testOnMatchProjectJoinCase() {
    // Create a RelOptRuleCall for a filter above join
    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();
    when(_input.getRowType()).thenReturn(intType);
    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(), joinCondition, Collections.emptySet(),
            JoinRelType.INNER);
    // project one column
    List<RexNode> projects = List.of(REX_BUILDER.makeInputRef(intType, 1));
    LogicalProject project =
        LogicalProject.create(originalJoin, Collections.emptyList(), projects, List.of("projectCol1"));
    Mockito.when(_call.rel(0)).thenReturn(project);
    Mockito.when(_call.rel(1)).thenReturn(originalJoin);
    PinotEnrichedJoinRule rule = PinotEnrichedJoinRule.Config.PROJECT_JOIN.toRule();
    Mockito.when(_call.getRule()).thenReturn(rule);
    rule.onMatch(_call);

    ArgumentCaptor<RelNode> logicalEnrichedNodeCapture = ArgumentCaptor.forClass(PinotLogicalEnrichedJoin.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(logicalEnrichedNodeCapture.capture());
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) logicalEnrichedNodeCapture.getValue();
    assertEquals(enrichedJoin.getProjects(), projects);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
  }

  @Test
  public void testRuleWithPlannerFilterJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    when(_input.getCluster()).thenReturn(cluster);
    // Create a RelOptRuleCall for a filter above join
    when(_input.getRowType()).thenReturn(intType);
    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(), joinCondition, Collections.emptySet(),
            JoinRelType.INNER);
    // filter condition col2 = 1
    RexNode filterCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 2),
        REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    LogicalFilter originalFilter = LogicalFilter.create(originalJoin, filterCondition);

    planner.setRoot(originalFilter);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
//    assertEquals(enrichedJoin.getFilter(), filterCondition);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
  }

  @Test
  public void testRuleWithPlannerProjectJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    when(_input.getCluster()).thenReturn(cluster);
    // Create a RelOptRuleCall for a filter above join
    when(_input.getRowType()).thenReturn(intType);
    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(), joinCondition, Collections.emptySet(),
            JoinRelType.INNER);
    // project one column
    List<RexNode> projects = List.of(REX_BUILDER.makeInputRef(intType, 1));
    LogicalProject project =
        LogicalProject.create(originalJoin, Collections.emptyList(), projects, List.of("projectCol1"));

    planner.setRoot(project);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
    assertEquals(enrichedJoin.getProjects(), projects);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
  }

  @Test
  public void testRuleWithPlannerProjectFilterJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    when(_input.getCluster()).thenReturn(cluster);
    // Create a RelOptRuleCall for a filter above join
    when(_input.getRowType()).thenReturn(intType);
    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(), joinCondition, Collections.emptySet(),
            JoinRelType.INNER);
    // filter condition col2 = 1
    RexNode filterCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 2),
        REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    LogicalFilter originalFilter = LogicalFilter.create(originalJoin, filterCondition);
    // project above filter
    List<RexNode> projects = List.of(REX_BUILDER.makeInputRef(intType, 1));
    LogicalProject project =
        LogicalProject.create(originalFilter, Collections.emptyList(), projects, List.of("projectCol1"));

    planner.setRoot(project);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getFilter(), filterCondition);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(1).getProjectAndResultRowType().getProject(), projects);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
  }

  @Test
  public void testRuleWithPlannerFilterProjectJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    when(_input.getCluster()).thenReturn(cluster);
    // Create a RelOptRuleCall for a filter above join
    when(_input.getRowType()).thenReturn(intType);
    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(), joinCondition, Collections.emptySet(),
            JoinRelType.INNER);
    // project above filter with width of 5
    List<RexNode> projects = List.of(REX_BUILDER.makeInputRef(intType, 0), REX_BUILDER.makeInputRef(intType, 1),
        REX_BUILDER.makeInputRef(intType, 2), REX_BUILDER.makeInputRef(intType, 3),
        REX_BUILDER.makeInputRef(intType, 4), REX_BUILDER.makeInputRef(intType, 4),
        REX_BUILDER.makeInputRef(intType, 0));
    LogicalProject project =
        LogicalProject.create(originalJoin, Collections.emptyList(), projects, (List<? extends String>) null);
    // filter condition col2 = 1
    RexNode filterCondition =
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(project.getRowType(), 6),
            REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    LogicalFilter originalFilter = LogicalFilter.create(project, filterCondition);

    planner.setRoot(originalFilter);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getProjectAndResultRowType().getProject(), projects);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(1).getFilter(), filterCondition);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
  }

  @Test
  public void testRuleWithPlannerLimitFilterProjectJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    when(_input.getCluster()).thenReturn(cluster);
    // Create a RelOptRuleCall for a filter above join
    when(_input.getRowType()).thenReturn(intType);
    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(), joinCondition, Collections.emptySet(),
            JoinRelType.INNER);
    // project above filter with width of 5
    List<RexNode> projects = List.of(REX_BUILDER.makeInputRef(intType, 0), REX_BUILDER.makeInputRef(intType, 1),
        REX_BUILDER.makeInputRef(intType, 2), REX_BUILDER.makeInputRef(intType, 3),
        REX_BUILDER.makeInputRef(intType, 4), REX_BUILDER.makeInputRef(intType, 4),
        REX_BUILDER.makeInputRef(intType, 0));
    LogicalProject project =
        LogicalProject.create(originalJoin, Collections.emptyList(), projects, (List<? extends String>) null);
    // filter condition col2 = 1
    RexNode filterCondition =
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(project.getRowType(), 6),
            REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    LogicalFilter originalFilter = LogicalFilter.create(project, filterCondition);

    RelCollation collation = RelCollations.EMPTY;
    RexNode limit = literal(1);
    RexNode offset = literal(1);
    LogicalSort sort = LogicalSort.create(originalFilter, collation, offset, limit);

    planner.setRoot(sort);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getProjectAndResultRowType().getProject(), projects);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(1).getFilter(), filterCondition);
    assertEquals(enrichedJoin.getOffset(), offset);
    assertEquals(enrichedJoin.getFetch(), limit);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
  }

  @Test
  public void testRuleWithPlannerLimitProjectFilterJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    when(_input.getCluster()).thenReturn(cluster);
    // Create a RelOptRuleCall for a filter above join
    when(_input.getRowType()).thenReturn(intType);
    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(), joinCondition, Collections.emptySet(),
            JoinRelType.INNER);
    // filter condition col2 = 1
    RexNode filterCondition =
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(originalJoin.getRowType(), 0),
            REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    LogicalFilter originalFilter = LogicalFilter.create(originalJoin, filterCondition);
    // project above filter with width of 5
    List<RexNode> projects = List.of(REX_BUILDER.makeInputRef(intType, 0), REX_BUILDER.makeInputRef(intType, 1),
        REX_BUILDER.makeInputRef(intType, 2), REX_BUILDER.makeInputRef(intType, 3),
        REX_BUILDER.makeInputRef(intType, 4), REX_BUILDER.makeInputRef(intType, 4),
        REX_BUILDER.makeInputRef(intType, 0));
    LogicalProject project =
        LogicalProject.create(originalFilter, Collections.emptyList(), projects, (List<? extends String>) null);

    RelCollation collation = RelCollations.EMPTY;
    RexNode limit = literal(1);
    RexNode offset = literal(1);
    LogicalSort sort = LogicalSort.create(project, collation, offset, limit);

    planner.setRoot(sort);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getFilter(), filterCondition);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(1).getProjectAndResultRowType().getProject(), projects);
    assertEquals(enrichedJoin.getOffset(), offset);
    assertEquals(enrichedJoin.getFetch(), limit);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
  }

  @Test
  public void testRuleRejectSortWithCollationProjectFilterJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    when(_input.getCluster()).thenReturn(cluster);
    // Create a RelOptRuleCall for a filter above join
    when(_input.getRowType()).thenReturn(intType);
    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(), joinCondition, Collections.emptySet(),
            JoinRelType.INNER);
    // filter condition col2 = 1
    RexNode filterCondition =
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(originalJoin.getRowType(), 0),
            REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    LogicalFilter originalFilter = LogicalFilter.create(originalJoin, filterCondition);
    // project above filter with width of 5
    List<RexNode> projects = List.of(REX_BUILDER.makeInputRef(intType, 0), REX_BUILDER.makeInputRef(intType, 1),
        REX_BUILDER.makeInputRef(intType, 2), REX_BUILDER.makeInputRef(intType, 3),
        REX_BUILDER.makeInputRef(intType, 4), REX_BUILDER.makeInputRef(intType, 4),
        REX_BUILDER.makeInputRef(intType, 0));
    LogicalProject project =
        LogicalProject.create(originalFilter, Collections.emptyList(), projects, (List<? extends String>) null);

    RelCollation collation = RelCollations.of(0);
    RexNode limit = literal(1);
    RexNode offset = literal(1);
    LogicalSort sort = LogicalSort.create(project, collation, offset, limit);

    planner.setRoot(sort);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof LogicalSort);
    assert (newRoot.getInput(0) instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot.getInput(0);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getFilter(), filterCondition);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(1).getProjectAndResultRowType().getProject(), projects);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
    assertNull(enrichedJoin.getOffset());
    assertNull(enrichedJoin.getFetch());
  }

  private static RexNode literal(int i) {
    return REX_BUILDER.makeLiteral(i, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
  }
}
