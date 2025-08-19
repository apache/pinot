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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.pinot.calcite.rel.logical.PinotLogicalEnrichedJoin;
import org.apache.pinot.query.type.TypeFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class PinotEnrichedJoinRuleTest {
  private static final TypeFactory TYPE_FACTORY = TypeFactory.INSTANCE;
  private static final RexBuilder REX_BUILDER = RexBuilder.DEFAULT;

  private final RelDataType _intType = TYPE_FACTORY.builder()
      .add("col1", SqlTypeName.INTEGER)
      .add("col2", SqlTypeName.INTEGER)
      .add("col3", SqlTypeName.INTEGER)
      .build();

  private LogicalValues _input;

  @BeforeMethod
  public void setUp() {
    RelTraitSet traits = RelTraitSet.createEmpty();

    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);

    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
    _input = new LogicalValues(cluster, traits, _intType, ImmutableList.of());
  }

  @Test
  public void testRuleWithPlannerFilterJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

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
    assertEquals(enrichedJoin.getCondition(), joinCondition);
  }

  @Test
  public void testRuleWithPlannerFilterEnrichedJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    RexNode prevFilterCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 1),
        REX_BUILDER.makeLiteral(2, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    PinotLogicalEnrichedJoin originalJoin =
        new PinotLogicalEnrichedJoin(cluster, cluster.traitSet(), List.of(), _input, _input, joinCondition,
            Collections.emptySet(),
            JoinRelType.INNER, List.of(new PinotLogicalEnrichedJoin.FilterProjectRexNode(prevFilterCondition)), null,
            null, null, null);
    // filter condition col2 = 1
    RexNode filterCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 2),
        REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    LogicalFilter originalFilter = LogicalFilter.create(originalJoin, filterCondition);

    planner.setRoot(originalFilter);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
    assertEquals(enrichedJoin.getFilterProjectRexNodes().size(), 2);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getFilter(), prevFilterCondition);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(1).getFilter(), filterCondition);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
    assertEquals(enrichedJoin.getRowType(), enrichedJoin.getJoinRowType());
  }

  @Test
  public void testRuleWithPlannerProjectJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

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
  public void testRuleWithPlannerProjectEnrichedJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    RexNode prevFilterCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 1),
        REX_BUILDER.makeLiteral(2, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));

    // old project
    List<RexNode> oldProjects = List.of(REX_BUILDER.makeInputRef(intType, 1), REX_BUILDER.makeInputRef(intType, 0));
    RelDataType oldRowType =
        RexUtil.createStructType(cluster.getTypeFactory(), oldProjects,
            List.of("projectCol2", "projectCol1"), SqlValidatorUtil.F_SUGGESTER);

    // one project and one filter
    PinotLogicalEnrichedJoin originalJoin =
        new PinotLogicalEnrichedJoin(cluster, cluster.traitSet(), List.of(), _input, _input, joinCondition,
            Collections.emptySet(),
            JoinRelType.INNER, List.of(new PinotLogicalEnrichedJoin.FilterProjectRexNode(oldProjects, oldRowType),
            new PinotLogicalEnrichedJoin.FilterProjectRexNode(prevFilterCondition)), oldRowType,
            null, null, null);

    // new project
    List<RexNode> projects = List.of(REX_BUILDER.makeInputRef(intType, 1));
    LogicalProject project =
        LogicalProject.create(originalJoin, Collections.emptyList(), projects, List.of("projectCol1"));
    RelDataType rowType =
        RexUtil.createStructType(cluster.getTypeFactory(), projects,
            List.of("projectCol1"), SqlValidatorUtil.F_SUGGESTER);

    planner.setRoot(project);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
    assertEquals(enrichedJoin.getFilterProjectRexNodes().size(), 3);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getProjectAndResultRowType().getProject(), oldProjects);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getProjectAndResultRowType().getDataType(), oldRowType);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(1).getFilter(), prevFilterCondition);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(2).getProjectAndResultRowType().getProject(), projects);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(2).getProjectAndResultRowType().getDataType(), rowType);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
    assertEquals(enrichedJoin.getRowType(), rowType);
  }

  @Test
  public void testRuleWithPlannerLimitOffsetJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(), joinCondition, Collections.emptySet(),
            JoinRelType.INNER);

    RelCollation collation = RelCollations.EMPTY;
    RexNode limit = literal(1);
    RexNode offset = literal(1);
    LogicalSort sort = LogicalSort.create(originalJoin, collation, offset, limit);

    planner.setRoot(sort);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
    assertEquals(enrichedJoin.getOffset(), offset);
    assertEquals(enrichedJoin.getFetch(), limit);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
  }

  @Test
  public void testRuleWithPlannerLimitOffsetEnrichedJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    // join condition col0 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));

    // old filter
    RexNode prevFilterCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 1),
        REX_BUILDER.makeLiteral(2, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));

    // old project
    List<RexNode> oldProjects = List.of(REX_BUILDER.makeInputRef(intType, 1), REX_BUILDER.makeInputRef(intType, 0));
    RelDataType oldRowType =
        RexUtil.createStructType(cluster.getTypeFactory(), oldProjects,
            List.of("projectCol2", "projectCol1"), SqlValidatorUtil.F_SUGGESTER);

    // one project and one filter
    PinotLogicalEnrichedJoin originalJoin =
        new PinotLogicalEnrichedJoin(cluster, cluster.traitSet(), List.of(), _input, _input, joinCondition,
            Collections.emptySet(),
            JoinRelType.INNER, List.of(new PinotLogicalEnrichedJoin.FilterProjectRexNode(oldProjects, oldRowType),
            new PinotLogicalEnrichedJoin.FilterProjectRexNode(prevFilterCondition)), oldRowType,
            null, null, null);

    RelCollation collation = RelCollations.EMPTY;
    RexNode limit = literal(1);
    RexNode offset = literal(1);
    LogicalSort sort = LogicalSort.create(originalJoin, collation, offset, limit);

    planner.setRoot(sort);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
    assertEquals(enrichedJoin.getFilterProjectRexNodes().size(), 2);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getProjectAndResultRowType().getProject(), oldProjects);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(0).getProjectAndResultRowType().getDataType(), oldRowType);
    assertEquals(enrichedJoin.getFilterProjectRexNodes().get(1).getFilter(), prevFilterCondition);
    assertEquals(enrichedJoin.getOffset(), offset);
    assertEquals(enrichedJoin.getFetch(), limit);
    assertEquals(enrichedJoin.getCondition(), joinCondition);
    assertEquals(enrichedJoin.getRowType(), oldRowType);
  }

  @Test
  public void testRuleWithPlannerProjectFilterJoinCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

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
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

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
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

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
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

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
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

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

  @Test
  public void testRuleWithPlannerProjectJoinAfterProjectPushdownCase() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    // add projection pushdown rule
    hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);
    // add enriched join rule
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    // join condition col1 = col1
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, REX_BUILDER.makeInputRef(intType, 0),
        REX_BUILDER.makeInputRef(intType, 3));
    LogicalJoin originalJoin =
        LogicalJoin.create(_input, _input, Collections.emptyList(), joinCondition, Collections.emptySet(),
            JoinRelType.INNER);
    // project that touches both left and right relations
    List<RexNode> projects = List.of(REX_BUILDER.makeCall(SqlStdOperatorTable.PLUS,
        REX_BUILDER.makeInputRef(intType, 1), REX_BUILDER.makeInputRef(intType, 4)));
    LogicalProject project =
        LogicalProject.create(originalJoin, Collections.emptyList(), projects, List.of("projectCol1"));

    planner.setRoot(project);
    RelNode newRoot = planner.findBestExp();

    assert (newRoot instanceof PinotLogicalEnrichedJoin);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) newRoot;
    // projection pushdown should trim both inputs to first two cols only
    assert (newRoot.getInput(0) instanceof LogicalProject);
    assert (newRoot.getInput(1) instanceof LogicalProject);
    LogicalProject leftChild = (LogicalProject) newRoot.getInput(0);
    LogicalProject rightChild = (LogicalProject) newRoot.getInput(1);
    assertEquals(leftChild.getProjects().size(), 2);
    assertEquals(rightChild.getProjects().size(), 2);
    // join output layout [t1.col1, t1.col2, t2.col1, t2.col2]
    List<RexNode> expectedProjects = List.of(REX_BUILDER.makeCall(SqlStdOperatorTable.PLUS,
        REX_BUILDER.makeInputRef(intType, 1), REX_BUILDER.makeInputRef(intType, 3)));
    assertEquals(enrichedJoin.getProjects(), expectedProjects);
    // join condition col1 = col1
    RexNode expectedJoinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(intType, 0), REX_BUILDER.makeInputRef(intType, 2));
    assertEquals(enrichedJoin.getCondition(), expectedJoinCondition);
  }

  private static RexNode literal(int i) {
    return REX_BUILDER.makeLiteral(i, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
  }
}
