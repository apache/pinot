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

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.rel.logical.PinotLogicalEnrichedJoin;
import org.apache.pinot.calcite.rel.rules.PinotEnrichedJoinRule;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RelToPlanNodeConverterTest {

  @Test
  public void testConvertToColumnDataTypeForObjectTypes() {
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.BOOLEAN, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.BOOLEAN);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.TINYINT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.SMALLINT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.INTEGER, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.BIGINT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.LONG);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.FLOAT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.FLOAT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.DOUBLE, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.DOUBLE);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.TIMESTAMP, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.TIMESTAMP);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.CHAR, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.VARCHAR, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.VARBINARY, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.BYTES);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.OTHER, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.OBJECT);
  }

  @Test
  public void testBigDecimal() {
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 10)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 38)),
        DataSchema.ColumnDataType.LONG);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 39)),
        DataSchema.ColumnDataType.BIG_DECIMAL);

    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 14, 10)),
        DataSchema.ColumnDataType.DOUBLE);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 30, 10)),
        DataSchema.ColumnDataType.DOUBLE);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 31, 10)),
        DataSchema.ColumnDataType.BIG_DECIMAL);
  }

  @Test
  public void testConvertToColumnDataTypeForArray() {
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.BOOLEAN, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.BOOLEAN_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.TINYINT, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.INT_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.SMALLINT, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.INT_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.INTEGER, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.INT_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.BIGINT, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.LONG_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.FLOAT, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.FLOAT_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.DOUBLE, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.DOUBLE_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.TIMESTAMP, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.TIMESTAMP_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.CHAR, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.STRING_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.VARCHAR, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.STRING_ARRAY);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.VARBINARY, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.BYTES_ARRAY);
  }

  @Test
  public void testConvertEnrichedJoinNodeTest() {
    final TypeFactory typeFactory = TypeFactory.INSTANCE;
    final RexBuilder rexBuilder = RexBuilder.DEFAULT;
    RelTraitSet traits = RelTraitSet.createEmpty();

    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleCollection(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

    RelDataType intType = typeFactory.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    LogicalValues input = new LogicalValues(cluster, traits, intType, ImmutableList.of());

    // join condition col0 = col1
    RexNode joinCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 3));
    LogicalJoin originalJoin = LogicalJoin.create(input, input, Collections.emptyList(),
        joinCondition, Collections.emptySet(), JoinRelType.INNER);

    // filter condition col2 = 1
    RexNode filterCondition = rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(intType, 2),
        rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER)));
    LogicalFilter originalFilter = LogicalFilter.create(originalJoin, filterCondition);

    // project above filter
    List<RexNode> projects = List.of(rexBuilder.makeInputRef(intType, 1));
    LogicalProject project = LogicalProject.create(
        originalFilter, Collections.emptyList(), projects, List.of("projectCol1"));

    planner.setRoot(project);
    PinotLogicalEnrichedJoin enrichedJoin = (PinotLogicalEnrichedJoin) planner.findBestExp();

    RelToPlanNodeConverter relToPlanNodeConverter = new RelToPlanNodeConverter(null,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);

    PlanNode node = relToPlanNodeConverter.toPlanNode(enrichedJoin);
    assert (node instanceof EnrichedJoinNode);

    EnrichedJoinNode enrichedJoinNode = (EnrichedJoinNode) node;
    Assert.assertEquals(enrichedJoinNode.getFilterProjectRexes().size(), 2);
  }

  @Test
  public void testConvertLogicalCorrelateProducesUnnestMetadata() {
    TypeFactory typeFactory = TypeFactory.INSTANCE;
    RelOptCluster cluster = createCluster(typeFactory);
    RelDataType leftRowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("arr", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1))
        .build();
    LogicalValues left = LogicalValues.create(cluster, leftRowType, ImmutableList.of());

    CorrelationId correlationId = new CorrelationId(0);
    LogicalProject project = buildCorrelatedProject(cluster, leftRowType, correlationId, "arr");
    Uncollect uncollect = Uncollect.create(project.getTraitSet(), project, false, List.of());
    LogicalCorrelate correlate =
        LogicalCorrelate.create(left, uncollect, correlationId, ImmutableBitSet.of(1), JoinRelType.INNER);

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);
    PlanNode planNode = converter.toPlanNode(correlate);

    Assert.assertTrue(planNode instanceof UnnestNode);
    UnnestNode unnestNode = (UnnestNode) planNode;
    Assert.assertEquals(((RexExpression.InputRef) unnestNode.getArrayExpr()).getIndex(), 1);
    Assert.assertEquals(unnestNode.getColumnAlias(), "arr");
    Assert.assertEquals(unnestNode.getElementIndex(), 2);
    Assert.assertFalse(unnestNode.isWithOrdinality());
  }

  @Test
  public void testConvertLogicalCorrelateWithFilterAndOrdinality() {
    TypeFactory typeFactory = TypeFactory.INSTANCE;
    RelOptCluster cluster = createCluster(typeFactory);
    RelDataType leftRowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("arr", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1))
        .build();
    LogicalValues left = LogicalValues.create(cluster, leftRowType, ImmutableList.of());

    CorrelationId correlationId = new CorrelationId(1);
    LogicalProject project = buildCorrelatedProject(cluster, leftRowType, correlationId, "arr");
    Uncollect uncollect = Uncollect.create(project.getTraitSet(), project, true, List.of());
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode ordRef = rexBuilder.makeInputRef(uncollect.getRowType(), 1);
    RexNode literal = rexBuilder.makeExactLiteral(BigDecimal.ONE, typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ordRef, literal);
    LogicalFilter filter = LogicalFilter.create(uncollect, condition);
    LogicalCorrelate correlate =
        LogicalCorrelate.create(left, filter, correlationId, ImmutableBitSet.of(1), JoinRelType.LEFT);

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);
    PlanNode planNode = converter.toPlanNode(correlate);

    Assert.assertTrue(planNode instanceof FilterNode);
    FilterNode filterNode = (FilterNode) planNode;
    Assert.assertEquals(filterNode.getInputs().size(), 1);
    Assert.assertTrue(filterNode.getInputs().get(0) instanceof UnnestNode);
    UnnestNode child = (UnnestNode) filterNode.getInputs().get(0);
    Assert.assertTrue(child.isWithOrdinality());
    Assert.assertEquals(child.getElementIndex(), 2);
    Assert.assertEquals(child.getOrdinalityIndex(), 3);

    RexExpression.FunctionCall conditionExpr = (RexExpression.FunctionCall) filterNode.getCondition();
    Assert.assertEquals(conditionExpr.getFunctionName(), SqlStdOperatorTable.GREATER_THAN.getKind().toString());
    RexExpression.InputRef rewrittenOrdinal =
        (RexExpression.InputRef) conditionExpr.getFunctionOperands().get(0);
    Assert.assertEquals(rewrittenOrdinal.getIndex(), child.getOrdinalityIndex());
  }

  private static RelOptCluster createCluster(TypeFactory typeFactory) {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
    return cluster;
  }

  @Test
  public void testConvertLogicalUncollectWithOrdinalityAndAliases() {
    TypeFactory typeFactory = TypeFactory.INSTANCE;
    RelOptCluster cluster = createCluster(typeFactory);
    RelDataType inputRowType = typeFactory.builder()
        .add("stringArrayCol", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
        .build();
    LogicalValues input = LogicalValues.create(cluster, inputRowType, ImmutableList.of());
    // Create Uncollect with WITH ORDINALITY and aliases: AS w(s, ord)
    Uncollect uncollect = Uncollect.create(input.getTraitSet(), input, true, List.of("s"));

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);
    PlanNode planNode = converter.toPlanNode(uncollect);

    Assert.assertTrue(planNode instanceof UnnestNode);
    UnnestNode unnestNode = (UnnestNode) planNode;
    // Check multiple arrays support
    Assert.assertEquals(unnestNode.getArrayExprs().size(), 1);
    Assert.assertEquals(((RexExpression.InputRef) unnestNode.getArrayExprs().get(0)).getIndex(), 0);
    // Check aliases - Calcite might generate different aliases, so we check that they're not null
    Assert.assertEquals(unnestNode.getColumnAliases().size(), 1);
    Assert.assertNotNull(unnestNode.getColumnAliases().get(0));
    // Check WITH ORDINALITY
    Assert.assertTrue(unnestNode.isWithOrdinality());
    Assert.assertNotNull(unnestNode.getOrdinalityAlias());
  }

  @Test
  public void testConvertLogicalUncollectMultipleArrays() {
    // Test direct UNNEST of multiple arrays without ordinality.
    TypeFactory typeFactory = TypeFactory.INSTANCE;
    RelOptCluster cluster = createCluster(typeFactory);
    RelDataType inputRowType = typeFactory.builder()
        .add("longArrayCol", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), -1))
        .add("stringArrayCol", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
        .build();
    LogicalValues input = LogicalValues.create(cluster, inputRowType, ImmutableList.of());
    RelDataType longArrayType = inputRowType.getFieldList().get(0).getType();
    RelDataType stringArrayType = inputRowType.getFieldList().get(1).getType();
    RexNode longArrayRef = new RexInputRef(0, longArrayType);
    RexNode stringArrayRef = new RexInputRef(1, stringArrayType);
    LogicalProject project = LogicalProject.create(input, Collections.emptyList(),
        List.of(longArrayRef, stringArrayRef), List.of("longArrayCol", "stringArrayCol"));
    // Create Uncollect without ordinality
    Uncollect uncollect = Uncollect.create(project.getTraitSet(), project, false, List.of("longValue", "stringValue"));

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);
    PlanNode planNode = converter.toPlanNode(uncollect);

    Assert.assertTrue(planNode instanceof UnnestNode);
    UnnestNode unnestNode = (UnnestNode) planNode;
    // Check multiple arrays
    Assert.assertEquals(unnestNode.getArrayExprs().size(), 2);
    Assert.assertEquals(((RexExpression.InputRef) unnestNode.getArrayExprs().get(0)).getIndex(), 0);
    Assert.assertEquals(((RexExpression.InputRef) unnestNode.getArrayExprs().get(1)).getIndex(), 1);
    // Check aliases
    Assert.assertEquals(unnestNode.getColumnAliases().size(), 2);
    Assert.assertEquals(unnestNode.getColumnAliases().get(0), "longValue");
    Assert.assertEquals(unnestNode.getColumnAliases().get(1), "stringValue");
    // Check no ordinality
    Assert.assertFalse(unnestNode.isWithOrdinality());
  }

  @Test
  public void testConvertLogicalUncollectMultipleArraysWithOrdinality() {
    TypeFactory typeFactory = TypeFactory.INSTANCE;
    RelOptCluster cluster = createCluster(typeFactory);
    RelDataType inputRowType = typeFactory.builder()
        .add("longArrayCol", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), -1))
        .add("stringArrayCol", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
        .build();
    LogicalValues input = LogicalValues.create(cluster, inputRowType, ImmutableList.of());
    RelDataType longArrayType = inputRowType.getFieldList().get(0).getType();
    RelDataType stringArrayType = inputRowType.getFieldList().get(1).getType();
    RexNode longArrayRef = new RexInputRef(0, longArrayType);
    RexNode stringArrayRef = new RexInputRef(1, stringArrayType);
    LogicalProject project = LogicalProject.create(input, Collections.emptyList(),
        List.of(longArrayRef, stringArrayRef), List.of("longArrayCol", "stringArrayCol"));
    // Create Uncollect with WITH ORDINALITY
    Uncollect uncollect = Uncollect.create(project.getTraitSet(), project, true,
        List.of("longVal", "strVal"));

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);
    PlanNode planNode = converter.toPlanNode(uncollect);

    Assert.assertTrue(planNode instanceof UnnestNode);
    UnnestNode unnestNode = (UnnestNode) planNode;
    // Check multiple arrays
    Assert.assertEquals(unnestNode.getArrayExprs().size(), 2);
    Assert.assertEquals(((RexExpression.InputRef) unnestNode.getArrayExprs().get(0)).getIndex(), 0);
    Assert.assertEquals(((RexExpression.InputRef) unnestNode.getArrayExprs().get(1)).getIndex(), 1);
    // Check aliases
    Assert.assertEquals(unnestNode.getColumnAliases().size(), 2);
    Assert.assertEquals(unnestNode.getColumnAliases().get(0), "longVal");
    Assert.assertEquals(unnestNode.getColumnAliases().get(1), "strVal");
    // Check WITH ORDINALITY
    Assert.assertTrue(unnestNode.isWithOrdinality());
    Assert.assertEquals(unnestNode.getOrdinalityAlias(), SqlUnnestOperator.ORDINALITY_COLUMN_NAME);
  }

  @Test
  public void testConvertLogicalCorrelateMultipleArrays() {
    TypeFactory typeFactory = TypeFactory.INSTANCE;
    RelOptCluster cluster = createCluster(typeFactory);
    RelDataType leftRowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("longArrayCol", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), -1))
        .add("stringArrayCol", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
        .build();
    LogicalValues left = LogicalValues.create(cluster, leftRowType, ImmutableList.of());

    CorrelationId correlationId = new CorrelationId(0);
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode longArrayAccess =
        rexBuilder.makeFieldAccess(rexBuilder.makeCorrel(leftRowType, correlationId), "longArrayCol", true);
    RexNode stringArrayAccess =
        rexBuilder.makeFieldAccess(rexBuilder.makeCorrel(leftRowType, correlationId), "stringArrayCol", true);
    LogicalProject project = LogicalProject.create(LogicalValues.createOneRow(cluster), Collections.emptyList(),
        List.of(longArrayAccess, stringArrayAccess), List.of("longArrayCol", "stringArrayCol"));
    Uncollect uncollect = Uncollect.create(project.getTraitSet(), project, false,
        List.of("longValue", "stringValue"));
    LogicalCorrelate correlate =
        LogicalCorrelate.create(left, uncollect, correlationId, ImmutableBitSet.of(1, 2), JoinRelType.INNER);

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);
    PlanNode planNode = converter.toPlanNode(correlate);

    Assert.assertTrue(planNode instanceof UnnestNode);
    UnnestNode unnestNode = (UnnestNode) planNode;
    // Check multiple arrays
    Assert.assertEquals(unnestNode.getArrayExprs().size(), 2);
    Assert.assertEquals(((RexExpression.InputRef) unnestNode.getArrayExprs().get(0)).getIndex(), 1);
    Assert.assertEquals(((RexExpression.InputRef) unnestNode.getArrayExprs().get(1)).getIndex(), 2);
    // Check aliases
    Assert.assertEquals(unnestNode.getColumnAliases().size(), 2);
    Assert.assertEquals(unnestNode.getColumnAliases().get(0), "longValue");
    Assert.assertEquals(unnestNode.getColumnAliases().get(1), "stringValue");
    // Check element indexes
    Assert.assertEquals(unnestNode.getElementIndexes().size(), 2);
    Assert.assertEquals(unnestNode.getElementIndexes().get(0).intValue(),
        3); // base (left columns) = 3 (id, longArrayCol, stringArrayCol)
    Assert.assertEquals(unnestNode.getElementIndexes().get(1).intValue(), 4);
    Assert.assertFalse(unnestNode.isWithOrdinality());
  }

  @Test
  public void testConvertLogicalCorrelateMultipleArraysWithOrdinality() {
    TypeFactory typeFactory = TypeFactory.INSTANCE;
    RelOptCluster cluster = createCluster(typeFactory);
    RelDataType leftRowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("longArrayCol", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), -1))
        .add("stringArrayCol", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
        .build();
    LogicalValues left = LogicalValues.create(cluster, leftRowType, ImmutableList.of());

    CorrelationId correlationId = new CorrelationId(0);
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode longArrayAccess =
        rexBuilder.makeFieldAccess(rexBuilder.makeCorrel(leftRowType, correlationId), "longArrayCol", true);
    RexNode stringArrayAccess =
        rexBuilder.makeFieldAccess(rexBuilder.makeCorrel(leftRowType, correlationId), "stringArrayCol", true);
    LogicalProject project = LogicalProject.create(LogicalValues.createOneRow(cluster), Collections.emptyList(),
        List.of(longArrayAccess, stringArrayAccess), List.of("longArrayCol", "stringArrayCol"));
    Uncollect uncollect = Uncollect.create(project.getTraitSet(), project, true,
        List.of("longValue", "stringValue"));
    LogicalCorrelate correlate =
        LogicalCorrelate.create(left, uncollect, correlationId, ImmutableBitSet.of(1, 2), JoinRelType.INNER);

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);
    PlanNode planNode = converter.toPlanNode(correlate);

    Assert.assertTrue(planNode instanceof UnnestNode);
    UnnestNode unnestNode = (UnnestNode) planNode;
    // Check multiple arrays
    Assert.assertEquals(unnestNode.getArrayExprs().size(), 2);
    Assert.assertEquals(((RexExpression.InputRef) unnestNode.getArrayExprs().get(0)).getIndex(), 1);
    Assert.assertEquals(((RexExpression.InputRef) unnestNode.getArrayExprs().get(1)).getIndex(), 2);
    // Check aliases
    Assert.assertEquals(unnestNode.getColumnAliases().size(), 2);
    Assert.assertEquals(unnestNode.getColumnAliases().get(0), "longValue");
    Assert.assertEquals(unnestNode.getColumnAliases().get(1), "stringValue");
    // Check WITH ORDINALITY
    Assert.assertTrue(unnestNode.isWithOrdinality());
    Assert.assertEquals(unnestNode.getOrdinalityAlias(), SqlUnnestOperator.ORDINALITY_COLUMN_NAME);
    // Check element indexes
    Assert.assertEquals(unnestNode.getElementIndexes().size(), 2);
    Assert.assertEquals(unnestNode.getElementIndexes().get(0).intValue(), 3); // base (left columns) = 3
    Assert.assertEquals(unnestNode.getElementIndexes().get(1).intValue(), 4);
    Assert.assertEquals(unnestNode.getOrdinalityIndex(), 5);
  }

  private static LogicalProject buildCorrelatedProject(RelOptCluster cluster, RelDataType leftRowType,
      CorrelationId correlationId, String fieldName) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode fieldAccess =
        rexBuilder.makeFieldAccess(rexBuilder.makeCorrel(leftRowType, correlationId), fieldName, true);
    return LogicalProject.create(LogicalValues.createOneRow(cluster), Collections.emptyList(),
        List.of(fieldAccess), List.of(fieldName));
  }
}
