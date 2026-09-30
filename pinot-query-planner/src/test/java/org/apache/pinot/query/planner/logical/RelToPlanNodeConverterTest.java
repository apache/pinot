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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.common.utils.DataSchema;
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
    // Unsigned integer types (Calcite 1.41+, CALCITE-1466): the representable ones map to the narrowest signed type
    // that holds their range (UTINYINT/USMALLINT -> INT, UINTEGER -> LONG); UBIGINT is rejected because no signed type
    // holds its full 0..2^64-1 range.
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.UTINYINT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.USMALLINT, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ObjectSqlType(SqlTypeName.UINTEGER, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.LONG);
    Assert.assertThrows(IllegalArgumentException.class, () -> RelToPlanNodeConverter.convertToColumnDataType(
        new ObjectSqlType(SqlTypeName.UBIGINT, SqlIdentifier.STAR, true, null, null)));
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
            new ObjectSqlType(SqlTypeName.UUID, SqlIdentifier.STAR, true, null, null)),
        DataSchema.ColumnDataType.UUID);
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
    // Unsigned integer types (Calcite 1.41+, CALCITE-1466) map to their signed-equivalent array types.
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.UINTEGER, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.LONG_ARRAY);
    Assert.assertThrows(IllegalArgumentException.class, () -> RelToPlanNodeConverter.convertToColumnDataType(
        new ArraySqlType(new ObjectSqlType(SqlTypeName.UBIGINT, SqlIdentifier.STAR, true, null, null), true)));
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
    Assert.assertEquals(RelToPlanNodeConverter.convertToColumnDataType(
            new ArraySqlType(new ObjectSqlType(SqlTypeName.UUID, SqlIdentifier.STAR, true, null, null), true)),
        DataSchema.ColumnDataType.UUID_ARRAY);
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
    // Check WITH ORDINALITY
    Assert.assertTrue(unnestNode.isWithOrdinality());
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
    LogicalProject project = LogicalProject.create(input, List.of(),
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
    LogicalProject project = LogicalProject.create(input, List.of(),
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
    // Check WITH ORDINALITY
    Assert.assertTrue(unnestNode.isWithOrdinality());
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
    LogicalProject project = LogicalProject.create(LogicalValues.createOneRow(cluster), List.of(),
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
    LogicalProject project = LogicalProject.create(LogicalValues.createOneRow(cluster), List.of(),
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
    // Check WITH ORDINALITY
    Assert.assertTrue(unnestNode.isWithOrdinality());
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
    return LogicalProject.create(LogicalValues.createOneRow(cluster), List.of(),
        List.of(fieldAccess), List.of(fieldName));
  }

  /// Every converted node must carry the estimate the optimizer had for it. This is the only point
  /// where the RelNode (which holds the estimate) and the PlanNode (which the runtime reports stats
  /// against) coexist, so a node missed here can never be compared against its actual row count.
  @Test
  public void testEveryConvertedNodeRecordsAnEstimatedRowCount() {
    TypeFactory typeFactory = TypeFactory.INSTANCE;
    RelOptCluster cluster = createCluster(typeFactory);
    RelDataType rowType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("arr", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1))
        .build();
    LogicalValues left = LogicalValues.create(cluster, rowType, ImmutableList.of());
    CorrelationId correlationId = new CorrelationId(0);
    LogicalProject project = buildCorrelatedProject(cluster, rowType, correlationId, "arr");
    Uncollect uncollect = Uncollect.create(project.getTraitSet(), project, false, List.of());
    LogicalCorrelate correlate =
        LogicalCorrelate.create(left, uncollect, correlationId, ImmutableBitSet.of(1), JoinRelType.INNER);

    RelToPlanNodeConverter converter = capturingConverter();
    PlanNode root = converter.toPlanNode(correlate);

    Map<PlanNode, Double> estimates = converter.getEstimatedRowCounts();
    Assert.assertFalse(estimates.isEmpty(), "No estimates were captured at all");

    List<PlanNode> missing = new ArrayList<>();
    collectMissingEstimates(root, estimates, missing);
    Assert.assertTrue(missing.isEmpty(),
        "Every converted node should have an estimate; missing for: " + missing);
  }

  /// Nothing may be captured unless asked for -- the capture walks Calcite's metadata handlers, and
  /// a query that did not request estimates must not pay for them.
  @Test
  public void testNothingCapturedWhenNotRequested() {
    TypeFactory typeFactory = TypeFactory.INSTANCE;
    RelOptCluster cluster = createCluster(typeFactory);
    RelDataType rowType = typeFactory.builder().add("id", SqlTypeName.INTEGER).build();

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);
    converter.toPlanNode(LogicalValues.create(cluster, rowType, ImmutableList.of()));

    Assert.assertTrue(converter.getEstimatedRowCounts().isEmpty(),
        "Estimates were captured even though capture was not requested");
  }

  /// The estimates must be keyed by node identity, not by value.
  ///
  /// [org.apache.pinot.query.planner.plannode.BasePlanNode] defines structural equals/hashCode, so
  /// an equals-keyed map would merge two nodes that merely look alike -- the branches of a UNION ALL,
  /// say -- and report one branch's estimate for the other. Asserting the property rather than the
  /// concrete map class: an unmodifiable wrapper would break an `instanceof IdentityHashMap` check
  /// without changing behavior, and a wrongly populated IdentityHashMap would satisfy it.
  @Test
  public void testEstimatesAreKeyedByIdentityNotByValue() {
    TypeFactory typeFactory = TypeFactory.INSTANCE;
    RelOptCluster cluster = createCluster(typeFactory);
    RelDataType rowType = typeFactory.builder().add("id", SqlTypeName.INTEGER).build();

    RelToPlanNodeConverter converter = capturingConverter();
    PlanNode first = converter.toPlanNode(LogicalValues.create(cluster, rowType, ImmutableList.of()));
    PlanNode second = converter.toPlanNode(LogicalValues.create(cluster, rowType, ImmutableList.of()));

    Assert.assertNotSame(first, second, "test needs two distinct instances");
    Assert.assertEquals(first, second, "test needs two structurally equal nodes to be meaningful");

    Map<PlanNode, Double> estimates = converter.getEstimatedRowCounts();
    Assert.assertEquals(estimates.size(), 2,
        "Structurally equal nodes were merged into one entry, so estimates can be misattributed");
    Assert.assertTrue(estimates.containsKey(first) && estimates.containsKey(second),
        "Both instances must keep their own entry");
  }

  private static RelToPlanNodeConverter capturingConverter() {
    return new RelToPlanNodeConverter(null, CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION,
        !CommonConstants.Helix.DEFAULT_ENABLE_CASE_INSENSITIVE, false, true);
  }

  private static void collectMissingEstimates(PlanNode node, Map<PlanNode, Double> estimates,
      List<PlanNode> missing) {
    if (!estimates.containsKey(node)) {
      missing.add(node);
    }
    for (PlanNode input : node.getInputs()) {
      collectMissingEstimates(input, estimates, missing);
    }
  }
}
