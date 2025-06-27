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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.type.TypeFactory;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


public class RelToPlanNodeConverterTest {
  private static final TypeFactory TYPE_FACTORY = TypeFactory.INSTANCE;
  private static final RexBuilder REX_BUILDER = RexBuilder.DEFAULT;

  private AutoCloseable _mocks;

  @Mock
  private RelOptRuleCall _call;
  @Mock
  private RelOptCluster _cluster;
  @Mock
  private RelMetadataQuery _query;
  private RelTraitSet _traits;

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

    when(_call.getMetadataQuery()).thenReturn(_query);
    when(_query.getMaxRowCount(Mockito.any())).thenReturn(null);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

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
  public void testGetMergeJoinCollations() {
    RelTraitSet traitSet = RelTraitSet.createEmpty();
    traitSet = traitSet.plus(RelCollations.of(new RelFieldCollation(0)));

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .build();

    LogicalValues input = new LogicalValues(_cluster, traitSet, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)))));

    LogicalValues input2 = new LogicalValues(_cluster, traitSet, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)))));

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(intType, 0), REX_BUILDER.makeInputRef(intType, 1));

    Map<String, String> strategyHint = new HashMap<>();
    strategyHint.put(PinotHintOptions.JoinHintOptions.JOIN_STRATEGY,
        PinotHintOptions.JoinHintOptions.MERGE_JOIN_STRATEGY);
    RelHint hint = RelHint.builder(PinotHintOptions.JOIN_HINT_OPTIONS).hintOptions(strategyHint).build();

    LogicalJoin join =
        new LogicalJoin(_cluster, RelTraitSet.createEmpty(), List.of(hint), input, input2, joinCondition, Set.of(),
            JoinRelType.INNER, false, ImmutableList.of());

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null);
    PlanNode node = converter.toPlanNode(join);
    assert (node instanceof JoinNode);
    JoinNode joinNode = (JoinNode) node;
    Assert.assertEquals(joinNode.getCollations(), List.of(new RelFieldCollation(0)));
  }

  @Test
  public void testRejectMergeJoinCollationsEmptyCollations() {
    RelTraitSet traitSet = RelTraitSet.createEmpty();
    traitSet = traitSet.plus(RelCollations.of(new RelFieldCollation(0)));

    RelTraitSet traitSet2 = RelTraitSet.createEmpty();

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .build();

    LogicalValues input = new LogicalValues(_cluster, traitSet, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)))));

    LogicalValues input2 = new LogicalValues(_cluster, traitSet2, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)))));

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(intType, 0), REX_BUILDER.makeInputRef(intType, 1));

    Map<String, String> strategyHint = new HashMap<>();
    strategyHint.put(PinotHintOptions.JoinHintOptions.JOIN_STRATEGY,
        PinotHintOptions.JoinHintOptions.MERGE_JOIN_STRATEGY);
    RelHint hint = RelHint.builder(PinotHintOptions.JOIN_HINT_OPTIONS).hintOptions(strategyHint).build();

    LogicalJoin join =
        new LogicalJoin(_cluster, RelTraitSet.createEmpty(), List.of(hint), input, input2, joinCondition, Set.of(),
            JoinRelType.INNER, false, ImmutableList.of());

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null);
    PlanNode node = converter.toPlanNode(join);
    assert (node instanceof JoinNode);
    JoinNode joinNode = (JoinNode) node;
    Assert.assertNull(joinNode.getCollations());
  }

  @Test
  public void testRejectMergeJoinCollationsWrongDirection() {
    RelTraitSet traitSet = RelTraitSet.createEmpty();
    traitSet = traitSet.plus(RelCollations.of(new RelFieldCollation(0)));

    RelTraitSet traitSet2 = RelTraitSet.createEmpty();
    traitSet2 = traitSet2.plus(RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)));

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .build();

    LogicalValues input = new LogicalValues(_cluster, traitSet, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)))));

    LogicalValues input2 = new LogicalValues(_cluster, traitSet2, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)))));

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(intType, 0), REX_BUILDER.makeInputRef(intType, 1));

    Map<String, String> strategyHint = new HashMap<>();
    strategyHint.put(PinotHintOptions.JoinHintOptions.JOIN_STRATEGY,
        PinotHintOptions.JoinHintOptions.MERGE_JOIN_STRATEGY);
    RelHint hint = RelHint.builder(PinotHintOptions.JOIN_HINT_OPTIONS).hintOptions(strategyHint).build();

    LogicalJoin join =
        new LogicalJoin(_cluster, RelTraitSet.createEmpty(), List.of(hint), input, input2, joinCondition, Set.of(),
            JoinRelType.INNER, false, ImmutableList.of());

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null);
    PlanNode node = converter.toPlanNode(join);
    assert (node instanceof JoinNode);
    JoinNode joinNode = (JoinNode) node;
    Assert.assertNull(joinNode.getCollations());
  }

  @Test
  public void testRejectMergeJoinCollationsWrongColumn() {
    RelTraitSet traitSet = RelTraitSet.createEmpty();
    traitSet = traitSet.plus(RelCollations.of(new RelFieldCollation(0)));

    RelTraitSet traitSet2 = RelTraitSet.createEmpty();
    traitSet2 = traitSet2.plus(RelCollations.of(new RelFieldCollation(1)));

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .build();

    RelDataType intColType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    LogicalValues input = new LogicalValues(_cluster, traitSet, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, intColType),
            REX_BUILDER.makeLiteral(2, intColType)
        )));

    LogicalValues input2 = new LogicalValues(_cluster, traitSet2, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, intColType),
            REX_BUILDER.makeLiteral(2, intColType)
        )));

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(intType, 0), REX_BUILDER.makeInputRef(intType, 2));

    Map<String, String> strategyHint = new HashMap<>();
    strategyHint.put(PinotHintOptions.JoinHintOptions.JOIN_STRATEGY,
        PinotHintOptions.JoinHintOptions.MERGE_JOIN_STRATEGY);
    RelHint hint = RelHint.builder(PinotHintOptions.JOIN_HINT_OPTIONS).hintOptions(strategyHint).build();

    LogicalJoin join =
        new LogicalJoin(_cluster, RelTraitSet.createEmpty(), List.of(hint), input, input2, joinCondition, Set.of(),
            JoinRelType.INNER, false, ImmutableList.of());

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null);
    PlanNode node = converter.toPlanNode(join);
    assert (node instanceof JoinNode);
    JoinNode joinNode = (JoinNode) node;
    Assert.assertNull(joinNode.getCollations());
  }

  @Test
  public void testGetMergeJoinCollationsCompositeJoinKey() {
    RelTraitSet traitSet = RelTraitSet.createEmpty();
    traitSet = traitSet.plus(
        RelCollations.of(new RelFieldCollation(0), new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)));

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .build();

    RelDataType intColType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    LogicalValues input = new LogicalValues(_cluster, traitSet, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, intColType),
            REX_BUILDER.makeLiteral(2, intColType)
        )));

    LogicalValues input2 = new LogicalValues(_cluster, traitSet, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, intColType),
            REX_BUILDER.makeLiteral(2, intColType)
        )));

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.AND,
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            REX_BUILDER.makeInputRef(intType, 0), REX_BUILDER.makeInputRef(intType, 2)),
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            REX_BUILDER.makeInputRef(intType, 1), REX_BUILDER.makeInputRef(intType, 3)));

    Map<String, String> strategyHint = new HashMap<>();
    strategyHint.put(PinotHintOptions.JoinHintOptions.JOIN_STRATEGY,
        PinotHintOptions.JoinHintOptions.MERGE_JOIN_STRATEGY);
    RelHint hint = RelHint.builder(PinotHintOptions.JOIN_HINT_OPTIONS).hintOptions(strategyHint).build();

    LogicalJoin join =
        new LogicalJoin(_cluster, RelTraitSet.createEmpty(), List.of(hint), input, input2, joinCondition, Set.of(),
            JoinRelType.INNER, false, ImmutableList.of());

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null);
    PlanNode node = converter.toPlanNode(join);
    assert (node instanceof JoinNode);
    JoinNode joinNode = (JoinNode) node;
    Assert.assertEquals(joinNode.getCollations(),
        List.of(new RelFieldCollation(0), new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)));
  }

  @Test
  public void testGetMergeJoinCollationsShiftFieldIdx() {
    // a.co12 = b.col2 AND a.col3 = b.col1
    RelTraitSet traitSet = RelTraitSet.createEmpty();
    traitSet = traitSet.plus(
        RelCollations.of(new RelFieldCollation(1), new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING)));

    RelTraitSet traitSet2 = RelTraitSet.createEmpty();
    traitSet2 = traitSet2.plus(
        RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING), new RelFieldCollation(1)));

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    RelDataType intColType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    LogicalValues input = new LogicalValues(_cluster, traitSet, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, intColType),
            REX_BUILDER.makeLiteral(2, intColType),
            REX_BUILDER.makeLiteral(3, intColType)
        )));

    LogicalValues input2 = new LogicalValues(_cluster, traitSet2, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, intColType),
            REX_BUILDER.makeLiteral(2, intColType),
            REX_BUILDER.makeLiteral(3, intColType)
        )));

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.AND,
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            REX_BUILDER.makeInputRef(intType, 1), REX_BUILDER.makeInputRef(intType, 4)),
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            REX_BUILDER.makeInputRef(intType, 2), REX_BUILDER.makeInputRef(intType, 3)));

    Map<String, String> strategyHint = new HashMap<>();
    strategyHint.put(PinotHintOptions.JoinHintOptions.JOIN_STRATEGY,
        PinotHintOptions.JoinHintOptions.MERGE_JOIN_STRATEGY);
    RelHint hint = RelHint.builder(PinotHintOptions.JOIN_HINT_OPTIONS).hintOptions(strategyHint).build();

    LogicalJoin join =
        new LogicalJoin(_cluster, RelTraitSet.createEmpty(), List.of(hint), input, input2, joinCondition, Set.of(),
            JoinRelType.INNER, false, ImmutableList.of());

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null);
    PlanNode node = converter.toPlanNode(join);
    assert (node instanceof JoinNode);
    JoinNode joinNode = (JoinNode) node;
    Assert.assertEquals(joinNode.getCollations(),
        List.of(new RelFieldCollation(0), new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)));
  }

  @Test
  public void testGetMergeJoinCollationsRedundantCollation() {
    // a.co12 = b.col2 AND a.col3 = b.col1
    RelTraitSet traitSet = RelTraitSet.createEmpty();
    traitSet = traitSet.plus(
        RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING), new RelFieldCollation(1),
            new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING)));

    RelTraitSet traitSet2 = RelTraitSet.createEmpty();
    traitSet2 = traitSet2.plus(
        RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING), new RelFieldCollation(1),
            new RelFieldCollation(2)));

    RelDataType intType = TYPE_FACTORY.builder()
        .add("col1", SqlTypeName.INTEGER)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .build();

    RelDataType intColType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    LogicalValues input = new LogicalValues(_cluster, traitSet, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, intColType),
            REX_BUILDER.makeLiteral(2, intColType),
            REX_BUILDER.makeLiteral(3, intColType)
        )));

    LogicalValues input2 = new LogicalValues(_cluster, traitSet2, intType, ImmutableList.of(
        ImmutableList.of(REX_BUILDER.makeLiteral(1, intColType),
            REX_BUILDER.makeLiteral(2, intColType),
            REX_BUILDER.makeLiteral(3, intColType)
        )));

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.AND,
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            REX_BUILDER.makeInputRef(intType, 1), REX_BUILDER.makeInputRef(intType, 4)),
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            REX_BUILDER.makeInputRef(intType, 2), REX_BUILDER.makeInputRef(intType, 3)));

    Map<String, String> strategyHint = new HashMap<>();
    strategyHint.put(PinotHintOptions.JoinHintOptions.JOIN_STRATEGY,
        PinotHintOptions.JoinHintOptions.MERGE_JOIN_STRATEGY);
    RelHint hint = RelHint.builder(PinotHintOptions.JOIN_HINT_OPTIONS).hintOptions(strategyHint).build();

    LogicalJoin join =
        new LogicalJoin(_cluster, RelTraitSet.createEmpty(), List.of(hint), input, input2, joinCondition, Set.of(),
            JoinRelType.INNER, false, ImmutableList.of());

    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(null);
    PlanNode node = converter.toPlanNode(join);
    assert (node instanceof JoinNode);
    JoinNode joinNode = (JoinNode) node;
    Assert.assertEquals(joinNode.getCollations(),
        List.of(new RelFieldCollation(0), new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)));
  }
}
