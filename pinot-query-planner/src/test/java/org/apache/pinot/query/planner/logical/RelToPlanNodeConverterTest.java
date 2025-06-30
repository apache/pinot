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
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
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
import org.apache.pinot.calcite.rel.logical.PinotLogicalEnrichedJoin;
import org.apache.pinot.calcite.rel.rules.PinotEnrichedJoinRule;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
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
}
