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
package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class EnrichedHashJoinOperatorTest {
  private AutoCloseable _mocks;
  private MultiStageOperator _leftInput;
  private MultiStageOperator _rightInput;
  @Mock
  private VirtualServerAddress _serverAddress;

  private static final DataSchema DEFAULT_CHILD_SCHEMA = new DataSchema(new String[]{"int_col", "string_col"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
    when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldHandleBasicInnerJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator operator = getBasicOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleInnerJoinWithTrueFilter() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });

    HashJoinOperator operator =
        getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
            PlanNode.NodeHint.EMPTY, null, RexExpression.Literal.TRUE, null);
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleInnerJoinWithFalseFilter() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });

    HashJoinOperator operator =
        getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
            PlanNode.NodeHint.EMPTY, null, RexExpression.Literal.FALSE, null);
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldHandleInnerJoinWithFuncCallFilter() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });

    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    HashJoinOperator operator =
        getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
            PlanNode.NodeHint.EMPTY, null, startsWith, null);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 3, "BB"});
  }

  // project tests ----
  @Test
  public void shouldHandleRefProject() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });

    DataSchema projectedSchema = new DataSchema(
        new String[]{"int_col1", "int_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});

    List<RexExpression> projects = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));

    HashJoinOperator operator =
        getProjectedOperator(DEFAULT_CHILD_SCHEMA, resultSchema, projectedSchema, JoinRelType.INNER, List.of(1),
            List.of(1), List.of(),
            PlanNode.NodeHint.EMPTY, null, null, projects);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, 2});
    assertEquals(resultRows.get(1), new Object[]{2, 2});
    assertEquals(resultRows.get(2), new Object[]{2, 3});
  }

  @Test
  public void shouldHandlePlusMinusTransform() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });

    DataSchema projectSchema = new DataSchema(
        new String[]{"sum", "diff"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});

    List<RexExpression> operands = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));

    List<RexExpression> projects =
        List.of(new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, SqlKind.PLUS.name(), operands),
            new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, SqlKind.MINUS.name(), operands));

    HashJoinOperator operator =
        getProjectedOperator(DEFAULT_CHILD_SCHEMA, resultSchema, projectSchema, JoinRelType.INNER, List.of(1),
            List.of(1), List.of(),
            PlanNode.NodeHint.EMPTY, null, null, projects);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{3.0, -1.0});
    assertEquals(resultRows.get(1), new Object[]{4.0, 0.0});
    assertEquals(resultRows.get(2), new Object[]{5.0, -1.0});
  }

  @Test
  public void shouldHandleInnerJoinFilteredProjected() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });

    // filter
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    // project
    List<RexExpression> projects = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));

    HashJoinOperator operator =
        getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
            PlanNode.NodeHint.EMPTY, null, startsWith, projects);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
  }

  @Test
  public void shouldHandleProjectFilterJoinWithNonEquiConditions() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(4, "Be")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(4, "Bd")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });

    // NonEquiCondition
    List<RexExpression> nonEquiConditions = List.of(
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.INT, SqlKind.GREATER_THAN_OR_EQUAL.name(), List.of(
            new RexExpression.InputRef(0), new RexExpression.InputRef(2)
        )));

    // filter is before project
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "Bc")));

    // project
    List<RexExpression> projects = List.of(new RexExpression.InputRef(0));

    HashJoinOperator operator =
        getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), nonEquiConditions,
            PlanNode.NodeHint.EMPTY, null, startsWith, projects);

    List<Object[]> resultRows = new ArrayList<>();
    MseBlock resultBlock = operator.nextBlock();
    while (!resultBlock.isEos()) {
      resultRows.addAll(((MseBlock.Data) resultBlock).asRowHeap().getRows());
      resultBlock = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{3});
  }

  // utils ----
  private EnrichedHashJoinOperator getOperator(DataSchema leftSchema, DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions,
      PlanNode.NodeHint nodeHint,
      RexExpression matchCondition, RexExpression filterCondition, List<RexExpression> projects
  ) {
    List<EnrichedJoinNode.FilterProjectRex> filterProjectRexes = new ArrayList<>();
    if (filterCondition != null) {
      filterProjectRexes.add(new EnrichedJoinNode.FilterProjectRex(filterCondition));
    }
    if (projects != null) {
      filterProjectRexes.add(new EnrichedJoinNode.FilterProjectRex(projects, resultSchema));
    }
    return new EnrichedHashJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, leftSchema, _rightInput,
        new EnrichedJoinNode(-1, resultSchema, resultSchema, nodeHint, List.of(), joinType, leftKeys, rightKeys,
            nonEquiConditions,
            JoinNode.JoinStrategy.HASH, matchCondition,
            filterProjectRexes));
  }

  private EnrichedHashJoinOperator getProjectedOperator(DataSchema leftSchema, DataSchema joinSchema,
      DataSchema projectSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions,
      PlanNode.NodeHint nodeHint,
      RexExpression matchCondition, RexExpression filterCondition, List<RexExpression> projects
  ) {
    List<EnrichedJoinNode.FilterProjectRex> filterProjectRexes = new ArrayList<>();
    if (filterCondition != null) {
      filterProjectRexes.add(new EnrichedJoinNode.FilterProjectRex(filterCondition));
    }
    if (projects != null) {
      filterProjectRexes.add(new EnrichedJoinNode.FilterProjectRex(projects, projectSchema));
    }
    return new EnrichedHashJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, leftSchema, _rightInput,
        new EnrichedJoinNode(-1, joinSchema, projectSchema, nodeHint, List.of(), joinType, leftKeys, rightKeys,
            nonEquiConditions,
            JoinNode.JoinStrategy.HASH, matchCondition,
            filterProjectRexes));
  }

  private HashJoinOperator getBasicOperator(DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions) {
    return getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, joinType, leftKeys, rightKeys, nonEquiConditions,
        PlanNode.NodeHint.EMPTY, null, null, null);
  }
}
