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

import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;

public class SortedMergeJoinOperatorTest {
  private AutoCloseable _mocks;
  private MultiStageOperator _leftInput;
  private MultiStageOperator _rightInput;
  @Mock
  private VirtualServerAddress _serverAddress;

  private static final DataSchema DEFAULT_CHILD_SCHEMA = new DataSchema(new String[]{"int_col", "string_col"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});

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
  public void shouldHandleBasicLeftJoin() {
    // Test basic LEFT join with sorted data
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .addRow(4, "DD")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Xx")
        .addRow(2, "Yy")
        .addRow(3, "Zz")
        .buildWithEos();

    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});

    SortedMergeJoinOperator operator = getOperator(resultSchema, JoinRelType.LEFT, List.of(0), List.of(0));
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Expected:
    // - (1, "Aa") matches (1, "Xx") -> (1, "Aa", 1, "Xx")
    // - (2, "BB") matches (2, "Yy") -> (2, "BB", 2, "Yy")
    // - (4, "DD") has no match -> (4, "DD", null, null)
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 1, "Xx"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "Yy"});
    assertEquals(resultRows.get(2), new Object[]{4, "DD", null, null});
  }

  @Test
  public void shouldHandleLeftJoinWithNulls() {
    // Test LEFT join with null keys
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, null)
        .addRow(3, "CC")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Xx")
        .addRow(2, null)
        .addRow(4, "Yy")
        .buildWithEos();

    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});

    SortedMergeJoinOperator operator = getOperator(resultSchema, JoinRelType.LEFT, List.of(0), List.of(0));
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Expected per SQL standard - null keys don't match:
    // - (1, "Aa") matches (1, "Xx") -> (1, "Aa", 1, "Xx")
    // - (2, null) doesn't match (2, null) -> (2, null, null, null)
    // - (3, "CC") has no match -> (3, "CC", null, null)
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 1, "Xx"});
    assertEquals(resultRows.get(1), new Object[]{2, null, 2, null});
  }

  @Test
  public void shouldHandleEmptyLeftInput() {
    // Test behavior when left input is empty
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});

    SortedMergeJoinOperator operator = getOperator(resultSchema, JoinRelType.LEFT, List.of(0), List.of(0));
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Expected: Empty result since no left rows to preserve
    assertEquals(resultRows.size(), 0);
  }

  private SortedMergeJoinOperator getOperator(DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys) {
    // Create mock input plan nodes
    PlanNode leftInputNode = Mockito.mock(PlanNode.class);
    PlanNode rightInputNode = Mockito.mock(PlanNode.class);
    when(leftInputNode.getDataSchema()).thenReturn(DEFAULT_CHILD_SCHEMA);
    when(rightInputNode.getDataSchema()).thenReturn(DEFAULT_CHILD_SCHEMA);

    return new SortedMergeJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, _rightInput, new JoinNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(leftInputNode, rightInputNode),
        joinType, leftKeys, rightKeys, List.<RexExpression>of(), JoinNode.JoinStrategy.HASH));
  }
}
