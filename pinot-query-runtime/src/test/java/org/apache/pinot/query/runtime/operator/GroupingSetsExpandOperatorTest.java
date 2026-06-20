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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.GroupingSetsExpandNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class GroupingSetsExpandOperatorTest {
  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _input;

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  /**
   * ROLLUP(d1, d2) over a row [a, x, 5]: emits one output row per grouping set, nulling the rolled-up group keys and
   * appending the per-set $groupingId. The grouping sets {d1,d2}, {d1}, {} have rolled-up bitmasks 0, 2 (d2 at union
   * position 1), and 3 (both). An already-present value in a non-rolled-up key (d1 in set {d1}) must survive.
   */
  @Test
  public void shouldEmitOneRowPerGroupingSetWithNullsAndGroupingId() {
    DataSchema inputSchema = new DataSchema(new String[]{"d1", "d2", "m"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{"a", "x", 5}));

    DataSchema resultSchema = new DataSchema(new String[]{"d1", "d2", "m", "$groupingId"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT});
    GroupingSetsExpandNode node = new GroupingSetsExpandNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(),
        List.of(0, 1), List.of(0, 2, 3));
    GroupingSetsExpandOperator operator =
        new GroupingSetsExpandOperator(OperatorTestUtil.getTracingContext(), _input, node);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 3, "one row per grouping set");
    // {d1, d2}: nothing rolled up, $groupingId 0.
    assertEquals(rows.get(0), new Object[]{"a", "x", 5, 0});
    // {d1}: d2 rolled up -> null, $groupingId 2; d1 survives.
    assertEquals(rows.get(1), new Object[]{"a", null, 5, 2});
    // {}: both rolled up -> null, $groupingId 3.
    assertEquals(rows.get(2), new Object[]{null, null, 5, 3});
  }

  @Test
  public void shouldEmitGroupingSetsForEveryInputRow() {
    DataSchema inputSchema =
        new DataSchema(new String[]{"d1", "m"}, new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{"a", 1}, new Object[]{"b", 2}));

    DataSchema resultSchema = new DataSchema(new String[]{"d1", "m", "$groupingId"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT});
    // ROLLUP(d1): grouping sets {d1}, {} -> rolled-up bitmasks 0, 1.
    GroupingSetsExpandNode node = new GroupingSetsExpandNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(),
        List.of(0), List.of(0, 1));
    GroupingSetsExpandOperator operator =
        new GroupingSetsExpandOperator(OperatorTestUtil.getTracingContext(), _input, node);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 4, "2 input rows x 2 grouping sets");
    assertEquals(rows.get(0), new Object[]{"a", 1, 0});
    assertEquals(rows.get(1), new Object[]{null, 1, 1});
    assertEquals(rows.get(2), new Object[]{"b", 2, 0});
    assertEquals(rows.get(3), new Object[]{null, 2, 1});
  }

  @Test
  public void shouldPropagateEos() {
    DataSchema resultSchema = new DataSchema(new String[]{"d1", "$groupingId"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    GroupingSetsExpandNode node = new GroupingSetsExpandNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(),
        List.of(0), List.of(0, 1));
    when(_input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    GroupingSetsExpandOperator operator =
        new GroupingSetsExpandOperator(OperatorTestUtil.getTracingContext(), _input, node);
    assertTrue(operator.nextBlock().isEos(), "EOS from the input must propagate unchanged");
  }
}
