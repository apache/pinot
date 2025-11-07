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
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;


public class UnnestOperatorTest {
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

  @Test
  public void shouldExpandPrimitiveArrayElements() {
    DataSchema inputSchema = new DataSchema(new String[]{"id", "arr"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.INT_ARRAY
    });

    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema,
            new Object[]{1, new int[]{10, 20}},
            new Object[]{2, new int[]{}},
            new Object[]{3, null},
            new Object[]{4, new int[]{30}}));

    DataSchema resultSchema = new DataSchema(new String[]{"id", "arr", "elem"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.INT_ARRAY, ColumnDataType.INT
    });
    RexExpression arrayExpr = new RexExpression.InputRef(1);
    UnnestNode node =
        new UnnestNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), arrayExpr, "elem", false, null);
    UnnestOperator operator = new UnnestOperator(OperatorTestUtil.getTracingContext(), _input, inputSchema, node);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 3); // [1,10], [1,20], [4,30]
    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(0)[1], new int[]{10, 20});
    assertEquals(rows.get(0)[2], 10);
    assertEquals(rows.get(1)[0], 1);
    assertEquals(rows.get(1)[1], new int[]{10, 20});
    assertEquals(rows.get(1)[2], 20);
    assertEquals(rows.get(2)[0], 4);
    assertEquals(rows.get(2)[1], new int[]{30});
    assertEquals(rows.get(2)[2], 30);
  }

  @Test
  public void shouldExpandListElements() {
    DataSchema inputSchema = new DataSchema(new String[]{"id", "tags"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING_ARRAY
    });

    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema,
            new Object[]{10, List.of("a", "b")},
            new Object[]{11, List.of()}));

    DataSchema resultSchema = new DataSchema(new String[]{"id", "tags", "tag"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING_ARRAY, ColumnDataType.STRING
    });
    RexExpression arrayExpr = new RexExpression.InputRef(1);
    UnnestNode node =
        new UnnestNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), arrayExpr, "tag", false, null);
    UnnestOperator operator = new UnnestOperator(OperatorTestUtil.getTracingContext(), _input, inputSchema, node);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[0], 10);
    assertEquals(rows.get(0)[2], "a");
    assertEquals(rows.get(1)[0], 10);
    assertEquals(rows.get(1)[2], "b");
  }

  @Test
  public void shouldExpandWithOrdinality() {
    DataSchema inputSchema = new DataSchema(new String[]{"id", "arr"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.INT_ARRAY
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema,
            new Object[]{1, new int[]{5, 6}}));

    DataSchema resultSchema = new DataSchema(new String[]{"id", "arr", "elem", "idx"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.INT_ARRAY, ColumnDataType.INT, ColumnDataType.INT
    });
    RexExpression arrayExpr = new RexExpression.InputRef(1);
    UnnestNode node =
        new UnnestNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), arrayExpr, "elem", true, "idx");
    UnnestOperator operator = new UnnestOperator(OperatorTestUtil.getTracingContext(), _input, inputSchema, node);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(0)[2], 5);
    assertEquals(rows.get(0)[3], 1);
    assertEquals(rows.get(1)[0], 1);
    assertEquals(rows.get(1)[2], 6);
    assertEquals(rows.get(1)[3], 2);
  }

  @Test
  public void shouldRespectExplicitElementAndOrdinalPositions() {
    DataSchema inputSchema = new DataSchema(new String[]{"id", "tags"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING_ARRAY
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema,
            new Object[]{5, List.of("x", "y")}));

    DataSchema resultSchema = new DataSchema(new String[]{"id", "tags", "ord", "tag"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING_ARRAY, ColumnDataType.INT, ColumnDataType.STRING
    });
    RexExpression arrayExpr = new RexExpression.InputRef(1);
    UnnestNode node =
        new UnnestNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), arrayExpr, "tag", true, "ord",
            3, 2);
    UnnestOperator operator = new UnnestOperator(OperatorTestUtil.getTracingContext(), _input, inputSchema, node);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[2], 1);
    assertEquals(rows.get(0)[3], "x");
    assertEquals(rows.get(1)[2], 2);
    assertEquals(rows.get(1)[3], "y");
  }
}
