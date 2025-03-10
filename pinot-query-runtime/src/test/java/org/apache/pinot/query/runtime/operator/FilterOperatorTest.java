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
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockTestUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class FilterOperatorTest {
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
  public void shouldPropagateUpstreamErrorBlock() {
    when(_input.nextBlock()).thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("filterError")));
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol"}, new ColumnDataType[]{
        ColumnDataType.BOOLEAN
    });
    FilterOperator operator = getOperator(inputSchema, RexExpression.Literal.TRUE);
    TransferableBlock block = operator.nextBlock();
    assertTrue(block.isErrorBlock());
    assertTrue(block.getExceptions().get(QueryErrorCode.UNKNOWN.getId()).contains("filterError"));
  }

  @Test
  public void shouldPropagateUpstreamEOS() {
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{
        ColumnDataType.INT
    });
    when(_input.nextBlock()).thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    FilterOperator operator = getOperator(inputSchema, RexExpression.Literal.TRUE);
    TransferableBlock block = operator.nextBlock();
    assertTrue(block.isEndOfStreamBlock());
  }

  @Test
  public void shouldHandleTrueBooleanLiteralFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{
        ColumnDataType.INT
    });
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{0}, new Object[]{1}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    FilterOperator operator = getOperator(inputSchema, RexExpression.Literal.TRUE);
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{0});
    assertEquals(resultRows.get(1), new Object[]{1});
  }

  @Test
  public void shouldHandleFalseBooleanLiteralFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{
        ColumnDataType.INT
    });
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1}, new Object[]{2}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    FilterOperator operator = getOperator(inputSchema, RexExpression.Literal.FALSE);
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Filter operand must "
      + "return BOOLEAN, got: STRING")
  public void shouldThrowOnNonBooleanTypeBooleanLiteral() {
    RexExpression booleanLiteral = new RexExpression.Literal(ColumnDataType.STRING, "false");
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{
        ColumnDataType.INT
    });
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1}, new Object[]{2}));
    getOperator(inputSchema, booleanLiteral);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Filter operand must "
      + "return BOOLEAN, got: INT")
  public void shouldThrowOnNonBooleanTypeInputRef() {
    RexExpression ref0 = new RexExpression.InputRef(0);
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{
        ColumnDataType.INT
    });
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1}, new Object[]{2}));
    getOperator(inputSchema, ref0);
  }

  @Test
  public void shouldHandleBooleanInputRef() {
    RexExpression ref1 = new RexExpression.InputRef(1);
    DataSchema inputSchema = new DataSchema(new String[]{"intCol", "boolCol"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.BOOLEAN
    });
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1, 1}, new Object[]{2, 0}));
    FilterOperator operator = getOperator(inputSchema, ref1);
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{1, 1});
  }

  @Test
  public void shouldHandleAndFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol0", "boolCol1"}, new ColumnDataType[]{
        ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{1, 1}, new Object[]{0, 0}, new Object[]{1, 0}));
    RexExpression.FunctionCall andCall = new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.AND.name(),
        List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));
    FilterOperator operator = getOperator(inputSchema, andCall);
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{1, 1});
  }

  @Test
  public void shouldHandleOrFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol0", "boolCol1"}, new ColumnDataType[]{
        ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{1, 1}, new Object[]{0, 0}, new Object[]{1, 0}));
    RexExpression.FunctionCall orCall = new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.OR.name(),
        List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));
    FilterOperator operator = getOperator(inputSchema, orCall);
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, 1});
    assertEquals(resultRows.get(1), new Object[]{1, 0});
  }

  @Test
  public void shouldHandleNotFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol0", "boolCol1"}, new ColumnDataType[]{
        ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{1, 1}, new Object[]{0, 0}, new Object[]{1, 0}));
    RexExpression.FunctionCall notCall = new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.NOT.name(),
        List.of(new RexExpression.InputRef(0)));
    FilterOperator operator = getOperator(inputSchema, notCall);
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0)[0], 0);
    assertEquals(resultRows.get(0)[1], 0);
  }

  @Test
  public void shouldHandleGreaterThanFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"int0", "int1"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.INT
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{1, 2}, new Object[]{3, 2}, new Object[]{1, 1}));
    RexExpression.FunctionCall greaterThan =
        new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.GREATER_THAN.name(),
            List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));
    FilterOperator operator = getOperator(inputSchema, greaterThan);
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{3, 2});
  }

  @Test
  public void shouldHandleBooleanFunction() {
    DataSchema inputSchema = new DataSchema(new String[]{"string1"}, new ColumnDataType[]{
        ColumnDataType.STRING
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{"starTree"}, new Object[]{"treeStar"}));
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(0), new RexExpression.Literal(ColumnDataType.STRING, "star")));
    FilterOperator operator = getOperator(inputSchema, startsWith);
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{"starTree"});
  }

  //@formatter:off
  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = "Unsupported function: startsWithError"
  )
  //@formatter:on
  public void shouldThrowOnInvalidFunction() {
    DataSchema inputSchema = new DataSchema(new String[]{"string1"}, new ColumnDataType[]{
        ColumnDataType.STRING
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{"starTree"}, new Object[]{"treeStar"}));
    RexExpression.FunctionCall startsWith = new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, "startsWithError",
        List.of(new RexExpression.InputRef(0), new RexExpression.Literal(ColumnDataType.STRING, "star")));
    getOperator(inputSchema, startsWith);
  }

  private FilterOperator getOperator(DataSchema schema, RexExpression condition) {
    return new FilterOperator(OperatorTestUtil.getTracingContext(), _input,
        new FilterNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), condition));
  }
}
