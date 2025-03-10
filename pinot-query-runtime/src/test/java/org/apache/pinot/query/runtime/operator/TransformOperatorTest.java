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
import java.util.Map;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TransformOperatorTest {
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
  public void shouldHandleRefTransform() {
    DataSchema inputSchema = new DataSchema(new String[]{"intCol", "strCol"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{1, "a"}, new Object[]{2, "b"}));
    DataSchema resultSchema = new DataSchema(new String[]{"inCol", "strCol"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    List<RexExpression> projects = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1));
    TransformOperator operator = getOperator(inputSchema, resultSchema, projects);
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "a"});
    assertEquals(resultRows.get(1), new Object[]{2, "b"});
  }

  @Test
  public void shouldHandleLiteralTransform() {
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol", "strCol"}, new ColumnDataType[]{
        ColumnDataType.BOOLEAN, ColumnDataType.STRING
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{1, "a"}, new Object[]{2, "b"}));
    DataSchema resultSchema = new DataSchema(new String[]{"boolCol", "strCol"},
        new ColumnDataType[]{ColumnDataType.BOOLEAN, ColumnDataType.STRING});
    List<RexExpression> projects =
        List.of(RexExpression.Literal.TRUE, new RexExpression.Literal(ColumnDataType.STRING, "str"));
    TransformOperator operator = getOperator(inputSchema, resultSchema, projects);
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "str"});
    assertEquals(resultRows.get(1), new Object[]{1, "str"});
  }

  @Test
  public void shouldHandlePlusMinusFuncTransform() {
    DataSchema inputSchema = new DataSchema(new String[]{"doubleCol1", "doubleCol2"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{1.0, 1.0}, new Object[]{2.0, 3.0}));
    DataSchema resultSchema = new DataSchema(new String[]{"plusR", "minusR"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    List<RexExpression> operands = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1));
    List<RexExpression> projects =
        List.of(new RexExpression.FunctionCall(ColumnDataType.DOUBLE, SqlKind.PLUS.name(), operands),
            new RexExpression.FunctionCall(ColumnDataType.DOUBLE, SqlKind.MINUS.name(), operands));
    TransformOperator operator = getOperator(inputSchema, resultSchema, projects);
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2.0, 0.0});
    assertEquals(resultRows.get(1), new Object[]{5.0, -1.0});
  }

  @Test
  public void shouldThrowOnTypeMismatchFuncTransform() {
    DataSchema inputSchema = new DataSchema(new String[]{"string1", "string2"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{"str1", "str1"}, new Object[]{"str2", "str3"}));
    DataSchema resultSchema = new DataSchema(new String[]{"plusR", "minusR"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    List<RexExpression> operands = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1));
    List<RexExpression> projects =
        List.of(new RexExpression.FunctionCall(ColumnDataType.DOUBLE, SqlKind.PLUS.name(), operands),
            new RexExpression.FunctionCall(ColumnDataType.DOUBLE, SqlKind.MINUS.name(), operands));
    TransformOperator operator = getOperator(inputSchema, resultSchema, projects);
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    Map<QueryErrorCode, String> exceptions = ((ErrorMseBlock) block).getErrorMessages();
    String errorMsg = exceptions.get(QueryErrorCode.QUERY_EXECUTION);
    assertNotNull(errorMsg, "Expected QUERY_EXECUTION error but found " + exceptions);
    assertTrue(errorMsg.contains("Invalid conversion"), "Expected 'Invalid conversion' but found " + errorMsg);
  }

  @Test
  public void shouldPropagateUpstreamError() {
    DataSchema inputSchema = new DataSchema(new String[]{"string1", "string2"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    when(_input.nextBlock()).thenReturn(
        ErrorMseBlock.fromException(new Exception("transformError")));
    DataSchema resultSchema = new DataSchema(new String[]{"inCol", "strCol"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    List<RexExpression> projects =
        List.of(RexExpression.Literal.TRUE, new RexExpression.Literal(ColumnDataType.STRING, "str"));
    TransformOperator operator = getOperator(inputSchema, resultSchema, projects);
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains("transformError"));
  }

  @Test
  public void testNoopBlock() {
    DataSchema inputSchema = new DataSchema(new String[]{"string1", "string2"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    when(_input.nextBlock()).thenReturn(
            OperatorTestUtil.block(inputSchema, new Object[]{"a", "a"}, new Object[]{"b", "b"}))
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{"c", "c"}, new Object[]{"d", "d"}, new Object[]{
            "e", "e"
        }));
    DataSchema resultSchema = new DataSchema(new String[]{"boolCol", "strCol"},
        new ColumnDataType[]{ColumnDataType.BOOLEAN, ColumnDataType.STRING});
    List<RexExpression> projects =
        List.of(RexExpression.Literal.TRUE, new RexExpression.Literal(ColumnDataType.STRING, "str"));
    TransformOperator operator = getOperator(inputSchema, resultSchema, projects);
    // First block has 1 row.
    List<Object[]> resultRows1 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows1.size(), 2);
    assertEquals(resultRows1.get(0), new Object[]{1, "str"});
    assertEquals(resultRows1.get(1), new Object[]{1, "str"});
    // Second block has 2 rows.
    List<Object[]> resultRows2 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows2.size(), 3);
    assertEquals(resultRows2.get(0), new Object[]{1, "str"});
    assertEquals(resultRows2.get(1), new Object[]{1, "str"});
    assertEquals(resultRows2.get(2), new Object[]{1, "str"});
  }

  private TransformOperator getOperator(DataSchema inputSchema, DataSchema resultSchema, List<RexExpression> projects) {
    return new TransformOperator(OperatorTestUtil.getTracingContext(), _input, inputSchema,
        new ProjectNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), projects));
  }
}
