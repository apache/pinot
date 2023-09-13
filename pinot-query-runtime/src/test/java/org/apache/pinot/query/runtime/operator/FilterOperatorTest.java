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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class FilterOperatorTest {
  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _upstreamOperator;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldPropagateUpstreamErrorBlock() {
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("filterError")));
    RexExpression booleanLiteral = new RexExpression.Literal(ColumnDataType.BOOLEAN, 1);
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol"}, new ColumnDataType[]{
        ColumnDataType.BOOLEAN
    });
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, booleanLiteral);
    TransferableBlock errorBlock = op.getNextBlock();
    Assert.assertTrue(errorBlock.isErrorBlock());
    Assert.assertTrue(errorBlock.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains("filterError"));
  }

  @Test
  public void shouldPropagateUpstreamEOS() {
    RexExpression booleanLiteral = new RexExpression.Literal(ColumnDataType.BOOLEAN, 1);
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{
        ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, booleanLiteral);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertTrue(dataBlock.isEndOfStreamBlock());
  }

  @Test
  public void shouldHandleTrueBooleanLiteralFilter() {
    RexExpression booleanLiteral = new RexExpression.Literal(ColumnDataType.BOOLEAN, 1);
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{
        ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{0}, new Object[]{1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, booleanLiteral);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0)[0], 0);
    Assert.assertEquals(result.get(1)[0], 1);
  }

  @Test
  public void shouldHandleFalseBooleanLiteralFilter() {
    RexExpression booleanLiteral = new RexExpression.Literal(ColumnDataType.BOOLEAN, 0);
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{
        ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1}, new Object[]{2}));
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, booleanLiteral);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    Assert.assertTrue(result.isEmpty());
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Filter operand must "
      + "return BOOLEAN, got: STRING")
  public void shouldThrowOnNonBooleanTypeBooleanLiteral() {
    RexExpression booleanLiteral = new RexExpression.Literal(ColumnDataType.STRING, "false");
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{
        ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1}, new Object[]{2}));
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, booleanLiteral);
    op.getNextBlock();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Filter operand must "
      + "return BOOLEAN, got: INT")
  public void shouldThrowOnNonBooleanTypeInputRef() {
    RexExpression ref0 = new RexExpression.InputRef(0);
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{
        ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1}, new Object[]{2}));
    FilterOperator op = new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, ref0);
    op.getNextBlock();
  }

  @Test
  public void shouldHandleBooleanInputRef() {
    RexExpression ref1 = new RexExpression.InputRef(1);
    DataSchema inputSchema = new DataSchema(new String[]{"intCol", "boolCol"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.BOOLEAN
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1, 1}, new Object[]{2, 0}));
    FilterOperator op = new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, ref1);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0)[0], 1);
    Assert.assertEquals(result.get(0)[1], 1);
  }

  @Test
  public void shouldHandleAndFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol0", "boolCol1"}, new ColumnDataType[]{
        ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1, 1}, new Object[]{0, 0}, new Object[]{1, 0}));
    RexExpression.FunctionCall andCall = new RexExpression.FunctionCall(SqlKind.AND, ColumnDataType.BOOLEAN, "AND",
        ImmutableList.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));

    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, andCall);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0)[0], 1);
    Assert.assertEquals(result.get(0)[1], 1);
  }

  @Test
  public void shouldHandleOrFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol0", "boolCol1"}, new ColumnDataType[]{
        ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1, 1}, new Object[]{0, 0}, new Object[]{1, 0}));
    RexExpression.FunctionCall orCall = new RexExpression.FunctionCall(SqlKind.OR, ColumnDataType.BOOLEAN, "OR",
        ImmutableList.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));

    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, orCall);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0)[0], 1);
    Assert.assertEquals(result.get(0)[1], 1);
    Assert.assertEquals(result.get(1)[0], 1);
    Assert.assertEquals(result.get(1)[1], 0);
  }

  @Test
  public void shouldHandleNotFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol0", "boolCol1"}, new ColumnDataType[]{
        ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1, 1}, new Object[]{0, 0}, new Object[]{1, 0}));
    RexExpression.FunctionCall notCall = new RexExpression.FunctionCall(SqlKind.NOT, ColumnDataType.BOOLEAN, "NOT",
        ImmutableList.of(new RexExpression.InputRef(0)));

    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, notCall);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0)[0], 0);
    Assert.assertEquals(result.get(0)[1], 0);
  }

  @Test
  public void shouldHandleGreaterThanFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"int0", "int1"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1, 2}, new Object[]{3, 2}, new Object[]{1, 1}));
    RexExpression.FunctionCall greaterThan =
        new RexExpression.FunctionCall(SqlKind.GREATER_THAN, ColumnDataType.BOOLEAN, "greaterThan",
            ImmutableList.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, greaterThan);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    List<Object[]> expectedResult = ImmutableList.of(new Object[]{3, 2});
    Assert.assertEquals(result.size(), expectedResult.size());
    Assert.assertEquals(result.get(0), expectedResult.get(0));
  }

  @Test
  public void shouldHandleBooleanFunction() {
    DataSchema inputSchema = new DataSchema(new String[]{"string1"}, new ColumnDataType[]{
        ColumnDataType.STRING
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{"starTree"}, new Object[]{"treeStar"}));
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(SqlKind.OTHER, ColumnDataType.BOOLEAN, "startsWith",
            ImmutableList.of(new RexExpression.InputRef(0), new RexExpression.Literal(ColumnDataType.STRING, "star")));
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, startsWith);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    List<Object[]> expectedResult = ImmutableList.of(new Object[]{"starTree"});
    Assert.assertEquals(result.size(), expectedResult.size());
    Assert.assertEquals(result.get(0), expectedResult.get(0));
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Cannot find function "
      + "with name: startsWithError")
  public void shouldThrowOnUnfoundFunction() {
    DataSchema inputSchema = new DataSchema(new String[]{"string1"}, new ColumnDataType[]{
        ColumnDataType.STRING
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{"starTree"}, new Object[]{"treeStar"}));
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(SqlKind.OTHER, ColumnDataType.BOOLEAN, "startsWithError",
            ImmutableList.of(new RexExpression.InputRef(0), new RexExpression.Literal(ColumnDataType.STRING, "star")));
    new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, startsWith);
  }
}
