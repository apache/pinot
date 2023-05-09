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
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.spi.data.FieldSpec;
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
    RexExpression booleanLiteral = new RexExpression.Literal(FieldSpec.DataType.BOOLEAN, true);
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.BOOLEAN
    });
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, booleanLiteral);
    TransferableBlock errorBlock = op.getNextBlock();
    Assert.assertTrue(errorBlock.isErrorBlock());
    DataBlock error = errorBlock.getDataBlock();
    Assert.assertTrue(error.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains("filterError"));
  }

  @Test
  public void shouldPropagateUpstreamEOS() {
    RexExpression booleanLiteral = new RexExpression.Literal(FieldSpec.DataType.BOOLEAN, true);

    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, booleanLiteral);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertTrue(dataBlock.isEndOfStreamBlock());
  }

  @Test
  public void shouldPropagateUpstreamNoop() {
    RexExpression booleanLiteral = new RexExpression.Literal(FieldSpec.DataType.BOOLEAN, true);

    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock()).thenReturn(TransferableBlockUtils.getNoOpTransferableBlock());
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, booleanLiteral);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertTrue(dataBlock.isNoOpBlock());
  }

  @Test
  public void shouldHandleTrueBooleanLiteralFilter() {
    RexExpression booleanLiteral = new RexExpression.Literal(FieldSpec.DataType.BOOLEAN, true);

    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT
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
    RexExpression booleanLiteral = new RexExpression.Literal(FieldSpec.DataType.BOOLEAN, false);

    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT
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

  @Test
  public void shouldThrowOnNonBooleanTypeBooleanLiteral() {
    RexExpression booleanLiteral = new RexExpression.Literal(FieldSpec.DataType.STRING, "false");
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1}, new Object[]{2}));
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, booleanLiteral);
    TransferableBlock errorBlock = op.getNextBlock();
    Assert.assertTrue(errorBlock.isErrorBlock());
    DataBlock data = errorBlock.getDataBlock();
    Assert.assertTrue(data.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains("cast"));
  }

  @Test
  public void shouldThrowOnNonBooleanTypeInputRef() {
    RexExpression ref0 = new RexExpression.InputRef(0);
    DataSchema inputSchema = new DataSchema(new String[]{"intCol"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1}, new Object[]{2}));
    FilterOperator op = new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, ref0);
    TransferableBlock errorBlock = op.getNextBlock();
    Assert.assertTrue(errorBlock.isErrorBlock());
    DataBlock data = errorBlock.getDataBlock();
    Assert.assertTrue(data.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains("cast"));
  }

  @Test
  public void shouldHandleBooleanInputRef() {
    RexExpression ref1 = new RexExpression.InputRef(1);
    DataSchema inputSchema = new DataSchema(new String[]{"intCol", "boolCol"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.BOOLEAN
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1, true}, new Object[]{2, false}));
    FilterOperator op = new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, ref1);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0)[0], 1);
    Assert.assertEquals(result.get(0)[1], true);
  }

  @Test
  public void shouldHandleAndFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol0", "boolCol1"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.BOOLEAN
    });
    Mockito.when(_upstreamOperator.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{true, true}, new Object[]{false, false},
            new Object[]{true, false}));
    RexExpression.FunctionCall andCall = new RexExpression.FunctionCall(SqlKind.AND, FieldSpec.DataType.BOOLEAN, "AND",
        ImmutableList.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));

    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, andCall);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0)[0], true);
    Assert.assertEquals(result.get(0)[1], true);
  }

  @Test
  public void shouldHandleOrFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol0", "boolCol1"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.BOOLEAN
    });
    Mockito.when(_upstreamOperator.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{true, true}, new Object[]{false, false},
            new Object[]{true, false}));
    RexExpression.FunctionCall orCall = new RexExpression.FunctionCall(SqlKind.OR, FieldSpec.DataType.BOOLEAN, "OR",
        ImmutableList.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));

    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, orCall);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0)[0], true);
    Assert.assertEquals(result.get(0)[1], true);
    Assert.assertEquals(result.get(1)[0], true);
    Assert.assertEquals(result.get(1)[1], false);
  }

  @Test
  public void shouldHandleNotFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"boolCol0", "boolCol1"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.BOOLEAN
    });
    Mockito.when(_upstreamOperator.nextBlock()).thenReturn(
        OperatorTestUtil.block(inputSchema, new Object[]{true, true}, new Object[]{false, false},
            new Object[]{true, false}));
    RexExpression.FunctionCall notCall = new RexExpression.FunctionCall(SqlKind.NOT, FieldSpec.DataType.BOOLEAN, "NOT",
        ImmutableList.of(new RexExpression.InputRef(0)));

    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, notCall);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0)[0], false);
    Assert.assertEquals(result.get(0)[1], false);
  }

  @Test
  public void shouldHandleGreaterThanFilter() {
    DataSchema inputSchema = new DataSchema(new String[]{"int0", "int1"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1, 2}, new Object[]{3, 2}, new Object[]{1, 1}));
    RexExpression.FunctionCall greaterThan =
        new RexExpression.FunctionCall(SqlKind.GREATER_THAN, FieldSpec.DataType.BOOLEAN, "greaterThan",
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
    DataSchema inputSchema = new DataSchema(new String[]{"string1"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{"starTree"}, new Object[]{"treeStar"}));
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(SqlKind.OTHER, FieldSpec.DataType.BOOLEAN, "startsWith",
            ImmutableList.of(new RexExpression.InputRef(0),
                new RexExpression.Literal(FieldSpec.DataType.STRING, "star")));
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, startsWith);
    TransferableBlock dataBlock = op.getNextBlock();
    Assert.assertFalse(dataBlock.isErrorBlock());
    List<Object[]> result = dataBlock.getContainer();
    List<Object[]> expectedResult = ImmutableList.of(new Object[]{"starTree"});
    Assert.assertEquals(result.size(), expectedResult.size());
    Assert.assertEquals(result.get(0), expectedResult.get(0));
  }

  @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = ".*Cannot find function "
      + "with Name: startsWithError.*")
  public void shouldThrowOnUnfoundFunction() {
    DataSchema inputSchema = new DataSchema(new String[]{"string1"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_upstreamOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{"starTree"}, new Object[]{"treeStar"}));
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(SqlKind.OTHER, FieldSpec.DataType.BOOLEAN, "startsWithError",
            ImmutableList.of(new RexExpression.InputRef(0),
                new RexExpression.Literal(FieldSpec.DataType.STRING, "star")));
    FilterOperator op =
        new FilterOperator(OperatorTestUtil.getDefaultContext(), _upstreamOperator, inputSchema, startsWith);
  }
}
