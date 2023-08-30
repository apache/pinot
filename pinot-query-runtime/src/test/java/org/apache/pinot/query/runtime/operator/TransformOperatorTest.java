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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

import static org.apache.calcite.sql.SqlKind.MINUS;
import static org.apache.calcite.sql.SqlKind.PLUS;


public class TransformOperatorTest {
  private AutoCloseable _mocks;

  @Mock
  private MultiStageOperator _upstreamOp;

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
  public void shouldHandleRefTransform() {
    DataSchema upStreamSchema = new DataSchema(new String[]{"intCol", "strCol"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema resultSchema = new DataSchema(new String[]{"inCol", "strCol"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    Mockito.when(_upstreamOp.nextBlock())
        .thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{1, "a"}, new Object[]{2, "b"}));
    // Output column value
    RexExpression.InputRef ref0 = new RexExpression.InputRef(0);
    RexExpression.InputRef ref1 = new RexExpression.InputRef(1);
    TransformOperator op = new TransformOperator(OperatorTestUtil.getDefaultContext(), _upstreamOp, resultSchema,
        ImmutableList.of(ref0, ref1), upStreamSchema);
    TransferableBlock result = op.nextBlock();

    Assert.assertTrue(!result.isErrorBlock());
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "a"}, new Object[]{2, "b"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void shouldHandleLiteralTransform() {
    DataSchema upStreamSchema = new DataSchema(new String[]{"boolCol", "strCol"}, new ColumnDataType[]{
        ColumnDataType.BOOLEAN, ColumnDataType.STRING
    });
    DataSchema resultSchema = new DataSchema(new String[]{"boolCol", "strCol"},
        new ColumnDataType[]{ColumnDataType.BOOLEAN, ColumnDataType.STRING});
    Mockito.when(_upstreamOp.nextBlock())
        .thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{1, "a"}, new Object[]{2, "b"}));
    // Set up literal operands
    RexExpression.Literal boolLiteral = new RexExpression.Literal(ColumnDataType.BOOLEAN, 1);
    RexExpression.Literal strLiteral = new RexExpression.Literal(ColumnDataType.STRING, "str");
    TransformOperator op = new TransformOperator(OperatorTestUtil.getDefaultContext(), _upstreamOp, resultSchema,
        ImmutableList.of(boolLiteral, strLiteral), upStreamSchema);
    TransferableBlock result = op.nextBlock();
    // Literal operands should just output original literals.
    Assert.assertTrue(!result.isErrorBlock());
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "str"}, new Object[]{1, "str"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void shouldHandlePlusMinusFuncTransform() {
    DataSchema upStreamSchema = new DataSchema(new String[]{"doubleCol1", "doubleCol2"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    Mockito.when(_upstreamOp.nextBlock())
        .thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{1.0, 1.0}, new Object[]{2.0, 3.0}));
    // Run a plus and minus function operand on double columns.
    RexExpression.InputRef ref0 = new RexExpression.InputRef(0);
    RexExpression.InputRef ref1 = new RexExpression.InputRef(1);
    List<RexExpression> functionOperands = ImmutableList.of(ref0, ref1);
    RexExpression.FunctionCall plus01 =
        new RexExpression.FunctionCall(PLUS, ColumnDataType.DOUBLE, "plus", functionOperands);
    RexExpression.FunctionCall minus01 =
        new RexExpression.FunctionCall(MINUS, ColumnDataType.DOUBLE, "minus", functionOperands);
    DataSchema resultSchema = new DataSchema(new String[]{"plusR", "minusR"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    TransformOperator op = new TransformOperator(OperatorTestUtil.getDefaultContext(), _upstreamOp, resultSchema,
        ImmutableList.of(plus01, minus01), upStreamSchema);
    TransferableBlock result = op.nextBlock();
    Assert.assertTrue(!result.isErrorBlock());
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{2.0, 0.0}, new Object[]{5.0, -1.0});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void shouldThrowOnTypeMismatchFuncTransform() {
    DataSchema upStreamSchema = new DataSchema(new String[]{"string1", "string2"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    Mockito.when(_upstreamOp.nextBlock())
        .thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{"str1", "str1"}, new Object[]{"str2", "str3"}));
    // Run a plus and minus function operand on string columns.
    RexExpression.InputRef ref0 = new RexExpression.InputRef(0);
    RexExpression.InputRef ref1 = new RexExpression.InputRef(1);
    List<RexExpression> functionOperands = ImmutableList.of(ref0, ref1);
    RexExpression.FunctionCall plus01 =
        new RexExpression.FunctionCall(PLUS, ColumnDataType.DOUBLE, "plus", functionOperands);
    RexExpression.FunctionCall minus01 =
        new RexExpression.FunctionCall(MINUS, ColumnDataType.DOUBLE, "minus", functionOperands);
    DataSchema resultSchema = new DataSchema(new String[]{"plusR", "minusR"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    TransformOperator op = new TransformOperator(OperatorTestUtil.getDefaultContext(), _upstreamOp, resultSchema,
        ImmutableList.of(plus01, minus01), upStreamSchema);

    TransferableBlock result = op.nextBlock();
    Assert.assertTrue(result.isErrorBlock());
    Assert.assertTrue(result.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains("NumberFormatException"));
  }

  @Test
  public void shouldPropagateUpstreamError() {
    DataSchema upStreamSchema = new DataSchema(new String[]{"string1", "string2"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    Mockito.when(_upstreamOp.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("transformError")));
    RexExpression.Literal boolLiteral = new RexExpression.Literal(ColumnDataType.BOOLEAN, 1);
    RexExpression.Literal strLiteral = new RexExpression.Literal(ColumnDataType.STRING, "str");
    DataSchema resultSchema = new DataSchema(new String[]{"inCol", "strCol"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    TransformOperator op = new TransformOperator(OperatorTestUtil.getDefaultContext(), _upstreamOp, resultSchema,
        ImmutableList.of(boolLiteral, strLiteral), upStreamSchema);
    TransferableBlock result = op.nextBlock();
    Assert.assertTrue(result.isErrorBlock());
    Assert.assertTrue(result.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains("transformError"));
  }

  @Test
  public void testNoopBlock() {
    DataSchema upStreamSchema = new DataSchema(new String[]{"string1", "string2"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    Mockito.when(_upstreamOp.nextBlock())
        .thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{"a", "a"}, new Object[]{"b", "b"}))
        .thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{"c", "c"}, new Object[]{"d", "d"}, new Object[]{
            "e", "e"
        }));
    RexExpression.Literal boolLiteral = new RexExpression.Literal(ColumnDataType.BOOLEAN, 1);
    RexExpression.Literal strLiteral = new RexExpression.Literal(ColumnDataType.STRING, "str");
    DataSchema resultSchema = new DataSchema(new String[]{"boolCol", "strCol"},
        new ColumnDataType[]{ColumnDataType.BOOLEAN, ColumnDataType.STRING});
    TransformOperator op = new TransformOperator(OperatorTestUtil.getDefaultContext(), _upstreamOp, resultSchema,
        ImmutableList.of(boolLiteral, strLiteral), upStreamSchema);
    TransferableBlock result = op.nextBlock();
    // First block has two rows
    Assert.assertFalse(result.isErrorBlock());
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "str"}, new Object[]{1, "str"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
    // Second block has one row.
    result = op.nextBlock();
    Assert.assertFalse(result.isErrorBlock());
    resultRows = result.getContainer();
    expectedRows = Arrays.asList(new Object[]{1, "str"}, new Object[]{1, "str"}, new Object[]{1, "str"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
    Assert.assertEquals(resultRows.get(2), expectedRows.get(2));
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*transform operand "
      + "should not be empty.*")
  public void testWrongNumTransform() {
    DataSchema resultSchema = new DataSchema(new String[]{"inCol", "strCol"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    DataSchema upStreamSchema = new DataSchema(new String[]{"string1", "string2"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    new TransformOperator(OperatorTestUtil.getDefaultContext(), _upstreamOp, resultSchema, new ArrayList<>(),
        upStreamSchema);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*doesn't match "
      + "transform operand size.*")
  public void testMismatchedSchemaOperandSize() {
    DataSchema resultSchema = new DataSchema(new String[]{"inCol", "strCol"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    DataSchema upStreamSchema = new DataSchema(new String[]{"string1", "string2"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING
    });
    RexExpression.InputRef ref0 = new RexExpression.InputRef(0);
    new TransformOperator(OperatorTestUtil.getDefaultContext(), _upstreamOp, resultSchema, ImmutableList.of(ref0),
        upStreamSchema);
  }
};
