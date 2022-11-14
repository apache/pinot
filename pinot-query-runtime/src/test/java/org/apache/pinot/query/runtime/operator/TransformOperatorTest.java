package org.apache.pinot.query.runtime.operator;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.spi.data.FieldSpec;
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
  private Operator<TransferableBlock> _upstreamOp;

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
  public void testRefTransform() {
    RexExpression.InputRef ref0 = new RexExpression.InputRef(0);
    RexExpression.InputRef ref1 = new RexExpression.InputRef(1);
    DataSchema upStreamSchema = new DataSchema(new String[]{"intCol", "strCol"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema resultSchema = new DataSchema(new String[]{"inCol", "strCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
    Mockito.when(_upstreamOp.nextBlock())
        .thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{1, "a"}, new Object[]{2, "b"}));
    TransformOperator op = new TransformOperator(_upstreamOp, resultSchema, ImmutableList.of(ref0, ref1), upStreamSchema);
    TransferableBlock result = op.nextBlock();
    Assert.assertTrue(!result.isErrorBlock());
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "a"}, new Object[]{2, "b"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void testLiteralTransform() {
    RexExpression.Literal boolLiteral = new RexExpression.Literal(FieldSpec.DataType.BOOLEAN, true);
    RexExpression.Literal strLiteral = new RexExpression.Literal(FieldSpec.DataType.STRING, "str");
    DataSchema upStreamSchema = new DataSchema(new String[]{"boolCol", "strCol"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.STRING
    });
    DataSchema resultSchema = new DataSchema(new String[]{"boolCol", "strCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.STRING});
    Mockito.when(_upstreamOp.nextBlock())
        .thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{1, "a"}, new Object[]{2, "b"}));
    TransformOperator op =
        new TransformOperator(_upstreamOp, resultSchema, ImmutableList.of(boolLiteral, strLiteral), upStreamSchema);
    TransferableBlock result = op.nextBlock();
    Assert.assertTrue(!result.isErrorBlock());
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{true, "str"}, new Object[]{true, "str"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void testPlusMinusFuncTransform() {
    RexExpression.InputRef ref0 = new RexExpression.InputRef(0);
    RexExpression.InputRef ref1 = new RexExpression.InputRef(1);
    List<RexExpression> functionOperands = ImmutableList.of(ref0, ref1);
    RexExpression.FunctionCall plus01 = new RexExpression.FunctionCall(PLUS, FieldSpec.DataType.DOUBLE, "plus",
        functionOperands );
    RexExpression.FunctionCall minus01 = new RexExpression.FunctionCall(MINUS, FieldSpec.DataType.DOUBLE, "minus",
        functionOperands );
    DataSchema upStreamSchema = new DataSchema(new String[]{"doubleCol1", "doubleCol2"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE
    });
    Mockito.when(_upstreamOp.nextBlock())
        .thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{1.0, 1.0}, new Object[]{2.0, 3.0}));
    DataSchema resultSchema = new DataSchema(new String[]{"plusR", "minusR"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    TransformOperator op =
        new TransformOperator(_upstreamOp, resultSchema, ImmutableList.of(plus01, minus01), upStreamSchema);
    TransferableBlock result = op.nextBlock();
    Assert.assertTrue(!result.isErrorBlock());
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{2.0, 0.0}, new Object[]{5.0, -1.0});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void testTypeMismatchFuncTransform() {
    RexExpression.InputRef ref0 = new RexExpression.InputRef(0);
    RexExpression.InputRef ref1 = new RexExpression.InputRef(1);
    List<RexExpression> functionOperands = ImmutableList.of(ref0, ref1);
    RexExpression.FunctionCall plus01 = new RexExpression.FunctionCall(PLUS, FieldSpec.DataType.DOUBLE, "plus",
        functionOperands );
    RexExpression.FunctionCall minus01 = new RexExpression.FunctionCall(MINUS, FieldSpec.DataType.DOUBLE, "minus",
        functionOperands );
    DataSchema upStreamSchema = new DataSchema(new String[]{"string1", "string2"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_upstreamOp.nextBlock())
        .thenReturn(OperatorTestUtil.block(upStreamSchema, new Object[]{"1.0", "1.0"}, new Object[]{"2.0", "3.0"}));
    DataSchema resultSchema = new DataSchema(new String[]{"plusR", "minusR"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    TransformOperator op =
        new TransformOperator(_upstreamOp, resultSchema, ImmutableList.of(plus01, minus01), upStreamSchema);
    TransferableBlock result = op.nextBlock();
    Assert.assertTrue(result.isErrorBlock());
    DataBlock data = result.getDataBlock();
    //Assert.assertTrue(data.getExceptions().get().matches("ArithmeticFunctions.plus(double,double)"));
    //result.getDataBlock()
  }
};
