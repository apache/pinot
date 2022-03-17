package org.apache.pinot.core.operator.transform.function;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RoundDecimalTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testRoundDecimalTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpressionFromSQL(String.format("round_decimal(%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof RoundDecimalTransformFunction);
    Assert.assertEquals(transformFunction.getName(), RoundDecimalTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];

    expression = RequestContextUtils.getExpressionFromSQL(String.format("round_decimal(%s,2)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof RoundDecimalTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = round(_doubleSVValues[i], 2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpressionFromSQL(String.format("round_decimal(%s, -2)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof RoundDecimalTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = round(_doubleSVValues[i], -2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpressionFromSQL(String.format("round_decimal(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof RoundDecimalTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = round(_doubleSVValues[i], 0);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  public Double round(double a, int b) {
    return BigDecimal.valueOf(a).setScale(b, RoundingMode.HALF_UP).doubleValue();
  }
}
