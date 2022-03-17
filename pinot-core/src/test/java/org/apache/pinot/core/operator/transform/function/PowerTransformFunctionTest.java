package org.apache.pinot.core.operator.transform.function;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PowerTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testPowerTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpressionFromSQL(String.format("power(%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    Assert.assertEquals(transformFunction.getName(), PowerTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow((double) _intSVValues[i] , (double) _longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpressionFromSQL(String.format("power(%s,%s)", LONG_SV_COLUMN, FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow((double) _longSVValues[i] , (double) _floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        RequestContextUtils.getExpressionFromSQL(String.format("power(%s,%s)", FLOAT_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow((double) _floatSVValues[i] , _doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        RequestContextUtils.getExpressionFromSQL(String.format("power(%s,%s)", DOUBLE_SV_COLUMN, STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow(_doubleSVValues[i] , Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpressionFromSQL(String.format("power(%s,%s)", STRING_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow(Double.parseDouble(_stringSVValues[i]) , (double) _intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }
}
