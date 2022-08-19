package org.apache.pinot.core.operator.transform.function;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ExtractTransformFunctionTest extends BaseTransformFunctionTest {

  @DataProvider
  public static Object[] testCases() {
    return new Object[] {
      "year", 
      "month",
      "day", 
      "hour",
      "minute",
      "second", 
//      "timezone_hour",
//      "timezone_minute",
    };
  }
  
  @Test(dataProvider = "testCases")
  public void testExtractTransformFunction(String field) {
    // NOTE: functionality of ExtractTransformFunction is covered in ExtractTransformFunctionTest  
    // SELECT EXTRACT(YEAR FROM '2017-10-10')

    ExpressionContext expression = RequestContextUtils.getExpression(String.format("extract(%s FROM '2017-10-10 11:11:11')", field));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExtractTransformFunction);
    String[] value = transformFunction.transformToStringValuesSV(_projectionBlock);
    for(int i=0; i< _projectionBlock.getNumDocs(); ++i) {
      switch (field) {
        case "year":
          assertEquals(value[i], _timeValues[i]);
          break; 
        default:
      }
    }
  }
}
