package org.apache.pinot.core.operator.transform.function;

import java.util.function.LongToIntFunction;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ExtractTransformFunctionTest extends BaseTransformFunctionTest {

  @DataProvider
  public static Object[][] testCases() {
    return new Object[][] {
        {"year", (LongToIntFunction) DateTimeFunctions::year},
        {"month", (LongToIntFunction) DateTimeFunctions::month},
        {"day", (LongToIntFunction) DateTimeFunctions::dayOfMonth},
        {"hour", (LongToIntFunction) DateTimeFunctions::hour},
        {"minute", (LongToIntFunction) DateTimeFunctions::minute},
        {"second", (LongToIntFunction) DateTimeFunctions::second},
        // TODO: Need to add timezone_hour and timezone_minute
//      "timezone_hour",
//      "timezone_minute",
    };
  }
  
  @Test(dataProvider = "testCases")
  public void testExtractTransformFunction(String field, LongToIntFunction expected) {
    // NOTE: functionality of ExtractTransformFunction is covered in ExtractTransformFunctionTest  
    // SELECT EXTRACT(YEAR FROM '2017-10-10')

    ExpressionContext expression = RequestContextUtils.getExpression(String.format("extract(%s FROM %s)", field, TIMESTAMP_COLUMN));
    
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExtractTransformFunction);
    int[] value = transformFunction.transformToIntValuesSV(_projectionBlock);
    for(int i=0; i< _projectionBlock.getNumDocs(); ++i) {
      assertEquals(value[i], expected.applyAsInt(_timeValues[i]));
    }
  }
}
