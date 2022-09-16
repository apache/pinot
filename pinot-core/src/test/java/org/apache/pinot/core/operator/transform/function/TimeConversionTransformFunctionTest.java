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
package org.apache.pinot.core.operator.transform.function;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class TimeConversionTransformFunctionTest extends BaseTransformFunctionTest {

  @Test(dataProvider = "testTimeConversionTransformFunction")
  public void testTimeConversionTransformFunction(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof TimeConversionTransformFunction);
    assertEquals(transformFunction.getName(), TimeConversionTransformFunction.FUNCTION_NAME);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), DataType.LONG);
    assertTrue(resultMetadata.isSingleValue());
    assertFalse(resultMetadata.hasDictionary());
    long[] expectedValues = new long[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = TimeUnit.MILLISECONDS.toDays(_timeValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @DataProvider(name = "testTimeConversionTransformFunction")
  public Object[][] testTimeConversionTransformFunction() {
    return new Object[][]{
        new Object[]{
            String.format("timeConvert(%s,'MILLISECONDS','DAYS')", TIME_COLUMN)
        }, new Object[]{
        String.format(
            "timeConvert(timeConvert(timeConvert(%s,'MILLISECONDS','SECONDS'),'SECONDS','HOURS'),'HOURS','DAYS')",
            TIME_COLUMN)
    }
    };
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{
            String.format("timeConvert(%s,'MILLISECONDS')", TIME_COLUMN)
        }, new Object[]{"timeConvert(5,'MILLISECONDS','DAYS')"}, new Object[]{
        String.format("timeConvert(%s,'MILLISECONDS','DAYS')", INT_MV_COLUMN)
    }, new Object[]{
        String.format("timeConvert(%s,'MILLISECONDS','1:DAYS')", TIME_COLUMN)
    }, new Object[]{
        String.format("timeConvert(%s,%s,'DAYS')", TIME_COLUMN, INT_SV_COLUMN)
    }
    };
  }
}
