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

import java.util.function.LongToIntFunction;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ExtractTransformFunctionTest extends BaseTransformFunctionTest {

  @DataProvider
  public static Object[][] testCases() {
    return new Object[][]{
        //@formatter:off
        {"year", (LongToIntFunction) DateTimeFunctions::year},
        {"month", (LongToIntFunction) DateTimeFunctions::monthOfYear},
        {"day", (LongToIntFunction) DateTimeFunctions::dayOfMonth},
        {"hour", (LongToIntFunction) DateTimeFunctions::hour},
        {"minute", (LongToIntFunction) DateTimeFunctions::minute},
        {"second", (LongToIntFunction) DateTimeFunctions::second},
        // TODO: Need to add timezone_hour and timezone_minute
//      "timezone_hour",
//      "timezone_minute",
        //@formatter:on
    };
  }

  @Test(dataProvider = "testCases")
  public void testExtractTransformFunction(String field, LongToIntFunction expected) {
    // NOTE: functionality of ExtractTransformFunction is covered in ExtractTransformFunctionTest
    // SELECT EXTRACT(YEAR FROM '2017-10-10')

    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("extract(%s FROM %s)", field, TIMESTAMP_COLUMN));

    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExtractTransformFunction);
    int[] value = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < _projectionBlock.getNumDocs(); i++) {
      assertEquals(value[i], expected.applyAsInt(_timeValues[i]));
    }
  }

  @Test(dataProvider = "testCases")
  public void testExtractTransformFunctionNull(String field, LongToIntFunction expected) {
    // NOTE: functionality of ExtractTransformFunction is covered in ExtractTransformFunctionTest
    // SELECT EXTRACT(YEAR FROM '2017-10-10')
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("extract(%s FROM %s)", field, TIMESTAMP_COLUMN_NULL));

    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExtractTransformFunction);
    int[] values = transformFunction.transformToIntValuesSV(_projectionBlock);
    RoaringBitmap nullBitmap = transformFunction.getNullBitmap(_projectionBlock);
    for (int i = 0; i < _projectionBlock.getNumDocs(); i++) {
      if (isNullRow(i)) {
        assertTrue(nullBitmap.contains(i));
      } else {
        assertEquals(values[i], expected.applyAsInt(_timeValues[i]));
      }
    }
  }
}
