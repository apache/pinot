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


public class DateTimeTransformFunctionTest extends BaseTransformFunctionTest {

  @DataProvider
  public static Object[][] testCasesUTC() {
    return new Object[][]{
        {"year", (LongToIntFunction) DateTimeFunctions::year, DateTimeTransformFunction.Year.class},
        {"yearOfWeek", (LongToIntFunction) DateTimeFunctions::yearOfWeek, DateTimeTransformFunction.YearOfWeek.class},
        {"yow", (LongToIntFunction) DateTimeFunctions::yearOfWeek, DateTimeTransformFunction.YearOfWeek.class},
        {"month", (LongToIntFunction) DateTimeFunctions::monthOfYear, DateTimeTransformFunction.Month.class},
        {"week", (LongToIntFunction) DateTimeFunctions::weekOfYear, DateTimeTransformFunction.WeekOfYear.class},
        {"weekOfYear", (LongToIntFunction) DateTimeFunctions::weekOfYear, DateTimeTransformFunction.WeekOfYear.class},
        {"quarter", (LongToIntFunction) DateTimeFunctions::quarter, DateTimeTransformFunction.Quarter.class},
        {"dayOfWeek", (LongToIntFunction) DateTimeFunctions::dayOfWeek, DateTimeTransformFunction.DayOfWeek.class},
        {"dow", (LongToIntFunction) DateTimeFunctions::dayOfWeek, DateTimeTransformFunction.DayOfWeek.class},
        {"dayOfYear", (LongToIntFunction) DateTimeFunctions::dayOfYear, DateTimeTransformFunction.DayOfYear.class},
        {"doy", (LongToIntFunction) DateTimeFunctions::dayOfYear, DateTimeTransformFunction.DayOfYear.class},
        {"dayOfMonth", (LongToIntFunction) DateTimeFunctions::dayOfMonth, DateTimeTransformFunction.DayOfMonth.class},
        {"day", (LongToIntFunction) DateTimeFunctions::dayOfMonth, DateTimeTransformFunction.DayOfMonth.class},
        {"hour", (LongToIntFunction) DateTimeFunctions::hour, DateTimeTransformFunction.Hour.class},
        {"minute", (LongToIntFunction) DateTimeFunctions::minute, DateTimeTransformFunction.Minute.class},
        {"millisecond", (LongToIntFunction) DateTimeFunctions::millisecond,
            DateTimeTransformFunction.Millisecond.class},
    };
  }

  @DataProvider
  public static Object[][] testCasesZoned() {
    return new Object[][]{
        {"year", (ZonedTimeFunction) DateTimeFunctions::year, DateTimeTransformFunction.Year.class},
        {"yearOfWeek", (ZonedTimeFunction) DateTimeFunctions::yearOfWeek, DateTimeTransformFunction.YearOfWeek.class},
        {"yow", (ZonedTimeFunction) DateTimeFunctions::yearOfWeek, DateTimeTransformFunction.YearOfWeek.class},
        {"month", (ZonedTimeFunction) DateTimeFunctions::monthOfYear, DateTimeTransformFunction.Month.class},
        {"week", (ZonedTimeFunction) DateTimeFunctions::weekOfYear, DateTimeTransformFunction.WeekOfYear.class},
        {"weekOfYear", (ZonedTimeFunction) DateTimeFunctions::weekOfYear, DateTimeTransformFunction.WeekOfYear.class},
        {"quarter", (ZonedTimeFunction) DateTimeFunctions::quarter, DateTimeTransformFunction.Quarter.class},
        {"dayOfWeek", (ZonedTimeFunction) DateTimeFunctions::dayOfWeek, DateTimeTransformFunction.DayOfWeek.class},
        {"dow", (ZonedTimeFunction) DateTimeFunctions::dayOfWeek, DateTimeTransformFunction.DayOfWeek.class},
        {"dayOfYear", (ZonedTimeFunction) DateTimeFunctions::dayOfYear, DateTimeTransformFunction.DayOfYear.class},
        {"doy", (ZonedTimeFunction) DateTimeFunctions::dayOfYear, DateTimeTransformFunction.DayOfYear.class},
        {"dayOfMonth", (ZonedTimeFunction) DateTimeFunctions::dayOfMonth, DateTimeTransformFunction.DayOfMonth.class},
        {"day", (ZonedTimeFunction) DateTimeFunctions::dayOfMonth, DateTimeTransformFunction.DayOfMonth.class},
        {"hour", (ZonedTimeFunction) DateTimeFunctions::hour, DateTimeTransformFunction.Hour.class},
        {"minute", (ZonedTimeFunction) DateTimeFunctions::minute, DateTimeTransformFunction.Minute.class},
        {"millisecond", (ZonedTimeFunction) DateTimeFunctions::millisecond,
            DateTimeTransformFunction.Millisecond.class},
    };
  }

  @Test(dataProvider = "testCasesUTC")
  public void testUTC(String function, LongToIntFunction expected, Class<? extends TransformFunction> expectedClass) {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("%s(%s)", function, TIMESTAMP_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(expectedClass.isInstance(transformFunction));
    int[] values = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < _projectionBlock.getNumDocs(); i++) {
      assertEquals(values[i], expected.applyAsInt(_timeValues[i]));
    }
  }

  @Test(dataProvider = "testCasesUTC")
  public void testUTCNullColumn(String function, LongToIntFunction expected,
      Class<? extends TransformFunction> expectedClass) {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("%s(%s)", function, TIMESTAMP_COLUMN_NULL));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(expectedClass.isInstance(transformFunction));
    int[] values = transformFunction.transformToIntValuesSV(_projectionBlock);
    RoaringBitmap nullBitmap = transformFunction.getNullBitmap(_projectionBlock);

    for (int i = 0; i < _projectionBlock.getNumDocs(); i++) {
      if (i % 2 == 0) {
        assertEquals(values[i], expected.applyAsInt(_timeValues[i]));
      } else {
        assertTrue(nullBitmap.contains(i));
      }
    }
  }

  @Test(dataProvider = "testCasesZoned")
  public void testZoned(String function, ZonedTimeFunction expected, Class<? extends TransformFunction> expectedClass) {
    for (String zone : new String[]{"Europe/Berlin", "America/New_York", "Asia/Katmandu"}) {
      ExpressionContext expression =
          RequestContextUtils.getExpression(String.format("%s(%s, '%s')", function, TIMESTAMP_COLUMN, zone));
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
      Assert.assertTrue(expectedClass.isInstance(transformFunction));
      int[] values = transformFunction.transformToIntValuesSV(_projectionBlock);
      for (int i = 0; i < _projectionBlock.getNumDocs(); i++) {
        assertEquals(values[i], expected.apply(_timeValues[i], zone));
      }
    }
  }

  @FunctionalInterface
  public interface ZonedTimeFunction {
    int apply(long millis, String zone);
  }
}
