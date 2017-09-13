/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.transform;

import com.linkedin.pinot.core.operator.docvalsets.ConstantBlockValSet;
import com.linkedin.pinot.core.operator.transform.function.TimeConversionTransform;
import com.linkedin.pinot.core.operator.transform.function.time.converter.TimeConverterFactory;
import com.linkedin.pinot.core.operator.transform.function.time.converter.TimeUnitConverter;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link TimeConversionTransform}.
 */
public class TimeConversionTransformTest {

  private static final int NUM_ROWS = 10001;
  private static final DateTime EPOCH_START_DATE = new DateTime(0L, DateTimeZone.UTC);

  @Test
  public void test() {
    long[] input = new long[NUM_ROWS];
    long[] expected = new long[NUM_ROWS];

    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < NUM_ROWS; i++) {
      input[i] = random.nextLong() % seed;
      expected[i] = TimeUnit.DAYS.convert(input[i], TimeUnit.MILLISECONDS);
    }

    TransformTestUtils.TestBlockValSet inputTimes = new TransformTestUtils.TestBlockValSet(input, NUM_ROWS);
    ConstantBlockValSet inputTimeUnit = new ConstantBlockValSet("MILLISECONDS", NUM_ROWS);
    ConstantBlockValSet outputTimeUnit = new ConstantBlockValSet("DAYS", NUM_ROWS);

    TimeConversionTransform function = new TimeConversionTransform();
    long[] actual = function.transform(NUM_ROWS, inputTimes, inputTimeUnit, outputTimeUnit);

    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(actual[i], expected[i]);
    }
  }

  @Test
  public void testCustom() {
    long inputs[] = new long[NUM_ROWS];
    long expectedWeeks[] = new long[NUM_ROWS];
    long expectedMonths[] = new long[NUM_ROWS];
    long expectedYears[] = new long[NUM_ROWS];


    long seed = System.nanoTime();
    Random random = new Random(seed);

    MutableDateTime dateTime = new MutableDateTime(0L, DateTimeZone.UTC);

    for (int i = 0; i < NUM_ROWS; i++) {
      inputs[i] = Math.abs(random.nextLong() % System.currentTimeMillis());

      dateTime.setDate(inputs[i]);
      expectedWeeks[i] = Weeks.weeksBetween(EPOCH_START_DATE, dateTime).getWeeks();
      expectedMonths[i] = Months.monthsBetween(EPOCH_START_DATE, dateTime).getMonths();
      expectedYears[i] = Years.yearsBetween(EPOCH_START_DATE, dateTime).getYears();
    }

    // Test for WEEKS conversion.
    long output[] = new long[NUM_ROWS];
    TimeUnitConverter weekConverter = TimeConverterFactory.getTimeConverter("wEeKs");
    weekConverter.convert(inputs, TimeUnit.MILLISECONDS, NUM_ROWS, output);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(output[i], expectedWeeks[i]);
    }

    // Test for MONTHS conversion.
    TimeUnitConverter monthConverter = TimeConverterFactory.getTimeConverter("mOnThS");
    monthConverter.convert(inputs, TimeUnit.MILLISECONDS, NUM_ROWS, output);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(output[i], expectedMonths[i]);
    }

    // Test for YEARS conversion.
    TimeUnitConverter yearConverter = TimeConverterFactory.getTimeConverter("yEaRs");
    yearConverter.convert(inputs, TimeUnit.MILLISECONDS, NUM_ROWS, output);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(output[i], expectedYears[i]);
    }
  }
}
