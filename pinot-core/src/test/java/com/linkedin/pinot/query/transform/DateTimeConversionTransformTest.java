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
import com.linkedin.pinot.core.operator.transform.function.DateTimeConversionTransform;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit test for {@link DateTimeConversionTransform}.
 * Checks if the bucketing is right, and the final format is right
 */
public class DateTimeConversionTransformTest {

  private static final int NUM_ROWS = 3;

  //Test conversion of a dateTimeColumn value from a format to millis
  @Test(dataProvider = "testDateTimeConversionTransformDataProvider")
  public void testDateTimeConversionTransform(String inputFormat, String outputFormat, String outputGranularity,
      long[] input, long[] expected) {

    TransformTestUtils.TestBlockValSet inputBlockSet = new TransformTestUtils.TestBlockValSet(input, NUM_ROWS);
    ConstantBlockValSet inputFormatBlockSet = new ConstantBlockValSet(inputFormat, NUM_ROWS);
    ConstantBlockValSet outputFormatBlockSet = new ConstantBlockValSet(outputFormat, NUM_ROWS);
    ConstantBlockValSet outputGranularityBlockSet = new ConstantBlockValSet(outputGranularity, NUM_ROWS);

    DateTimeConversionTransform function = new DateTimeConversionTransform();
    long[] actual = function.transform(NUM_ROWS, inputBlockSet, inputFormatBlockSet, outputFormatBlockSet, outputGranularityBlockSet);

    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(actual[i], expected[i]);
    }
  }

  @DataProvider(name = "testDateTimeConversionTransformDataProvider")
  public Object[][] providetestDateTimeConversionTransformData() {
    long[] input = null;
    long[] expected = null;

    List<Object[]> entries = new ArrayList<>();

    input = new long[NUM_ROWS];
    expected = new long[NUM_ROWS];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505898300000L /* 20170920T02:05:00 */;
    input[2] = 1505898960000L /* 20170920T02:16:00 */;
    expected[0] = 1505898000000L /* 20170920T02:00:00 */;
    expected[1] = 1505898000000L; /* 20170920T02:00:00 */
    expected[2] = 1505898900000L; /* 20170920T02:15:00 */
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "15:MINUTES", input, expected
    });

    input = new long[NUM_ROWS];
    expected = new long[NUM_ROWS];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505898300000L /* 20170920T02:05:00 */;
    input[2] = 1505898960000L /* 20170920T02:16:00 */;
    expected[0] = 1505898000000L /* 20170920T02:00:00 */;
    expected[1] = 1505898300000L; /* 20170920T02:05:00 */
    expected[2] = 1505898960000L; /* 20170920T02:16:00 */
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", input, expected
    });

    input = new long[NUM_ROWS];
    expected = new long[NUM_ROWS];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505898300000L /* 20170920T02:05:00 */;
    input[2] = 1505902560000L /* 20170920T03:16:00 */;
    expected[0] = 418305L /* 20170920T02:00:00 */;
    expected[1] = 418305L; /* 20170920T02:00:00 */
    expected[2] = 418306L; /* 20170920T02:15:00 */
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:HOURS:EPOCH", "1:HOURS", input, expected
    });

    input = new long[NUM_ROWS];
    expected = new long[NUM_ROWS];
    input[0] = 1505890800000L; /* 20170920T00:00:00 */
    input[1] = 1505898300000L; /* 20170920T02:05:00 */
    input[2] = 1505985360000L; /* 20170921T02:16:00 */
    expected[0] = 20170920L; /* 20170920T00:00:00 */
    expected[1] = 20170920L; /* 20170920T00:00:00 */
    expected[2] = 20170921L; /* 20170921T00:00:00 */
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS", input, expected
    });


    input = new long[NUM_ROWS];
    expected = new long[NUM_ROWS];
    input[0] = 5019660L /* 20170920T02:00:00 */;
    input[1] = 5019661L /* 20170920T02:05:00 */;
    input[2] = 5019675L /* 20170920T03:15:00 */;
    expected[0] = 1505898000000L /* 20170920T02:00:00 */;
    expected[1] = 1505898000000L; /* 20170920T02:00:00 */
    expected[2] = 1505901600000L; /* 20170920T03:00:00 */
    entries.add(new Object[] {
        "5:MINUTES:EPOCH", "1:MILLISECONDS:EPOCH", "1:HOURS", input, expected
    });

    input = new long[NUM_ROWS];
    expected = new long[NUM_ROWS];
    input[0] = 5019660L /* 20170920T02:00:00 */;
    input[1] = 5019661L /* 20170920T02:05:00 */;
    input[2] = 5019675L /* 20170920T03:15:00 */;
    expected[0] = 418305L /* 20170920T02:00:00 */;
    expected[1] = 418305L; /* 20170920T02:00:00 */
    expected[2] = 418306L; /* 20170920T03:00:00 */
    entries.add(new Object[] {
        "5:MINUTES:EPOCH", "1:HOURS:EPOCH", "1:HOURS", input, expected
    });

    input = new long[NUM_ROWS];
    expected = new long[NUM_ROWS];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505199600000L /* 20170912T00:00:00 */;
    input[2] = 1504257300000L /* 20170901T00:20:00 */;
    expected[0] = 2489L;
    expected[1] = 2488L;
    expected[2] = 2487L;
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:WEEKS:EPOCH", "1:MILLISECONDS", input, expected
    });
    return entries.toArray(new Object[entries.size()][]);
  }

}
