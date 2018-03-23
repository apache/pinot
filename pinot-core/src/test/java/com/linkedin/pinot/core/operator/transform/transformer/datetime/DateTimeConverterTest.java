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
package com.linkedin.pinot.core.operator.transform.transformer.datetime;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DateTimeConverterTest {

  @SuppressWarnings("unchecked")
  @Test(dataProvider = "testDateTimeConversion")
  public void testDateTimeConversion(String inputFormat, String outputFormat, String outputGranularity,
      Object inputValue, Object outputValue, Object expectedValue) {
    BaseDateTimeTransformer converter =
        DateTimeTransformerFactory.getDateTimeTransformer(inputFormat, outputFormat, outputGranularity);
    converter.transform(inputValue, outputValue, 1);
    Assert.assertEquals(outputValue, expectedValue);
  }

  @DataProvider(name = "testDateTimeConversion")
  public Object[][] provideTestData() {
    return new Object[][]{
        new Object[]{"1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "15:MINUTES", new long[]{1505898300000L}, new long[1], new long[]{1505898000000L}},
        new Object[]{"1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", new long[]{1505898000000L}, new long[1], new long[]{1505898000000L}},
        new Object[]{"1:MILLISECONDS:EPOCH", "1:HOURS:EPOCH", "1:HOURS", new long[]{1505902560000L}, new long[1], new long[]{418306L}},
        new Object[]{"1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS", new long[]{1505898300000L}, new String[1], new String[]{"20170920"}},
        new Object[]{"1:YEARS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:MILLISECONDS:EPOCH", "1:DAYS", new String[]{"20170601"}, new long[1], new long[]{1496275200000L}},
        new Object[]{"1:MINUTES:SIMPLE_DATE_FORMAT:M/d/yyyy hh a", "1:MILLISECONDS:EPOCH", "5:DAYS", new String[]{"8/7/2017 1 AM"}, new long[1], new long[]{1502064000000L}},
        new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy hh:mm a", "1:MONTHS:SIMPLE_DATE_FORMAT:yyyyMMddHHmm", "1:DAYS", new String[]{"1/2/1994 06:28 AM"}, new String[1], new String[]{"199401020628"}},
        new Object[]{"1:SECONDS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:MILLISECONDS:EPOCH", "1:HOURS", new String[]{"12/27/2016 11:20:00 PM"}, new long[1], new long[]{1482879600000L}},
        new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", new String[]{"8/7/2017 12:45:50 AM"}, new long[1], new long[]{1502066750000L}},
        new Object[]{"5:MINUTES:EPOCH", "1:MILLISECONDS:EPOCH", "1:HOURS", new long[]{5019675L}, new long[1], new long[]{1505901600000L}},
        new Object[]{"5:MINUTES:EPOCH", "1:HOURS:EPOCH", "1:HOURS", new long[]{5019661L}, new long[1], new long[]{418305L}},
        new Object[]{"1:MILLISECONDS:EPOCH", "1:WEEKS:EPOCH", "1:MILLISECONDS", new long[]{1505898000000L}, new long[1], new long[]{2489L}}
    };
  }
}
