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
package org.apache.pinot.spi.data;

import java.util.TimeZone;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class DateTimeFormatPatternSpecTest {

  @Test
  public void testValidateFormat() {
    DateTimeFormatPatternSpec dateTimeFormatPatternSpec =
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyy-MM-dd");
    assertEquals(dateTimeFormatPatternSpec.getSdfPattern(), "yyyy-MM-dd");
    assertEquals(dateTimeFormatPatternSpec.getDateTimeZone(), DateTimeZone.UTC);
    assertEquals(dateTimeFormatPatternSpec,
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyy-MM-dd", null));

    dateTimeFormatPatternSpec = new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd tz(CST)");
    assertEquals(dateTimeFormatPatternSpec.getSdfPattern(), "yyyyMMdd");
    assertEquals(dateTimeFormatPatternSpec.getDateTimeZone(), DateTimeZone.forTimeZone(TimeZone.getTimeZone("CST")));
    assertEquals(dateTimeFormatPatternSpec,
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd", "CST"));

    dateTimeFormatPatternSpec =
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd HH tz(GMT+0700)");
    assertEquals(dateTimeFormatPatternSpec.getSdfPattern(), "yyyyMMdd HH");
    assertEquals(dateTimeFormatPatternSpec.getDateTimeZone(),
        DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+0700")));
    assertEquals(dateTimeFormatPatternSpec,
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd HH", "GMT+0700"));

    dateTimeFormatPatternSpec =
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMddHH tz(America/Chicago)");
    assertEquals(dateTimeFormatPatternSpec.getSdfPattern(), "yyyyMMddHH");
    assertEquals(dateTimeFormatPatternSpec.getDateTimeZone(),
        DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Chicago")));
    assertEquals(dateTimeFormatPatternSpec,
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMddHH", "America/Chicago"));

    // Unknown time zone is treated as UTC
    dateTimeFormatPatternSpec = new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd tz(CSEMT)");
    assertEquals(dateTimeFormatPatternSpec.getSdfPattern(), "yyyyMMdd");
    assertEquals(dateTimeFormatPatternSpec.getDateTimeZone(), DateTimeZone.UTC);
    assertEquals(dateTimeFormatPatternSpec,
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd", "CSEMT"));

    dateTimeFormatPatternSpec = new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd tz(GMT+5000)");
    assertEquals(dateTimeFormatPatternSpec.getSdfPattern(), "yyyyMMdd");
    assertEquals(dateTimeFormatPatternSpec.getDateTimeZone(), DateTimeZone.UTC);
    assertEquals(dateTimeFormatPatternSpec,
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd", "GMT+5000"));

    dateTimeFormatPatternSpec =
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd tz(HAHA/Chicago)");
    assertEquals(dateTimeFormatPatternSpec.getSdfPattern(), "yyyyMMdd");
    assertEquals(dateTimeFormatPatternSpec.getDateTimeZone(), DateTimeZone.UTC);
    assertEquals(dateTimeFormatPatternSpec,
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd", "HAHA/Chicago"));

    // Invalid pattern
    assertThrows(IllegalArgumentException.class,
        () -> new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyc-MM-dd"));
    assertThrows(IllegalArgumentException.class,
        () -> new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, "yyyy-MM-dd ff(a)"));
  }
}
