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
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


public class DateTimeFormatSpecTest {

  @Test
  public void testDateTimeFormatSpec() {
    DateTimeFormatSpec dateTimeFormatSpec = new DateTimeFormatSpec("5:DAYS:EPOCH");
    assertEquals(dateTimeFormatSpec.getTimeFormat(), DateTimeFieldSpec.TimeFormat.EPOCH);
    assertEquals(dateTimeFormatSpec.getColumnSize(), 5);
    assertEquals(dateTimeFormatSpec.getColumnUnit(), TimeUnit.DAYS);
    assertEquals(dateTimeFormatSpec.getColumnDateTimeTransformUnit(),
        DateTimeFormatUnitSpec.DateTimeTransformUnit.DAYS);
    assertNull(dateTimeFormatSpec.getSDFPattern());

    assertEquals(new DateTimeFormatSpec("EPOCH|DAYS|5"), dateTimeFormatSpec);

    dateTimeFormatSpec = new DateTimeFormatSpec("1:DAYS:TIMESTAMP");
    assertEquals(dateTimeFormatSpec.getTimeFormat(), DateTimeFieldSpec.TimeFormat.TIMESTAMP);
    assertEquals(dateTimeFormatSpec.getColumnSize(), 1);
    assertEquals(dateTimeFormatSpec.getColumnUnit(), TimeUnit.MILLISECONDS);
    assertEquals(dateTimeFormatSpec.getColumnDateTimeTransformUnit(),
        DateTimeFormatUnitSpec.DateTimeTransformUnit.MILLISECONDS);
    assertNull(dateTimeFormatSpec.getSDFPattern());

    assertEquals(new DateTimeFormatSpec("TIMESTAMP"), dateTimeFormatSpec);

    dateTimeFormatSpec = new DateTimeFormatSpec("1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd");
    assertEquals(dateTimeFormatSpec.getTimeFormat(), DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT);
    assertEquals(dateTimeFormatSpec.getColumnSize(), 1);
    assertEquals(dateTimeFormatSpec.getColumnUnit(), TimeUnit.MILLISECONDS);
    assertEquals(dateTimeFormatSpec.getColumnDateTimeTransformUnit(),
        DateTimeFormatUnitSpec.DateTimeTransformUnit.MILLISECONDS);
    assertEquals(dateTimeFormatSpec.getSDFPattern(), "yyyyMMdd");
    assertEquals(dateTimeFormatSpec.getDateTimezone(), DateTimeZone.UTC);

    assertEquals(new DateTimeFormatSpec("SIMPLE_DATE_FORMAT|yyyyMMdd"), dateTimeFormatSpec);

    dateTimeFormatSpec = new DateTimeFormatSpec("1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd tz(CST)");
    assertEquals(dateTimeFormatSpec.getTimeFormat(), DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT);
    assertEquals(dateTimeFormatSpec.getColumnSize(), 1);
    assertEquals(dateTimeFormatSpec.getColumnUnit(), TimeUnit.MILLISECONDS);
    assertEquals(dateTimeFormatSpec.getColumnDateTimeTransformUnit(),
        DateTimeFormatUnitSpec.DateTimeTransformUnit.MILLISECONDS);
    assertEquals(dateTimeFormatSpec.getSDFPattern(), "yyyy-MM-dd");
    assertEquals(dateTimeFormatSpec.getDateTimezone(), DateTimeZone.forTimeZone(TimeZone.getTimeZone("CST")));

    assertEquals(new DateTimeFormatSpec("SIMPLE_DATE_FORMAT|yyyy-MM-dd|CST"), dateTimeFormatSpec);

    assertThrows(IllegalArgumentException.class, () -> new DateTimeFormatSpec("1:DAY"));

    assertThrows(IllegalArgumentException.class, () -> new DateTimeFormatSpec("one:DAYS:EPOCH"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeFormatSpec("EPOCH|DAYS|one"));

    assertThrows(IllegalArgumentException.class, () -> new DateTimeFormatSpec("1:DAY:EPOCH"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeFormatSpec("EPOCH|DAY"));

    assertThrows(IllegalArgumentException.class, () -> new DateTimeFormatSpec("1:DAY:EPOCH:yyyyMMdd"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeFormatSpec("EPOCH|yyyyMMdd"));

    assertThrows(IllegalArgumentException.class, () -> new DateTimeFormatSpec("1:DAY:SIMPLE_DATE_FORMAT:yyycMMdd"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeFormatSpec("SIMPLE_DATE_FORMAT|yyycMMdd"));
  }
}
