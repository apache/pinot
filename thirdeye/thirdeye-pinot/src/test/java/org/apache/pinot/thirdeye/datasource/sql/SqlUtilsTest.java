/*
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

package org.apache.pinot.thirdeye.datasource.sql;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SqlUtilsTest {

  String dailyColumnName = "timestamp";
  TimeGranularity dailyGranularity = new TimeGranularity(1, TimeUnit.DAYS);
  String dailyTimeFormat = "yyyyMMdd";
  TimeSpec dailyTimeSpec = new TimeSpec(dailyColumnName, dailyGranularity, dailyTimeFormat);

  DateTime dateTime;

  @BeforeMethod
  public void beforeMethod() {
    dateTime = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss").parseDateTime("01/01/2000 00:00:00").withZone(DateTimeZone.forID("America/Los_Angeles"));
  }

  @AfterMethod
  public void afterMethod() {

  }

  // Tests using daily data granularity

  // test how getCeilingAlignedStartDateTimeString behaves when the data granularity is a perfect multiple of the input dateTime.
  @Test
  public void testGetCeilingAlignedStartDateTimeStringWithPerfectFactorDailyGranularity() {
    Assert.assertEquals(
        SqlUtils.getCeilingAlignedStartDateTimeString(dateTime, dailyGranularity, dailyTimeFormat),
        "20000101");
  }

  // test how getCeilingAlignedStartDateTimeString behaves when the data granularity isn't a perfect multiple of the input dateTime.
  @Test
  public void testGetCeilingAlignedStartDateTimeStringWithoutPerfectFactorDailyGranularity() {
    addHalfStep();
    Assert.assertEquals(
        SqlUtils.getCeilingAlignedStartDateTimeString(dateTime, dailyGranularity, dailyTimeFormat),
        "20000102");
  }

  @Test
  public void testGetFloorAlignedEndDateTimeStringWithPerfectFactorDailyGranularity() {
    // should be dateTime minus one full time granularity.
    Assert.assertEquals(
        SqlUtils.getFloorAlignedEndDateTimeString(dateTime, dailyGranularity, dailyTimeFormat),
        "19991231");
  }

  @Test
  public void testGetFloorAlignedEndDateTimeStringWithoutPerfectFactorDailyGranularity() {
    addHalfStep();
    Assert.assertEquals(
        SqlUtils.getFloorAlignedEndDateTimeString(dateTime, dailyGranularity, dailyTimeFormat),
        "20000101");
  }


  // Tests using hourly data granularity

  private void addHalfStep() {
    dateTime = dateTime.withPeriodAdded(Period.hours(12), 1);
  }
}
