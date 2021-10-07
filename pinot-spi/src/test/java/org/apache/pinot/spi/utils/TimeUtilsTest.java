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
package org.apache.pinot.spi.utils;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class TimeUtilsTest {

  @Test
  public void testConvertPeriodToMillis() {
    assertEquals(TimeUtils.convertPeriodToMillis(null), Long.valueOf(0L));
    assertEquals(TimeUtils.convertPeriodToMillis("1s"), Long.valueOf(1000L));
    assertEquals(TimeUtils.convertPeriodToMillis("1m"), Long.valueOf(60000L));
    assertEquals(TimeUtils.convertPeriodToMillis("1h"), Long.valueOf(3600000L));
    assertEquals(TimeUtils.convertPeriodToMillis("24h"), Long.valueOf(86400000L));
    assertEquals(TimeUtils.convertPeriodToMillis("1d"), Long.valueOf(86400000L));
    assertEquals(TimeUtils.convertPeriodToMillis("72h"), Long.valueOf(259200000L));
    assertEquals(TimeUtils.convertPeriodToMillis("2400h"), Long.valueOf(8640000000L));
    assertEquals(TimeUtils.convertPeriodToMillis("2400h"), Long.valueOf(8640000000L));
    assertEquals(TimeUtils.convertPeriodToMillis("2402h3m1s"), Long.valueOf(8647381000L));
    assertEquals(TimeUtils.convertPeriodToMillis("100d2h3m1s"), Long.valueOf(8647381000L));
    assertThrows(IllegalArgumentException.class, () -> TimeUtils.convertPeriodToMillis("invalid"));
  }

  @Test
  public void testConvertMillisToPeriod() {
    assertEquals(TimeUtils.convertMillisToPeriod(null), null);
    assertEquals(TimeUtils.convertMillisToPeriod(60000L), "1m");
    assertEquals(TimeUtils.convertMillisToPeriod(3600000L), "1h");
    assertEquals(TimeUtils.convertMillisToPeriod(82800000L), "23h");
    // assertEquals(TimeUtils.convertMillisToPeriod(86400000L), "1d"); // In some platform this returns 24h
  }

  @Test
  public void testIsPeriodValid() {
    assertTrue(TimeUtils.isPeriodValid("1D13H60M90S"));
    assertTrue(TimeUtils.isPeriodValid("1d13h60m90s"));
    assertTrue(TimeUtils.isPeriodValid("100d"));
    assertTrue(TimeUtils.isPeriodValid("70h175s"));
    assertTrue(TimeUtils.isPeriodValid("50m"));
    assertFalse(TimeUtils.isPeriodValid("string"));
    assertFalse(TimeUtils.isPeriodValid("12"));
    assertFalse(TimeUtils.isPeriodValid("1w3h"));
  }
}
