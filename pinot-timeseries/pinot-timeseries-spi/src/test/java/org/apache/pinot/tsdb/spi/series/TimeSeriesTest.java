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
package org.apache.pinot.tsdb.spi.series;

import java.time.Duration;
import java.util.Collections;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TimeSeriesTest {
  private static final TimeBuckets TIME_BUCKETS = TimeBuckets.ofSeconds(100, Duration.ofSeconds(10), 10);

  @Test
  public void testTimeSeriesAcceptsDoubleValues() {
    Double[] values = new Double[10];
    TimeSeries timeSeries = new TimeSeries("anything", null, TIME_BUCKETS, values, Collections.emptyList(),
        new Object[0]);
    assertEquals(timeSeries.getDoubleValues(), values);
  }

  @Test
  public void testTimeSeriesAcceptsBytesValues() {
    byte[][] byteValues = new byte[10][1231];
    TimeSeries timeSeries = new TimeSeries("anything", null, TIME_BUCKETS, byteValues, Collections.emptyList(),
        new Object[0]);
    assertEquals(timeSeries.getBytesValues(), byteValues);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testTimeSeriesDeniesWhenValuesNotDoubleOrBytes() {
    Object[] someValues = new Long[10];
    TimeSeries timeSeries = new TimeSeries("anything", null, TIME_BUCKETS, someValues, Collections.emptyList(),
        new Object[0]);
  }
}
