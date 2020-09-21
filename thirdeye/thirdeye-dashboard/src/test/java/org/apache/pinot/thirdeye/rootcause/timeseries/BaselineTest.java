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

package org.apache.pinot.thirdeye.rootcause.timeseries;

import com.google.common.collect.ArrayListMultimap;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.StringSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.ArrayUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BaselineTest {
  private static final String COL_TIME = Baseline.COL_TIME;
  private static final String COL_VALUE = Baseline.COL_VALUE;

  private static final MetricSlice BASE_SLICE = MetricSlice.from(12345, 15000, 17000, ArrayListMultimap.<String, String>create(), new TimeGranularity(500, TimeUnit.MILLISECONDS));

  // PDT -> PST 2018
  // 1520681115000 // Saturday, March 10, 2018 3:25:15 AM GMT-08:00
  // 1520842545000 // Monday, March 12, 2018 1:15:45 AM GMT-07:00

  // PST -> PDT 2018
  // 1541240715000 // Saturday, November 3, 2018 3:25:15 AM GMT-07:00
  // 1541409345000 // Monday, November 5, 2018 1:15:45 AM GMT-08:00

  private static final MetricSlice PDT_PST_SLICE = MetricSlice.from(12345, 1520681115000L, 1520842545000L, ArrayListMultimap.<String, String>create(), new TimeGranularity(500, TimeUnit.MILLISECONDS));
  private static final MetricSlice PST_PDT_SLICE = MetricSlice.from(12345, 1541240715000L, 1541409345000L, ArrayListMultimap.<String, String>create(), new TimeGranularity(500, TimeUnit.MILLISECONDS));

  @Test
  public void testBaselineOffsetFrom() {
    BaselineOffset baseline = BaselineOffset.fromOffset(-1000);

    List<MetricSlice> slices = baseline.scatter(BASE_SLICE);
    Assert.assertEquals(slices.size(), 1);
    Assert.assertEquals(slices.get(0).getMetricId(), 12345);
    Assert.assertEquals(slices.get(0).getStart(), 14000);
    Assert.assertEquals(slices.get(0).getEnd(), 16000);
  }

  @Test
  public void testBaselineOffsetCompute() {
    BaselineOffset baseline = BaselineOffset.fromOffset(-1000);

    Map<MetricSlice, DataFrame> data = new HashMap<>();
    data.put(BASE_SLICE.withStart(14000).withEnd(16000),
        new DataFrame(COL_TIME, LongSeries.buildFrom(14000L, 14500L, 15000L, 15500L))
            .addSeries(COL_VALUE, 400d, 500d, 600d, 700d)
    );
    data.put(BASE_SLICE.withStart(14000).withEnd(99999),
        new DataFrame(COL_TIME, LongSeries.buildFrom(99999L, 99999L, 99999L, 99999L))
            .addSeries(COL_VALUE, 99999d, 99999d, 99999d, 99999d)
    );

    DataFrame result = baseline.gather(BASE_SLICE, data);

    assertEquals(result.getLongs(COL_TIME), 15000L, 15500L, 16000L, 16500L);
    assertEquals(result.getDoubles(COL_VALUE), 400d, 500d, 600d, 700d);
  }

  @Test
  public void testBaselineOffsetComputeMultiIndex() {
    BaselineOffset baseline = BaselineOffset.fromOffset(-1000);

    Map<MetricSlice, DataFrame> data = Collections.singletonMap(
        BASE_SLICE.withStart(14000).withEnd(16000),
        new DataFrame()
            .addSeries(COL_TIME, LongSeries.buildFrom(14000L, 14500L, 15000L, 15500L))
            .addSeries("long", LongSeries.buildFrom(1, 2, 3, 4))
            .addSeries("string", StringSeries.buildFrom("A", "B", "C", "D"))
            .addSeries("double", DoubleSeries.buildFrom(2.5, 3.5, 4.5, 5.5))
            .addSeries(COL_VALUE, 400d, 500d, 600d, 700d)
            .setIndex(COL_TIME, "long", "string", "double")
    );

    DataFrame result = baseline.gather(BASE_SLICE, data);

    assertEquals(result.getLongs(COL_TIME), 15000L, 15500L, 16000L, 16500L);
    assertEquals(result.getLongs("long"), 1, 2, 3, 4);
    assertEquals(result.getStrings("string"), "A", "B", "C", "D");
    assertEquals(result.getDoubles("double"), 2.5, 3.5, 4.5, 5.5);
    assertEquals(result.getDoubles(COL_VALUE), 400d, 500d, 600d, 700d);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBaselineOffsetComputeInvalidSlice() {
    BaselineOffset baseline = BaselineOffset.fromOffset(-1000);
    baseline.gather(BASE_SLICE, Collections.singletonMap(
        BASE_SLICE.withStart(13999), new DataFrame()
    ));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBaselineOffsetComputeInvalidDataSize() {
    BaselineOffset baseline = BaselineOffset.fromOffset(-1000);
    baseline.gather(BASE_SLICE, Collections.<MetricSlice, DataFrame>emptyMap());
  }

  @Test
  public void testBaselineAggregateFrom() {
    BaselineAggregate baseline = BaselineAggregate.fromOffsets(BaselineAggregateType.MEDIAN, makePeriods(PeriodType.millis(), -1200L, -4500L), DateTimeZone.UTC);

    List<MetricSlice> slices = baseline.scatter(BASE_SLICE);
    Assert.assertEquals(slices.size(), 2);
    Assert.assertEquals(slices.get(0).getMetricId(), 12345);
    Assert.assertEquals(slices.get(0).getStart(), 13800);
    Assert.assertEquals(slices.get(0).getEnd(), 15800);
    Assert.assertEquals(slices.get(1).getMetricId(), 12345);
    Assert.assertEquals(slices.get(1).getStart(), 10500);
    Assert.assertEquals(slices.get(1).getEnd(), 12500);
  }

  @Test
  public void testBaselineAggregateCompute() {
    BaselineAggregate baseline = BaselineAggregate.fromOffsets(BaselineAggregateType.MEDIAN, makePeriods(PeriodType.millis(), -1200L, -4500L), DateTimeZone.UTC);

    Map<MetricSlice, DataFrame> data = new HashMap<>();
    data.put(BASE_SLICE.withStart(13800).withEnd(15800),
        new DataFrame(COL_TIME, LongSeries.buildFrom(13800L, 14300L, 14800L, 99999L))
            .addSeries(COL_VALUE, 400d, 500d, 600d, 700d)
    );
    data.put(BASE_SLICE.withStart(10500).withEnd(12500),
        new DataFrame(COL_TIME, LongSeries.buildFrom(10500L, 11000L, 11500L, 12000L))
            .addSeries(COL_VALUE, 500d, 600d, 700d, 800d)
    );
    data.put(BASE_SLICE.withStart(13800).withEnd(99999),
        new DataFrame(COL_TIME, LongSeries.buildFrom(99999L, 99999L, 99999L, 99999L))
            .addSeries(COL_VALUE, 99999d, 99999d, 99999d, 99999d)
    );

    DataFrame result = baseline.gather(BASE_SLICE, data);

    assertEquals(result.getLongs(COL_TIME), 15000L, 15500L, 16000L, 16500L);
    assertEquals(result.getDoubles(COL_VALUE), 450d, 550d, 650d, 800d);
  }

  @Test
  public void testBaselineAggregateComputeMultiIndex() {
    BaselineAggregate baseline = BaselineAggregate.fromOffsets(BaselineAggregateType.MEDIAN, makePeriods(PeriodType.millis(), -1200L, -4500L), DateTimeZone.UTC);

    Map<MetricSlice, DataFrame> data = new HashMap<>();
    data.put(BASE_SLICE.withStart(13800).withEnd(15800),
        new DataFrame()
            .addSeries(COL_TIME, LongSeries.buildFrom(13800L, 13800L, 14300L, 14300L, 14800L, 14800L, 15300L, 15300L))
            .addSeries("string", StringSeries.buildFrom("A", "B", "A", "B", "A", "B", "A", "B"))
            .addSeries(COL_VALUE, 400d, 4000d, 500d, 5000d, 600d, 6000d, 700d, 7000d)
            .setIndex(COL_TIME, "string")
    );
    data.put(BASE_SLICE.withStart(10500).withEnd(12500),
        new DataFrame()
            .addSeries(COL_TIME, LongSeries.buildFrom(10500L, 11000L, 11500L, 12000L, 10500, 11000L, 11500L, 12000L))
            .addSeries("string", StringSeries.buildFrom("A", "A", "A", "A", "B", "B", "B", "B"))
            .addSeries(COL_VALUE, 500d, 600d, 700d, 800d, 5000d, 6000d, 7000d, 8000d)
            .setIndex(COL_TIME, "string")
    );

    DataFrame result = baseline.gather(BASE_SLICE, data).sortedBy("string", COL_TIME);

    assertEquals(result.getLongs(COL_TIME), 15000L, 15500L, 16000L, 16500L, 15000L, 15500L, 16000L, 16500L);
    assertEquals(result.getStrings("string"), "A", "A", "A", "A", "B", "B", "B", "B");
    assertEquals(result.getDoubles(COL_VALUE), 450d, 550d, 650d, 750d, 4500d, 5500d, 6500d, 7500d);
  }

  @Test
  public void testBaselineWeekly() {
    BaselineAggregate baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MEDIAN, 2, 1, DateTimeZone.forID("America/Los_Angeles"));

    List<MetricSlice> slicesDefault = baseline.scatter(PDT_PST_SLICE);

    // expect DST correction
    Assert.assertEquals(slicesDefault.get(0).getStart(), 1520681115000L - TimeUnit.DAYS.toMillis(7));
    Assert.assertEquals(slicesDefault.get(0).getEnd(), 1520842545000L - TimeUnit.DAYS.toMillis(7) + TimeUnit.HOURS.toMillis(1));
    Assert.assertEquals(slicesDefault.get(1).getStart(), 1520681115000L - TimeUnit.DAYS.toMillis(14));
    Assert.assertEquals(slicesDefault.get(1).getEnd(), 1520842545000L - TimeUnit.DAYS.toMillis(14) + TimeUnit.HOURS.toMillis(1));

    List<MetricSlice> slicesSummer = baseline.scatter(PST_PDT_SLICE);

    // expect DST correction
    Assert.assertEquals(slicesSummer.get(0).getStart(), 1541240715000L - TimeUnit.DAYS.toMillis(7));
    Assert.assertEquals(slicesSummer.get(0).getEnd(), 1541409345000L - TimeUnit.DAYS.toMillis(7) - TimeUnit.HOURS.toMillis(1));
    Assert.assertEquals(slicesSummer.get(1).getStart(), 1541240715000L - TimeUnit.DAYS.toMillis(14));
    Assert.assertEquals(slicesSummer.get(1).getEnd(), 1541409345000L - TimeUnit.DAYS.toMillis(14) - TimeUnit.HOURS.toMillis(1));
  }

  @Test
  public void testBaselineDaily() {
    BaselineAggregate baseline = BaselineAggregate.fromDayOverDay(BaselineAggregateType.MEDIAN, 2, 1, DateTimeZone.forID("America/Los_Angeles"));

    List<MetricSlice> slicesDefault = baseline.scatter(PDT_PST_SLICE);

    // expect DST correction
    Assert.assertEquals(slicesDefault.get(0).getStart(), 1520681115000L - TimeUnit.DAYS.toMillis(1));
    Assert.assertEquals(slicesDefault.get(0).getEnd(), 1520842545000L - TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(1));
    Assert.assertEquals(slicesDefault.get(1).getStart(), 1520681115000L - TimeUnit.DAYS.toMillis(2));
    Assert.assertEquals(slicesDefault.get(1).getEnd(), 1520842545000L - TimeUnit.DAYS.toMillis(2) + TimeUnit.HOURS.toMillis(1));

    List<MetricSlice> slicesSummer = baseline.scatter(PST_PDT_SLICE);

    // expect DST correction
    Assert.assertEquals(slicesSummer.get(0).getStart(), 1541240715000L - TimeUnit.DAYS.toMillis(1));
    Assert.assertEquals(slicesSummer.get(0).getEnd(), 1541409345000L - TimeUnit.DAYS.toMillis(1) - TimeUnit.HOURS.toMillis(1));
    Assert.assertEquals(slicesSummer.get(1).getStart(), 1541240715000L - TimeUnit.DAYS.toMillis(2));
    Assert.assertEquals(slicesSummer.get(1).getEnd(), 1541409345000L - TimeUnit.DAYS.toMillis(2) - TimeUnit.HOURS.toMillis(1));
  }

  @Test
  public void testBaselineHourly() {
    BaselineAggregate baseline = BaselineAggregate.fromHourOverHour(BaselineAggregateType.MEDIAN, 2, 23, DateTimeZone.forID("America/Los_Angeles"));

    List<MetricSlice> slicesDefault = baseline.scatter(PDT_PST_SLICE);

    // do NOT expect DST correction
    Assert.assertEquals(slicesDefault.get(0).getStart(), 1520681115000L - TimeUnit.HOURS.toMillis(23));
    Assert.assertEquals(slicesDefault.get(0).getEnd(), 1520842545000L - TimeUnit.HOURS.toMillis(23));
    Assert.assertEquals(slicesDefault.get(1).getStart(), 1520681115000L - TimeUnit.HOURS.toMillis(24));
    Assert.assertEquals(slicesDefault.get(1).getEnd(), 1520842545000L - TimeUnit.HOURS.toMillis(24));

    List<MetricSlice> slicesSummer = baseline.scatter(PST_PDT_SLICE);

    // do NOT expect DST correction
    Assert.assertEquals(slicesSummer.get(0).getStart(), 1541240715000L - TimeUnit.HOURS.toMillis(23));
    Assert.assertEquals(slicesSummer.get(0).getEnd(), 1541409345000L - TimeUnit.HOURS.toMillis(23));
    Assert.assertEquals(slicesSummer.get(1).getStart(), 1541240715000L - TimeUnit.HOURS.toMillis(24));
    Assert.assertEquals(slicesSummer.get(1).getEnd(), 1541409345000L - TimeUnit.HOURS.toMillis(24));
  }

  private static List<Period> makePeriods(PeriodType periodType, long... offsetMillis) {
    List<Period> output = new ArrayList<>();
    for (long offset : offsetMillis) {
      output.add(new Period(offset, periodType));
    }
    return output;
  }

  private static void assertEquals(LongSeries series, long... values) {
    Assert.assertEquals(
        Arrays.asList(ArrayUtils.toObject(series.values())),
        Arrays.asList(ArrayUtils.toObject(values)));
  }

  private static void assertEquals(DoubleSeries series, double... values) {
    Assert.assertEquals(
        Arrays.asList(ArrayUtils.toObject(series.values())),
        Arrays.asList(ArrayUtils.toObject(values)));
  }

  private static void assertEquals(StringSeries series, String... values) {
    Assert.assertEquals(
        Arrays.asList(series.values()),
        Arrays.asList(values));
  }
}
