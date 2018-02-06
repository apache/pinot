package com.linkedin.thirdeye.rootcause.timeseries;

import com.google.common.collect.ArrayListMultimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.ArrayUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BaselineTest {
  private static final String COL_TIME = Baseline.COL_TIME;
  private static final String COL_VALUE = Baseline.COL_VALUE;

  private static final MetricSlice BASE_SLICE = MetricSlice.from(12345, 15000, 17000, ArrayListMultimap.<String, String>create(), new TimeGranularity(500, TimeUnit.MILLISECONDS));

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
    BaselineAggregate baseline = BaselineAggregate.fromOffsets(BaselineAggregateType.MEDIAN, Arrays.asList(-1200L, -4500L));

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
    BaselineAggregate baseline = BaselineAggregate.fromOffsets(BaselineAggregateType.MEDIAN, Arrays.asList(-1200L, -4500L));

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
    assertEquals(result.getDoubles(COL_VALUE), 450d, 550d, 650d, Double.NaN);
  }

  @Test
  public void testBaselineAggregateComputeMultiIndex() {
    BaselineAggregate baseline = BaselineAggregate.fromOffsets(BaselineAggregateType.MEDIAN, Arrays.asList(-1200L, -4500L));

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

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBaselineAggregateComputeInvalidSlice() {
    BaselineAggregate baseline = BaselineAggregate.fromOffsets(BaselineAggregateType.MEDIAN, Arrays.asList(-1200L, -4500L));
    baseline.gather(BASE_SLICE, Collections.singletonMap(
        BASE_SLICE.withStart(13999), new DataFrame()
    ));
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
