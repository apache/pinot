package com.linkedin.thirdeye.rootcause.timeseries;

import com.google.common.collect.ArrayListMultimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
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

  private static final MetricSlice baseSlice = MetricSlice.from(12345, 15000, 17000, ArrayListMultimap.<String, String>create(), new TimeGranularity(500, TimeUnit.MILLISECONDS));

  @Test
  public void testBaselineOffsetFrom() {
    BaselineOffset baseline = new BaselineOffset(-1000);

    List<MetricSlice> slices = baseline.from(baseSlice);
    Assert.assertEquals(slices.size(), 1);
    Assert.assertEquals(slices.get(0).getMetricId(), 12345);
    Assert.assertEquals(slices.get(0).getStart(), 14000);
    Assert.assertEquals(slices.get(0).getEnd(), 16000);
  }

  @Test
  public void testBaselineOffsetCompute() {
    BaselineOffset baseline = new BaselineOffset(-1000);

    Map<MetricSlice, DataFrame> data = Collections.singletonMap(
        MetricSlice.from(12345, 14000, 16000),
        new DataFrame(COL_TIME, LongSeries.buildFrom(14000L, 14500L, 15000L, 15500L))
            .addSeries(COL_VALUE, 400d, 500d, 600d, 700d)
    );

    DataFrame result = baseline.compute(baseSlice, data);

    assertEquals(result.getLongs(COL_TIME), 15000L, 15500L, 16000L, 16500L);
    assertEquals(result.getDoubles(COL_VALUE), 400d, 500d, 600d, 700d);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBaselineOffsetComputeInvalidSlice() {
    BaselineOffset baseline = new BaselineOffset(-1000);
    baseline.compute(baseSlice, Collections.singletonMap(
        baseSlice.withStart(13999), new DataFrame()
    ));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBaselineOffsetComputeInvalidDataSize() {
    BaselineOffset baseline = new BaselineOffset(-1000);
    baseline.compute(baseSlice, Collections.<MetricSlice, DataFrame>emptyMap());
  }

  @Test
  public void testBaselineAggregateFrom() {
    BaselineAggregate baseline = new BaselineAggregate(BaselineAggregate.Type.MEDIAN, Arrays.asList(-1200L, -4500L));

    List<MetricSlice> slices = baseline.from(baseSlice);
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
    BaselineAggregate baseline = new BaselineAggregate(BaselineAggregate.Type.MEDIAN, Arrays.asList(-1200L, -4500L));

    Map<MetricSlice, DataFrame> data = new HashMap<>();
    data.put(
        MetricSlice.from(12345, 13800, 15800),
        new DataFrame(COL_TIME, LongSeries.buildFrom(13800L, 14300L, 14800L, 99999L))
            .addSeries(COL_VALUE, 400d, 500d, 600d, 700d)
    );
    data.put(
        MetricSlice.from(12345, 10500, 12500),
        new DataFrame(COL_TIME, LongSeries.buildFrom(10500, 11000L, 11500L, 12000L))
            .addSeries(COL_VALUE, 500d, 600d, 700d, 800d)
    );

    DataFrame result = baseline.compute(baseSlice, data);

    assertEquals(result.getLongs(COL_TIME), 15000L, 15500L, 16000L, 16500L);
    assertEquals(result.getDoubles(COL_VALUE), 450d, 550d, 650d, Double.NaN);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBaselineAggregateComputeInvalidSlice() {
    BaselineAggregate baseline = new BaselineAggregate(BaselineAggregate.Type.MEDIAN, Arrays.asList(-1200L, -4500L));
    baseline.compute(baseSlice, Collections.singletonMap(
        baseSlice.withStart(13999), new DataFrame()
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
}
