package com.linkedin.thirdeye.dashboard.resource;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.resources.v2.TimeSeriesResource;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.TimeSeriesLoader;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TimeSeriesResourceTest {
  private static final String COL_TIME = TimeSeriesResource.COL_TIME;
  private static final String COL_VALUE = TimeSeriesResource.COL_VALUE;

  private static final String RANGE = "10:70";

  private static class MockTimeSeriesLoader implements TimeSeriesLoader {
    @Override
    public DataFrame load(MetricSlice slice) throws Exception {
      final long mid = slice.getMetricId();

      return new DataFrame()
          .addSeries(COL_TIME, 0, 10, 20, 30, 40, 50)
          .addSeries(COL_VALUE, mid + 1, mid, mid - 1, mid, mid + 1, DoubleSeries.NULL)
          .setIndex(COL_TIME);
    }
  }

  private TimeSeriesResource resource;

  @BeforeMethod
  public void before() {
    this.resource = new TimeSeriesResource(Executors.newSingleThreadExecutor(), new MockTimeSeriesLoader());
  }

  @Test
  public void testBasic() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "0", RANGE, null, null, null, null);

    Assert.assertEquals(out.size(), 1);
    Assert.assertTrue(out.containsKey(RANGE));

    Map<String, List<? extends Number>> ts = out.get(RANGE);

    Assert.assertEquals(ts.size(), 2);
    Assert.assertTrue(ts.containsKey(COL_TIME));
    Assert.assertTrue(ts.containsKey("0"));

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 10L, 20L, 30L, 40L, 50L));
    assertEquals(ts.get("0"), Arrays.asList(1d, 0d, -1d, 0d, 1d, null));
  }

  @Test
  public void testTranformationCumulative() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "0", RANGE, null, null, "cumulative", null);
    Map<String, List<? extends Number>> ts = out.get(RANGE);

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 10L, 20L, 30L, 40L, 50L));
    assertEquals(ts.get("0"), Arrays.asList(1d, 1d, 0d, 0d, 1d, 1d));
  }

  @Test
  public void testTranformationDifference() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "0", RANGE, null, null, "difference", null);
    Map<String, List<? extends Number>> ts = out.get(RANGE);

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 10L, 20L, 30L, 40L, 50L));
    assertEquals(ts.get("0"), Arrays.asList(null, -1d, -1d, 1d, 1d, null));
  }

  @Test
  public void testTranformationFillForward() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "0", RANGE, null, null, "fillforward", null);
    Map<String, List<? extends Number>> ts = out.get(RANGE);

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 10L, 20L, 30L, 40L, 50L));
    assertEquals(ts.get("0"), Arrays.asList(1d, 0d, -1d, 0d, 1d, 1d));
  }

  @Test
  public void testTranformationFillZero() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "0", RANGE, null, null, "fillzero", null);
    Map<String, List<? extends Number>> ts = out.get(RANGE);

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 10L, 20L, 30L, 40L, 50L));
    assertEquals(ts.get("0"), Arrays.asList(1d, 0d, -1d, 0d, 1d, 0d));
  }

  @Test
  public void testTranformationChange() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "2", RANGE, null, null, "change", null);
    Map<String, List<? extends Number>> ts = out.get(RANGE);

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 10L, 20L, 30L, 40L, 50L));
    assertEquals(ts.get("2"), Arrays.asList(null, -0.333, -0.5d, 1d, 0.5d, null));
  }

  @Test
  public void testTranformationOffset() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "9", RANGE, null, null, "offset", null);
    Map<String, List<? extends Number>> ts = out.get(RANGE);

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 10L, 20L, 30L, 40L, 50L));
    assertEquals(ts.get("9"), Arrays.asList(0d, -1d, -2d, -1d, 0d, null));
  }

  @Test
  public void testTranformationRelative() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "1", RANGE, null, null, "relative", null);
    Map<String, List<? extends Number>> ts = out.get(RANGE);

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 10L, 20L, 30L, 40L, 50L));
    assertEquals(ts.get("1"), Arrays.asList(1d, 0.5d, 0d, 0.5d, 1d, null));
  }

  @Test
  public void testTranformationLog() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "0", RANGE, null, null, "log", null);
    Map<String, List<? extends Number>> ts = out.get(RANGE);

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 10L, 20L, 30L, 40L, 50L));
    assertEquals(ts.get("0"), Arrays.asList(0d, null, null, null, 0d, null));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testTranformationChangeNoGranularityFail() throws Exception {
    this.resource.getTimeSeries("0", RANGE, null, null, "timestamp", null);
  }

  @Test
  public void testMultiTranformation() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "2", RANGE, null, null, "change,fillforward,cumulative", null);
    Map<String, List<? extends Number>> ts = out.get(RANGE);

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 10L, 20L, 30L, 40L, 50L));
    assertEquals(ts.get("2"), Arrays.asList(null, -0.333, -0.833d, 0.166d, 0.666d, 1.166));
  }

  @Test
  public void testAggregationSingleRange() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "0,1", RANGE, null, null, null, "sum");

    Map<String, List<? extends Number>> ts = out.get("sum");

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L));
    assertEquals(ts.get("0"), Arrays.asList(1d, 0d, -1d, 0d, 1d, null));
    assertEquals(ts.get("1"), Arrays.asList(2d, 1d, 0d, 1d, 2d, null));
  }

  @Test
  public void testAggregationMultiRange() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "0,1", RANGE + ",20:80", null, null, null, "sum");

    Map<String, List<? extends Number>> ts = out.get("sum");

    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L));
    assertEquals(ts.get("0"), Arrays.asList(2d, 0d, -2d, 0d, 2d, null));
    assertEquals(ts.get("1"), Arrays.asList(4d, 2d, 0d, 2d, 4d, null));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAggregationMultiRangeDifferentLengthFail() throws Exception {
    this.resource.getTimeSeries("0,1", RANGE + ",20:90", null, null, null, "sum");
  }

  @Test
  public void testNoAggregationMultiRangeDifferentLengthPass() throws Exception {
    this.resource.getTimeSeries("0,1", RANGE + ",20:90", null, null, null, null);
  }

  @Test
  public void testMultiAggregationMultiRange() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "0,1", RANGE + ",20:80", null, null, null, "sum,product");

    Map<String, List<? extends Number>> ts = out.get("sum");
    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L));
    assertEquals(ts.get("0"), Arrays.asList(2d, 0d, -2d, 0d, 2d, null));
    assertEquals(ts.get("1"), Arrays.asList(4d, 2d, 0d, 2d, 4d, null));

    ts = out.get("product");
    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L));
    assertEquals(ts.get("0"), Arrays.asList(1d, 0d, 1d, 0d, 1d, null));
    assertEquals(ts.get("1"), Arrays.asList(4d, 1d, 0d, 1d, 4d, null));

  }

  @Test
  public void testMultiTransformationMultiAggregationMultiRange() throws Exception {
    Map<String, Map<String, List<? extends Number>>> out = this.resource.getTimeSeries(
        "0,1", RANGE + ",20:80", null, null, "fillforward,cumulative", "sum,product");

    // 0: 1d, 0d, -1d, 0d, 1d, null
    // 1: 2d, 1d,  0d, 1d, 2d, null

    // 0 ffill,cumulative: 1d, 1d, 0d, 0d, 1d, 2d
    // 1 ffill,cumulative: 2d, 3d, 3d, 4d, 6d, 8d

    Map<String, List<? extends Number>> ts = out.get("sum");
    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L));
    assertEquals(ts.get("0"), Arrays.asList(2d, 2d, 0d, 0d, 2d, 4d));
    assertEquals(ts.get("1"), Arrays.asList(4d, 6d, 6d, 8d, 12d, 16d));

    ts = out.get("product");
    Assert.assertEquals(ts.get(COL_TIME), Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L));
    assertEquals(ts.get("0"), Arrays.asList(1d, 1d, 0d, 0d, 1d, 4d));
    assertEquals(ts.get("1"), Arrays.asList(4d, 9d, 9d, 16d, 36d, 64d));

  }

  private static void assertEquals(List<? extends Number> actual, List<Double> expected) {
    Assert.assertEquals(actual.size(), expected.size());
    for (int i = 0; i < actual.size(); i++) {
      if (actual.get(i) == null) {
        Assert.assertEquals(actual.get(i), expected.get(i), "index=" + i);
      } else {
        Assert.assertEquals(actual.get(i).doubleValue(), expected.get(i), 0.001, "index=" + i);
      }
    }
  }

}
