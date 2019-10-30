package org.apache.pinot.thirdeye.detection.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ThirdEyeCacheResponseTests {

  private static final String COLLECTION = "collection";
  private static final MetricDataset METRIC = new MetricDataset("metric", COLLECTION);

  ThirdEyeCacheResponse cacheResponse;
  List<TimeSeriesDataPoint> rows = new ArrayList<>();

  MetricFunction
      metricFunction = new MetricFunction(MetricAggFunction.AVG, METRIC.getMetricName(), 1L, COLLECTION, null, null);

  ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
      .setMetricFunctions(Collections.singletonList(metricFunction))
      .setStartTimeInclusive(1000)
      .setEndTimeExclusive(20000)
      .setGroupBy("dimension")
      .setGroupByTimeGranularity(TimeGranularity.fromString("1_SECONDS"))
      .setLimit(12345)
      .build("ref");

  String metricUrn = MetricEntity.fromMetric(request.getFilterSet().asMap(), metricFunction.getMetricId()).getUrn();
  ThirdEyeCacheRequest cacheRequest = new ThirdEyeCacheRequest(request,1L, metricUrn,1000L, 20000L);

  @BeforeMethod
  public void beforeMethod() {
    cacheResponse = new ThirdEyeCacheResponse(cacheRequest, rows);
  }

  @AfterMethod
  public void afterMethod() {
    rows.clear();
  }

  @Test
  public void testHasNoRows() {
    Assert.assertTrue(cacheResponse.hasNoRows());
    rows.add(new TimeSeriesDataPoint(metricUrn, 2000, metricFunction.getMetricId(), "123"));
    Assert.assertFalse(cacheResponse.hasNoRows());
  }

  /**
   * ThirdEyeCacheResponse.isMissingSlice() method tests
   */

  @Test
  public void testIsMissingSliceWithNoRows() {
    long start = request.getStartTimeInclusive().getMillis();
    long end = request.getEndTimeExclusive().getMillis();

    Assert.assertTrue(cacheResponse.isMissingSlice(start, end));
  }

  @Test
  public void testIsMissingSliceWithMissingStartSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 2000, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 20000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);

    Assert.assertTrue(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithoutMissingStartSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 0, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 20000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  // makes sure that documents with less than 1 time granularity difference aren't counted as missing slices.
  @Test
  public void testIsMissingSliceWithMisalignedStart() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1050, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 20000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithMissingEndSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1000, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 18000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);

    Assert.assertTrue(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithoutMissingEndSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1000, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 20000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithMisalignedEnd() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1000, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 19500, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithMissingStartAndEndSlices() {
    TimeSeriesDataPoint dp = new TimeSeriesDataPoint(metricUrn, 10000, metricFunction.getMetricId(), "123");
    rows.add(dp);

    Assert.assertTrue(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithoutMissingStartAndEndSlices() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 0, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 25000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithMisalignedStartAndEndSlices() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1050, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 19500, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  /**
   * ThirdEyeCacheResponse.isMissingStartSlice() tests
   */

  @Test
  public void testIsMissingStartSliceWithNoRows() {
    Assert.assertTrue(cacheResponse.isMissingStartSlice(request.getStartTimeInclusive().getMillis()));
  }

  @Test
  public void testIsMissingStartSliceWithMissingStartSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 2000, metricFunction.getMetricId(), "123");
    rows.add(startDataPoint);

    Assert.assertTrue(cacheResponse.isMissingStartSlice(request.getStartTimeInclusive().getMillis()));
  }

  @Test
  public void testIsMissingStartSliceWithoutMissingStartSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1000, metricFunction.getMetricId(), "123");
    rows.add(startDataPoint);

    Assert.assertFalse(cacheResponse.isMissingStartSlice(request.getStartTimeInclusive().getMillis()));
  }

  @Test
  public void testIsMissingStartSliceWithExactStartSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1000, metricFunction.getMetricId(), "123");
    rows.add(startDataPoint);

    Assert.assertFalse(cacheResponse.isMissingStartSlice(request.getStartTimeInclusive().getMillis()));
  }

  @Test
  public void testIsMissingStartSliceWithMisalignedStart() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1099, metricFunction.getMetricId(), "123");
    rows.add(startDataPoint);

    Assert.assertFalse(cacheResponse.isMissingStartSlice(request.getStartTimeInclusive().getMillis()));
  }


  /**
   * ThirdEyeCacheResponse.isMissingEndSlice() tests
   */

  @Test
  public void testIsMissingEndSliceWithNoRows() {
    Assert.assertTrue(cacheResponse.isMissingEndSlice(request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingEndSliceWithMissingEndSlice() {
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 10000, metricFunction.getMetricId(), "123");
    rows.add(endDataPoint);

    Assert.assertTrue(cacheResponse.isMissingEndSlice(request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingEndSliceWithoutMissingEndSlice() {
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 19000, metricFunction.getMetricId(), "123");
    rows.add(endDataPoint);

    Assert.assertFalse(cacheResponse.isMissingEndSlice(request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingEndSliceWithExactEndSlice() {
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 20000, metricFunction.getMetricId(), "123");
    rows.add(endDataPoint);

    Assert.assertFalse(cacheResponse.isMissingEndSlice(request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingStartSliceWithMisalignedEnd() {
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 19999, metricFunction.getMetricId(), "123");
    rows.add(endDataPoint);

    Assert.assertFalse(cacheResponse.isMissingEndSlice(request.getEndTimeExclusive().getMillis()));
  }
}
