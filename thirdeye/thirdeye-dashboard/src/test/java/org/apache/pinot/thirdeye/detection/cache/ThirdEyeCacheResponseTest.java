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

package org.apache.pinot.thirdeye.detection.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.RelationalThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ThirdEyeCacheResponseTest {

  private static final String COLLECTION = "collection";
  private static final MetricDataset METRIC = new MetricDataset("metric", COLLECTION);

  private ThirdEyeCacheResponse cacheResponse;
  private List<TimeSeriesDataPoint> rows = new ArrayList<>();

  private static final MetricFunction
      metricFunction = new MetricFunction(MetricAggFunction.AVG, METRIC.getMetricName(), 1L, COLLECTION, null, null);

  private static final ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
      .setMetricFunctions(Collections.singletonList(metricFunction))
      .setStartTimeInclusive(1000)
      .setEndTimeExclusive(20000)
      .setGroupByTimeGranularity(TimeGranularity.fromString("1_SECONDS"))
      .setLimit(12345)
      .build("ref");

  private static final TimeSpec timeSpec = new TimeSpec(METRIC.getMetricName(), TimeGranularity.fromString("1_SECONDS"), TimeSpec.SINCE_EPOCH_FORMAT);

  private static final String metricUrn = MetricEntity.fromMetric(request.getFilterSet().asMap(), metricFunction.getMetricId()).getUrn();
  private static final ThirdEyeCacheRequest cacheRequest = ThirdEyeCacheRequest.from(request);

  @BeforeMethod
  public void beforeMethod() {
    cacheResponse = new ThirdEyeCacheResponse(cacheRequest, rows);
  }

  @AfterMethod
  public void afterMethod() {
    rows.clear();
  }

  @Test
  public void testHasNoRowsWithoutRows() {
    Assert.assertTrue(cacheResponse.hasNoRows());
  }

  @Test
  public void testHasNoRowsWithRows() {
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
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertTrue(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithoutMissingStartSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 0, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 20000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  // makes sure that documents with less than 1 time granularity difference aren't counted as missing slices.
  @Test
  public void testIsMissingSliceWithMisalignedStart() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1050, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 20000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithMissingEndSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1000, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 18000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertTrue(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithoutMissingEndSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1000, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 20000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithMisalignedEnd() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1000, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 19500, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithMissingStartAndEndSlices() {
    TimeSeriesDataPoint dp = new TimeSeriesDataPoint(metricUrn, 10000, metricFunction.getMetricId(), "123");
    rows.add(dp);
    cacheResponse.setFirstTimestamp(dp.getTimestamp());
    cacheResponse.setLastTimestamp(dp.getTimestamp());

    Assert.assertTrue(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithoutMissingStartAndEndSlices() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 0, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 25000, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertFalse(cacheResponse.isMissingSlice(request.getStartTimeInclusive().getMillis(), request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingSliceWithMisalignedStartAndEndSlices() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1050, metricFunction.getMetricId(), "123");
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 19500, metricFunction.getMetricId(), "321");
    rows.add(startDataPoint);
    rows.add(endDataPoint);
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

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
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(startDataPoint.getTimestamp());

    Assert.assertTrue(cacheResponse.isMissingStartSlice(request.getStartTimeInclusive().getMillis()));
  }

  @Test
  public void testIsMissingStartSliceWithoutMissingStartSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1000, metricFunction.getMetricId(), "123");
    rows.add(startDataPoint);
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(startDataPoint.getTimestamp());

    Assert.assertFalse(cacheResponse.isMissingStartSlice(request.getStartTimeInclusive().getMillis()));
  }

  @Test
  public void testIsMissingStartSliceWithExactStartSlice() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1000, metricFunction.getMetricId(), "123");
    rows.add(startDataPoint);
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(startDataPoint.getTimestamp());

    Assert.assertFalse(cacheResponse.isMissingStartSlice(request.getStartTimeInclusive().getMillis()));
  }

  @Test
  public void testIsMissingStartSliceWithMisalignedStart() {
    TimeSeriesDataPoint startDataPoint = new TimeSeriesDataPoint(metricUrn, 1099, metricFunction.getMetricId(), "123");
    rows.add(startDataPoint);
    cacheResponse.setFirstTimestamp(startDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(startDataPoint.getTimestamp());

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
    cacheResponse.setFirstTimestamp(endDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertTrue(cacheResponse.isMissingEndSlice(request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingEndSliceWithoutMissingEndSlice() {
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 19000, metricFunction.getMetricId(), "123");
    rows.add(endDataPoint);
    cacheResponse.setFirstTimestamp(endDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertFalse(cacheResponse.isMissingEndSlice(request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingEndSliceWithExactEndSlice() {
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 20000, metricFunction.getMetricId(), "123");
    rows.add(endDataPoint);
    cacheResponse.setFirstTimestamp(endDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertFalse(cacheResponse.isMissingEndSlice(request.getEndTimeExclusive().getMillis()));
  }

  @Test
  public void testIsMissingStartSliceWithMisalignedEnd() {
    TimeSeriesDataPoint endDataPoint = new TimeSeriesDataPoint(metricUrn, 19999, metricFunction.getMetricId(), "123");
    rows.add(endDataPoint);
    cacheResponse.setFirstTimestamp(endDataPoint.getTimestamp());
    cacheResponse.setLastTimestamp(endDataPoint.getTimestamp());

    Assert.assertFalse(cacheResponse.isMissingEndSlice(request.getEndTimeExclusive().getMillis()));
  }

  /**
   * ThirdEyeCacheResponse.mergeSliceIntoRows() tests
   */

  @Test
  public void testMergeSliceIntoRowsAppend() {
    for (int i = 0; i < 10; i++) {
      TimeSeriesDataPoint dataPoint = new TimeSeriesDataPoint(metricUrn, i * 1000, metricFunction.getMetricId(), String.valueOf(i));
      rows.add(dataPoint);
    }

    List<String[]> newRows = new ArrayList<>();

    for (int i = 10; i < 20; i++) {
      String[] rawTimeSeriesDataPoint = new String[3];
      rawTimeSeriesDataPoint[0] = String.valueOf(i);
      rawTimeSeriesDataPoint[1] = String.valueOf(i);
      rawTimeSeriesDataPoint[2] = String.valueOf(i * 1000);
      newRows.add(rawTimeSeriesDataPoint);
    }

    cacheResponse.mergeSliceIntoRows(new RelationalThirdEyeResponse(request, newRows, timeSpec));

    Assert.assertEquals(cacheResponse.getNumRows(), 20);

    List<TimeSeriesDataPoint> rows = cacheResponse.getTimeSeriesRows();

    for (int i = 0; i < 20; i++) {
      TimeSeriesDataPoint dp = rows.get(i);
      Assert.assertEquals(dp.getMetricId(), metricFunction.getMetricId().longValue());
      Assert.assertEquals(dp.getMetricUrn(), metricUrn);
      Assert.assertEquals(dp.getTimestamp(), i * 1000);
      Assert.assertEquals(dp.getDataValue(), String.valueOf(i));
    }
  }
}
