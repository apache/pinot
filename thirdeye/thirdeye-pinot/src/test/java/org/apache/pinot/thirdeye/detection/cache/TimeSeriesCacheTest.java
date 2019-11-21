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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.RelationalThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TimeSeriesCacheTest {
  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private QueryCache queryCache;
  private CouchbaseCacheDAO cacheDAO;
  private DefaultTimeSeriesCache cache;

  private boolean centralizedCacheToggle = CacheConfig.getInstance().useCentralizedCache();
  private CacheConfig config = new CacheConfig();

  ExecutorService executor;

  private final List<TimeSeriesDataPoint> pretendCacheStore = new ArrayList<>();

  private static final String COLLECTION = "collection";
  private static final MetricDataset METRIC = new MetricDataset("metric", COLLECTION);
  private static final MetricFunction
      metricFunction = new MetricFunction(MetricAggFunction.AVG, METRIC.getMetricName(), 1L, COLLECTION, null, null);

  private static final ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
      .setMetricFunctions(Collections.singletonList(metricFunction))
      .setStartTimeInclusive(0)
      .setEndTimeExclusive(10000)
      .setGroupByTimeGranularity(TimeGranularity.fromString("1_SECONDS"))
      .setLimit(12345)
      .build("ref");

  private static final TimeSpec
      timeSpec = new TimeSpec(METRIC.getMetricName(), TimeGranularity.fromString("1_SECONDS"), TimeSpec.SINCE_EPOCH_FORMAT);

  private static final String metricUrn = MetricEntity.fromMetric(request.getFilterSet().asMap(), metricFunction.getMetricId()).getUrn();


  @BeforeMethod
  public void beforeMethod() throws Exception {

    // mock QueryCache so it doesn't call data source
    this.queryCache = mock(QueryCache.class);

    // mock DAO object so that inserts put data into list
    this.cacheDAO = mock(CouchbaseCacheDAO.class);
    Mockito.doAnswer(invocation ->
      this.pretendCacheStore.add((TimeSeriesDataPoint) invocation.getArguments()[0])
    ).when(this.cacheDAO).insertTimeSeriesDataPoint(any(TimeSeriesDataPoint.class));

    this.datasetDAO = mock(DatasetConfigManager.class);
    this.metricDAO = mock(MetricConfigManager.class);

    this.executor = Executors.newSingleThreadExecutor();

    this.cache = new DefaultTimeSeriesCache(metricDAO, datasetDAO, queryCache, cacheDAO, executor);
    ThirdEyeCacheRegistry.getInstance().registerTimeSeriesCache(this.cache);
  }

  @AfterMethod
  public void afterMethod() {
    config.setUseCentralizedCache(centralizedCacheToggle);
    pretendCacheStore.clear();
    Mockito.reset(this.cacheDAO, this.datasetDAO, this.metricDAO);
  }

  @Test
  public void testInsertTimeSeriesIntoCache() throws InterruptedException {
    List<String[]> rows = buildRawResponseRowsWithTimestamps(0, 10);

    ThirdEyeResponse response = new RelationalThirdEyeResponse(request, rows, timeSpec);
    this.cache.insertTimeSeriesIntoCache(response);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    verify(this.cacheDAO, times(10)).insertTimeSeriesDataPoint(any(TimeSeriesDataPoint.class));

    Assert.assertEquals(this.pretendCacheStore.size(), 10);
    verifyTimeSeriesListCorrectness();
  }

  @Test
  public void testTryFetchExistingTimeSeriesWithoutCentralizedCacheEnabled() throws Exception {
    config.setUseCentralizedCache(false);

    List<String[]> rows = buildRawResponseRowsWithTimestamps(0, 10);

    // mock queryCache to return full rows
    Mockito
        .when(this.queryCache.getQueryResult(any(ThirdEyeRequest.class)))
        .thenAnswer(invocation -> {
          return new RelationalThirdEyeResponse(
              (ThirdEyeRequest)invocation.getArguments()[0],
              rows,
              timeSpec);
        });

    ThirdEyeResponse response = this.queryCache.getQueryResult(request);
    verifyRowSeriesCorrectness(response);
  }

  @Test
  public void testTryFetchExistingTimeSeriesFromCentralizedCacheWithEmptyCache() throws Exception {

    config.setUseCentralizedCache(true);

    // mock tryFetch method to return a ThirdEyeResponse with nothing in it.
    Mockito
        .when(this.cacheDAO.tryFetchExistingTimeSeries(any(ThirdEyeCacheRequest.class)))
        .thenAnswer(invocation -> {
          return new ThirdEyeCacheResponse(
              (ThirdEyeCacheRequest)invocation.getArguments()[0], new ArrayList<>());
        });

    Mockito.when(this.datasetDAO.findByDataset(anyString())).thenReturn(makeDatasetDTO());

    List<String[]> rows = buildRawResponseRowsWithTimestamps(0, 10);

    // mock queryCache to return full rows
    Mockito
        .when(this.queryCache.getQueryResult(any(ThirdEyeRequest.class)))
        .thenAnswer(invocation -> {
          ThirdEyeRequest request = (ThirdEyeRequest)invocation.getArguments()[0];
          Assert.assertEquals(request.getStartTimeInclusive().getMillis(), 0);
          Assert.assertEquals(request.getEndTimeExclusive().getMillis(), 10000);
          return new RelationalThirdEyeResponse(request, rows, timeSpec);
        });

    ThirdEyeResponse response = this.cache.fetchTimeSeries(request);

    // verify that the missing data points were inserted into the cache after miss.
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    verify(this.cacheDAO, times(10)).insertTimeSeriesDataPoint(any(TimeSeriesDataPoint.class));
    Assert.assertEquals(this.pretendCacheStore.size(), 10);

    verifyTimeSeriesListCorrectness();
    verifyRowSeriesCorrectness(response);
    verifyTimeSpec(response.getDataTimeSpec());
  }

  @Test
  public void testTryFetchExistingTimeSeriesFromCentralizedCacheWithFullCache() throws Exception {

    config.setUseCentralizedCache(true);

    List<TimeSeriesDataPoint> dataPoints = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      TimeSeriesDataPoint dp = new TimeSeriesDataPoint(metricUrn, i * 1000, 1, String.valueOf(i));
      dataPoints.add(dp);
    }


    // mock tryFetch method to return a ThirdEyeResponse with the correct data points in it.
    Mockito
        .when(this.cacheDAO.tryFetchExistingTimeSeries(any(ThirdEyeCacheRequest.class)))
        .thenAnswer(invocation -> {
          return new ThirdEyeCacheResponse(
              (ThirdEyeCacheRequest)invocation.getArguments()[0], dataPoints);
        });

    Mockito.when(this.datasetDAO.findByDataset(anyString())).thenReturn(makeDatasetDTO());

    ThirdEyeResponse response = this.cache.fetchTimeSeries(request);
    verifyRowSeriesCorrectness(response);
  }

  /**
   *   TODO: figure out how to test cases where the cache only has partial data.
   *   Right now it's difficult to do so because we are using DataFrameUtils.makeTimeSeriesRequestAligned(),
   *   which is a static method. We may have to refactor our code to do these tests. One idea could be to
   *   have all classes that use TimeSeriesLoader use TimeSeriesCache instead, and TimeSeriesCache is the
   *   only class to use TimeSeriesLoader.
   */

//  @Test
//  public void testTryFetchExistingTimeSeriesFromCentralizedCacheWithMissingStartSlice() throws Exception {
//    config.setUseCentralizedCache(true);
//
//    List<TimeSeriesDataPoint> dataPoints = new ArrayList<>();
//    for (int i = 0; i < 5; i++) {
//      TimeSeriesDataPoint dp = new TimeSeriesDataPoint(metricUrn, i * 1000, 1, String.valueOf(i));
//      dataPoints.add(dp);
//    }
//
//    Mockito
//        .when(this.cacheDAO.tryFetchExistingTimeSeries(any(ThirdEyeCacheRequest.class)))
//        .thenAnswer(invocation -> {
//          return new ThirdEyeCacheResponse(
//              (ThirdEyeCacheRequest)invocation.getArguments()[0], dataPoints);
//        });
//
//    Mockito.when(this.datasetDAO.findByDataset(anyString())).thenReturn(makeDatasetDTO());
//
//    List<String[]> rows = buildRawResponseRowsWithTimestamps(5, 10);
//
//    // mock queryCache to return missing rows
//    Mockito
//        .when(this.queryCache.getQueryResult(any(ThirdEyeRequest.class)))
//        .thenAnswer(invocation -> {
//          return new RelationalThirdEyeResponse(
//              (ThirdEyeRequest)invocation.getArguments()[0],
//              rows,
//              timeSpec);
//        });
//
//    ThirdEyeResponse response = this.cache.fetchTimeSeries(request);
//    verifyRowSeriesCorrectness(response);
//  }

  private List<String[]> buildRawResponseRowsWithTimestamps(int start, int end) {
    List<String[]> rows = new ArrayList<>();
    // index 0 is time bucket id, index 1 is value, index 2 is timestamp
    for (int i = start; i < end; i++) {
      String[] rawTimeSeriesDataPoint = new String[3];
      rawTimeSeriesDataPoint[0] = String.valueOf(i);
      rawTimeSeriesDataPoint[1] = String.valueOf(i);
      rawTimeSeriesDataPoint[2] = String.valueOf(i * 1000);
      rows.add(rawTimeSeriesDataPoint);
    }

    return rows;
  }

  private void verifyTimeSeriesListCorrectness() {
    for (int i = 0; i < 10; i++) {
      TimeSeriesDataPoint dp = this.pretendCacheStore.get(i);
      Assert.assertEquals(dp.getMetricId(), metricFunction.getMetricId().longValue());
      Assert.assertEquals(dp.getMetricUrn(), metricUrn);
      Assert.assertEquals(dp.getTimestamp(), i * 1000);
      Assert.assertEquals(dp.getDataValue(), String.valueOf(i));
    }
  }

  private void verifyRowSeriesCorrectness(ThirdEyeResponse response) {
    for (MetricFunction metric : response.getMetricFunctions()) {
      String metricUrn = MetricEntity.fromMetric(response.getRequest().getFilterSet().asMap(), metric.getMetricId()).getUrn();
      for (int i = 0; i < response.getNumRowsFor(metric); i++) {
        Map<String, String> row = response.getRow(metric, i);
        Assert.assertEquals(row.get("AVG_metric"), String.valueOf(i));
      }
    }
  }

  private void verifyTimeSpec(TimeSpec spec) {
    //Assert.assertEquals(timeSpec.getColumnName(), spec.getColumnName());
    Assert.assertEquals(timeSpec.getDataGranularity(), spec.getDataGranularity());
    Assert.assertEquals(timeSpec.getFormat(), spec.getFormat());
  }

  private DatasetConfigDTO makeDatasetDTO() {
    DatasetConfigDTO datasetDTO = new DatasetConfigDTO();
    datasetDTO.setTimeFormat(TimeSpec.SINCE_EPOCH_FORMAT);
    datasetDTO.setTimeDuration(1);
    datasetDTO.setTimeUnit(TimeUnit.SECONDS);
    datasetDTO.setTimezone("UTC");
    datasetDTO.setTimeColumn(METRIC.getMetricName());

    return datasetDTO;
  }
}
