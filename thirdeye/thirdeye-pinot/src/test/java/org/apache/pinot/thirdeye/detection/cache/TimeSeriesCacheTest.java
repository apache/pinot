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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.RelationalThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
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

  ExecutorService executor = Executors.newSingleThreadExecutor();

  private final List<TimeSeriesDataPoint> pretendCacheStore = new ArrayList<>();

  private static final String COLLECTION = "collection";
  private static final MetricDataset METRIC = new MetricDataset("metric", COLLECTION);
  private static final MetricFunction
      metricFunction = new MetricFunction(MetricAggFunction.AVG, METRIC.getMetricName(), 1L, COLLECTION, null, null);

  private static final ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
      .setMetricFunctions(Collections.singletonList(metricFunction))
      .setStartTimeInclusive(1000)
      .setEndTimeExclusive(10000)
      .setGroupByTimeGranularity(TimeGranularity.fromString("1_SECONDS"))
      .setLimit(12345)
      .build("ref");

  private static final TimeSpec
      timeSpec = new TimeSpec(METRIC.getMetricName(), TimeGranularity.fromString("1_SECONDS"), TimeSpec.SINCE_EPOCH_FORMAT);

  private static final String metricUrn = MetricEntity.fromMetric(request.getFilterSet().asMap(), metricFunction.getMetricId()).getUrn();
  private static final ThirdEyeCacheRequest cacheRequest = new ThirdEyeCacheRequest(request,1L, metricUrn,1000L, 20000L);



  @BeforeMethod
  public void beforeMethod() throws Exception {

    // mock QueryCache so it doesn't call data source
    this.queryCache = mock(QueryCache.class);

    // mock DAO object
    this.cacheDAO = mock(CouchbaseCacheDAO.class);
    Mockito.doAnswer(invocation ->
      this.pretendCacheStore.add((TimeSeriesDataPoint) invocation.getArguments()[0])
    ).when(this.cacheDAO).insertTimeSeriesDataPoint(any(TimeSeriesDataPoint.class));

    // mock this for TimeSpec
    Mockito.when(ThirdEyeUtils.getTimeSpecFromDatasetConfig(any(DatasetConfigDTO.class))).thenReturn(timeSpec);

    this.datasetDAO = mock(DatasetConfigManager.class);
    //when(datasetDAO.)
    this.metricDAO = mock(MetricConfigManager.class);

    // mock this for ThirdEyeRequest generation
    //DataFrameUtils.makeTimeSeriesRequestAligned(slice, "ref", this.metricDAO, this.datasetDAO);

    this.cache = new DefaultTimeSeriesCache(metricDAO, datasetDAO, queryCache, cacheDAO, executor);
    ThirdEyeCacheRegistry.getInstance().registerTimeSeriesCache(this.cache);
  }

  @AfterMethod
  public void afterMethod() {
    pretendCacheStore.clear();
    Mockito.reset(this.cacheDAO);
  }

  @Test
  public void testInsertTimeSeriesIntoCache() throws InterruptedException {
    List<String[]> rows = new ArrayList<>();

    // index 0 is time bucket id, index 1 is value, index 2 is timestamp
    for (int i = 0; i < 10; i++) {
      String[] rawTimeSeriesDataPoint = new String[3];
      rawTimeSeriesDataPoint[0] = String.valueOf(i);
      rawTimeSeriesDataPoint[1] = String.valueOf(i);
      rawTimeSeriesDataPoint[2] = String.valueOf(i * 1000);
      rows.add(rawTimeSeriesDataPoint);
    }

    ThirdEyeResponse response = new RelationalThirdEyeResponse(request, rows, timeSpec);
    this.cache.insertTimeSeriesIntoCache(response);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    verify(this.cacheDAO, times(10)).insertTimeSeriesDataPoint(any(TimeSeriesDataPoint.class));

    Assert.assertEquals(this.pretendCacheStore.size(), 10);
    for (int i = 0; i < 10; i++) {
      TimeSeriesDataPoint dp = this.pretendCacheStore.get(i);
      Assert.assertEquals(dp.getMetricId(), metricFunction.getMetricId().longValue());
      Assert.assertEquals(dp.getMetricUrn(), metricUrn);
      Assert.assertEquals(dp.getTimestamp(), i * 1000);
      Assert.assertEquals(dp.getDataValue(), String.valueOf(i));
    }
  }

  @Test
  public void testTryFetchExistingTimeSeriesWithEmptyCache() throws Exception {

    // mock tryFetch method to return a ThirdEyeResponse with nothing in it.
    Mockito
        .when(this.cacheDAO.tryFetchExistingTimeSeries(any(ThirdEyeCacheRequest.class)))
        .thenAnswer(invocation -> {
          return new ThirdEyeCacheResponse(
              (ThirdEyeCacheRequest)invocation.getArguments()[0], new ArrayList<>()) ;
        });

    DatasetConfigDTO datasetDTO = new DatasetConfigDTO();
    datasetDTO.setTimeFormat("SIMPLE_DATE_FORMAT:yyyyMMdd");
    datasetDTO.setTimeDuration(1);
    datasetDTO.setTimeUnit(TimeUnit.DAYS);
    datasetDTO.setTimezone("US/Pacific");

    Mockito.when(this.datasetDAO.findByDataset(anyString())).thenReturn(datasetDTO);

    //Mockito.when(this.queryCache.getQueryResult(any(ThirdEyeRequest.class))).thenReturn()
  }


  private static MetricConfigDTO makeMetric(Long id, String metric, String dataset) {
    MetricConfigDTO metricDTO = new MetricConfigDTO();
    metricDTO.setId(id);
    metricDTO.setName(metric);
    metricDTO.setDataset(dataset);
    metricDTO.setAlias(dataset + "::" + metric);
    return metricDTO;
  }

  private static DatasetConfigDTO makeDataset(Long id, String dataset) {
    DatasetConfigDTO datasetDTO = new DatasetConfigDTO();
    datasetDTO.setId(id);
    datasetDTO.setDataSource("myDataSource");
    datasetDTO.setDataset(dataset);
    datasetDTO.setTimeDuration(3600000);
    datasetDTO.setTimeUnit(TimeUnit.MILLISECONDS);
    return datasetDTO;
  }
}
