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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.dataframe.util.TimeSeriesRequestContainer;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.RelationalThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.TimeRangeUtils;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Default implementation of TimeSeriesCache, which is used to attempt fetching
 * data from centralized cache as an alternative to directly fetching from the
 * data source each time.
 */

public class DefaultTimeSeriesCache implements TimeSeriesCache {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTimeSeriesCache.class);

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final QueryCache queryCache;
  private CouchbaseCacheDAO cacheDAO;
  private final ExecutorService executor;

  public DefaultTimeSeriesCache(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, QueryCache queryCache, CouchbaseCacheDAO cacheDAO, ExecutorService executorService) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.queryCache = queryCache;
    this.cacheDAO = cacheDAO;
    this.executor = executorService;
  }

  /**
   * Given a ThirdEyeRequest object, builds the CacheRequest object and attempts to fetch data
   * from the cache. If the requested slice of data is not in the cache or is only partially in
   * the cache, will fetch the missing slices and build the complete timeseries.
   * @param thirdEyeRequest ThirdEyeRequest built from aligned metricSlice request.
   * @return ThirdEyeResponse, the full response for the request
   * @throws Exception if fetch from original data source was not successful.
   */
  public ThirdEyeResponse fetchTimeSeries(ThirdEyeRequest thirdEyeRequest) throws Exception {

    if (!CacheConfig.getInstance().useCentralizedCache()) {
      return this.queryCache.getQueryResult(thirdEyeRequest);
    }

    ThirdEyeCacheResponse cacheResponse = cacheDAO.tryFetchExistingTimeSeries(ThirdEyeCacheRequest.from(thirdEyeRequest));

    DateTime sliceStart = thirdEyeRequest.getStartTimeInclusive();
    DateTime sliceEnd = thirdEyeRequest.getEndTimeExclusive();

    if (cacheResponse.isMissingSlice(sliceStart.getMillis(), sliceEnd.getMillis())) {
      fetchMissingSlices(cacheResponse);
    }

    return buildThirdEyeResponseFromCacheResponse(cacheResponse);
  }

  /**
   * Used if cache had partial or no data for the requested time slice. Checks for which
   * slices are missing and fetches them, then adds them to the response.
   * @param cacheResponse cache response object
   * @throws Exception if fetch to data source throws error.
   */
  private void fetchMissingSlices(ThirdEyeCacheResponse cacheResponse) throws Exception {

    ThirdEyeRequest request = cacheResponse.getCacheRequest().getRequest();

    ThirdEyeResponse result;
    MetricSlice slice;

    if (cacheResponse.hasNoRows()) {
      result = this.queryCache.getQueryResult(cacheResponse.getCacheRequest().getRequest());
      insertTimeSeriesIntoCache(result);
      cacheResponse.mergeSliceIntoRows(result);
    } else {

      long metricId = request.getMetricFunctions().get(0).getMetricId();
      long requestSliceStart = request.getStartTimeInclusive().getMillis();
      long requestSliceEnd = request.getEndTimeExclusive().getMillis();

      if (cacheResponse.isMissingStartSlice(requestSliceStart)) {
        slice = MetricSlice.from(metricId, requestSliceStart, cacheResponse.getFirstTimestamp(), request.getFilterSet(),
            request.getGroupByTimeGranularity());
        result = fetchSliceFromSource(slice);
        insertTimeSeriesIntoCache(result);
        cacheResponse.mergeSliceIntoRows(result);
      }

      if (cacheResponse.isMissingEndSlice(requestSliceEnd)) {
        // we add one time granularity to start because the start is inclusive.
        slice = MetricSlice.from(metricId, cacheResponse.getLastTimestamp() + request.getGroupByTimeGranularity().toMillis(), requestSliceEnd,
            request.getFilterSet(), request.getGroupByTimeGranularity());
        result = fetchSliceFromSource(slice);
        insertTimeSeriesIntoCache(result);
        cacheResponse.mergeSliceIntoRows(result);
      }
    }
  }

  /**
   * Shorthand to call queryCache to fetch data from the data source.
   * @param slice MetricSlice used to build ThirdEyeRequest object for queryCache to load data
   * @return ThirdEyeResponse, the data for the given slice
   * @throws Exception if fetching from data source had an exception somewhere
   */
  private ThirdEyeResponse fetchSliceFromSource(MetricSlice slice) throws Exception {
    TimeSeriesRequestContainer rc = DataFrameUtils.makeTimeSeriesRequestAligned(slice, "ref", this.metricDAO, this.datasetDAO);
    return this.queryCache.getQueryResult(rc.getRequest());
  }

  /**
   * Parses cache response object and builds a ThirdEyeResponse object from it.
   * @param cacheResponse cache response
   * @return ThirdEyeResponse object
   */
  private ThirdEyeResponse buildThirdEyeResponseFromCacheResponse(ThirdEyeCacheResponse cacheResponse) {

    List<String[]> rows = new ArrayList<>();
    ThirdEyeRequest request = cacheResponse.getCacheRequest().getRequest();

    String dataset = request.getMetricFunctions().get(0).getDataset();
    DatasetConfigDTO datasetDTO = datasetDAO.findByDataset(dataset);
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetDTO);
    DateTimeZone timeZone = DateTimeZone.forID(datasetDTO.getTimezone());

    for (TimeSeriesDataPoint dataPoint : cacheResponse.getTimeSeriesRows()) {
      int timeBucketIndex = TimeRangeUtils.computeBucketIndex(
          request.getGroupByTimeGranularity(), request.getStartTimeInclusive(), new DateTime(dataPoint.getTimestamp(), timeZone));

      String[] row = new String[2];
      row[0] = String.valueOf(timeBucketIndex);
      row[1] = dataPoint.getDataValue();

      rows.add(row);
    }

    return new RelationalThirdEyeResponse(request, rows, timeSpec);
  }

  /**
   * Takes a ThirdEyeResponse time-series and inserts the data points individually,
   * in parallel. Alternatively, for dimension exploration jobs, updates the
   * corresponding document in the cache with a new value for the current dimension
   * combination if the document already exists in the cache.
   * @param response a object containing the time-series to be inserted
   */
  public void insertTimeSeriesIntoCache(ThirdEyeResponse response) {
    // insert points in parallel
    for (MetricFunction metric : response.getMetricFunctions()) {
      String metricUrn = MetricEntity.fromMetric(response.getRequest().getFilterSet().asMap(), metric.getMetricId()).getUrn();
      for (int i = 0; i < response.getNumRowsFor(metric); i++) {
        Map<String, String> row = response.getRow(metric, i);
        TimeSeriesDataPoint dp = new TimeSeriesDataPoint(metricUrn, Long.parseLong(row.get(CacheConstants.TIMESTAMP)), metric.getMetricId(), row.get(metric.toString()));
        executor.execute(() -> this.cacheDAO.insertTimeSeriesDataPoint(dp));
      }
    }
  }
}
