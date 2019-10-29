package org.apache.pinot.thirdeye.detection.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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


public class DefaultTimeSeriesCache implements TimeSeriesCache {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTimeSeriesCache.class);

  private static final String TIMESTAMP = "timestamp";

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final QueryCache queryCache;
  private CouchbaseCacheDAO cacheDAO = new CouchbaseCacheDAO();

  public DefaultTimeSeriesCache(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, QueryCache queryCache) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.queryCache = queryCache;
  }

  public ThirdEyeResponse fetchTimeSeries(ThirdEyeRequest thirdEyeRequest) throws Exception {
    //LOG.info("trying to fetch data from cache...");

    ThirdEyeCacheResponse cacheResponse = cacheDAO.tryFetchExistingTimeSeries(ThirdEyeCacheRequest.from(thirdEyeRequest));

    if (cacheResponse == null || cacheResponse.hasNoRows()) {
      ThirdEyeResponse dataSourceResponse = queryCache.getQueryResult(thirdEyeRequest);
      insertTimeSeriesIntoCache(dataSourceResponse);
      return dataSourceResponse;
    }

    DateTime sliceStart = thirdEyeRequest.getStartTimeInclusive();
    DateTime sliceEnd = thirdEyeRequest.getEndTimeExclusive();

    if (cacheResponse.isMissingSlice(sliceStart.getMillis(), sliceEnd.getMillis())) {
      fetchMissingSlices(cacheResponse);
    }

    return buildResponseFromCacheResponse(cacheResponse);
  }

  // NOTE: we will pretend the case where there are missing documents
  //       within the returned time-series doesn't exist.
  private void fetchMissingSlices(ThirdEyeCacheResponse cacheResponse) throws Exception {

    ThirdEyeRequest request = cacheResponse.getCacheRequest().getRequest();
    long metricId = request.getMetricFunctions().get(0).getMetricId();
    long requestSliceStart = request.getStartTimeInclusive().getMillis();
    long requestSliceEnd = request.getEndTimeExclusive().getMillis();

    ThirdEyeResponse result = null;
    MetricSlice slice = null;

    if (cacheResponse.isMissingStartSlice(requestSliceStart)) {
      slice = MetricSlice.from(metricId, requestSliceStart, cacheResponse.getFirstTimestamp(), request.getFilterSet(), request.getGroupByTimeGranularity());
      result = fetchSliceFromSource(slice);
      insertTimeSeriesIntoCache(result);
      cacheResponse.mergeSliceIntoRows(result, MergeSliceType.PREPEND);
    }

    if (cacheResponse.isMissingEndSlice(requestSliceEnd)) {
      // we add one time granularity to start because the start is inclusive.
      slice = MetricSlice.from(metricId, cacheResponse.getLastTimestamp() + request.getGroupByTimeGranularity().toMillis(), requestSliceEnd, request.getFilterSet(), request.getGroupByTimeGranularity());
      result = fetchSliceFromSource(slice);
      insertTimeSeriesIntoCache(result);
      cacheResponse.mergeSliceIntoRows(result, MergeSliceType.APPEND);
    }
  }

  private ThirdEyeResponse fetchSliceFromSource(MetricSlice slice) throws Exception {
    TimeSeriesRequestContainer rc = DataFrameUtils.makeTimeSeriesRequestAligned(slice, "ref", this.metricDAO, this.datasetDAO);
    return this.queryCache.getQueryResult(rc.getRequest());
  }

  private ThirdEyeResponse buildResponseFromCacheResponse(ThirdEyeCacheResponse cacheResponse) {

    List<String[]> rows = new ArrayList<>();
    ThirdEyeRequest request = cacheResponse.getCacheRequest().getRequest();

    String dataset = request.getMetricFunctions().get(0).getDataset();
    DatasetConfigDTO datasetDTO = datasetDAO.findByDataset(dataset);
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetDTO);
    DateTimeZone timeZone = DateTimeZone.forID(datasetDTO.getTimezone());

    for (TimeSeriesDataPoint dataPoint : cacheResponse.getRows()) {
      int timeBucketIndex = TimeRangeUtils.computeBucketIndex(
          request.getGroupByTimeGranularity(), request.getStartTimeInclusive(), new DateTime(dataPoint.getTimestamp(), timeZone));
      String dataValue = dataPoint.getDataValue();

      String[] row = new String[2];
      row[0] = String.valueOf(timeBucketIndex);
      row[1] = (dataValue == null || dataValue.equals("null")) ? "0" : dataValue;

      rows.add(row);
    }

    return new RelationalThirdEyeResponse(request, rows, timeSpec);
  }

  public void insertTimeSeriesIntoCache(ThirdEyeResponse response) {

    String dataSourceType = response.getClass().getSimpleName();

    switch (dataSourceType) {
      case "RelationalThirdEyeResponse":
        insertRelationalTimeSeries(response);
      case "CSVThirdEyeResponse":
        insertCSVTimeSeries(response);
    }
  }

  private void insertRelationalTimeSeries(ThirdEyeResponse response) {

    // use CachedThreadPool? or fixedThreadPool?
    ExecutorService executor = Executors.newCachedThreadPool();

    for (MetricFunction metric : response.getMetricFunctions()) {
      String metricUrn = MetricEntity.fromMetric(response.getRequest().getFilterSet().asMap(), metric.getMetricId()).getUrn();
      for (int i = 0; i < response.getNumRowsFor(metric); i++) {
        Map<String, String> row = response.getRow(metric, i);
        TimeSeriesDataPoint dp = new TimeSeriesDataPoint(metricUrn, Long.parseLong(row.get(CacheConstants.TIMESTAMP)), metric.getMetricId(), row.get(metric.toString()));
        executor.execute(() -> this.cacheDAO.insertTimeSeriesDataPoint(dp));
      }
    }
  }

  // fill this out later
  private void insertCSVTimeSeries(ThirdEyeResponse response) {
    return;
  }
}
