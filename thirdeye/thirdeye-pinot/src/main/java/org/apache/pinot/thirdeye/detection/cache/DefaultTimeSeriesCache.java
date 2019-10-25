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

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final QueryCache cache;
  private CouchbaseCacheDAO cacheDAO = null;

  public DefaultTimeSeriesCache(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, QueryCache cache) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.cache = cache;
    this.cacheDAO = new CouchbaseCacheDAO();
  }

  public ThirdEyeResponse fetchTimeSeries(ThirdEyeRequest thirdEyeRequest) throws Exception {
    LOG.info("trying to fetch data from cache...");

    ThirdEyeCacheResponse cacheResponse = cacheDAO.tryFetchExistingTimeSeries(ThirdEyeCacheRequest.from(thirdEyeRequest));

    if (cacheResponse == null || cacheResponse.hasNoRows()) {
      ThirdEyeResponse dataSourceResponse = cache.getQueryResult(thirdEyeRequest);
      insertTimeSeriesIntoCache(dataSourceResponse);
      return dataSourceResponse;
    }

    long sliceStart = thirdEyeRequest.getStartTimeInclusive().getMillis();
    long sliceEnd = thirdEyeRequest.getEndTimeExclusive().getMillis();

    if (cacheResponse.isMissingSlice(sliceStart, sliceEnd)) {
      fetchMissingSlices(cacheResponse);
    }

    List<String[]> rows = new ArrayList<>();
    String dataset = thirdEyeRequest.getMetricFunctions().get(0).getDataset();
    DatasetConfigDTO datasetDTO = datasetDAO.findByDataset(dataset);
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetDTO);

    for (TimeSeriesDataPoint dataPoint : cacheResponse.getRows()) {
      String[] row = new String[2];
      row[0] = String.valueOf(
          TimeRangeUtils.computeBucketIndex(
              thirdEyeRequest.getGroupByTimeGranularity(),
              thirdEyeRequest.getStartTimeInclusive(), new DateTime(dataPoint.getTimestamp(),
              DateTimeZone.forID(datasetDTO.getTimezone()))));
      row[1] = dataPoint.getDataValue();
      rows.add(row);
    }

    // make a new function that checks the type to return as and makes the corresponding ThirdEyeResponse
    return new RelationalThirdEyeResponse(thirdEyeRequest, rows, timeSpec);
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
    return this.cache.getQueryResult(rc.getRequest());
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
        TimeSeriesDataPoint dp = new TimeSeriesDataPoint(metricUrn, Long.parseLong(row.get("timestamp")), metric.getMetricId(), row.get(metric.toString()));
        executor.execute(() -> this.cacheDAO.insertTimeSeriesDataPoint(dp));
      }
    }
  }

  // fill this out later
  private void insertCSVTimeSeries(ThirdEyeResponse response) {
    return;
  }
}
