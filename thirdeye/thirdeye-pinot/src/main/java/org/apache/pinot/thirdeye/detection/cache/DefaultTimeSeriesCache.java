package org.apache.pinot.thirdeye.detection.cache;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.auto.onboard.AutoOnboardUtility;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.RelationalThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.CacheUtils;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultTimeSeriesCache implements TimeSeriesCache {

  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardUtility.class);

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

    DatasetConfigDTO dto = datasetDAO.findByDataset(thirdEyeRequest.getDataSource());
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(dto);

    ThirdEyeCacheResponse cacheResponse = cacheDAO.tryFetchExistingTimeSeries(ThirdEyeCacheRequest.from(thirdEyeRequest));

    if (cacheResponse == null|| cacheResponse.hasNoRows()) {
      ThirdEyeResponse dataSourceResponse = cache.getQueryResult(thirdEyeRequest);
      insertTimeSeriesIntoCache(dataSourceResponse);
      return dataSourceResponse;
    }

    long sliceStart = thirdEyeRequest.getStartTimeInclusive().getMillis();
    long sliceEnd = thirdEyeRequest.getEndTimeExclusive().getMillis();

    if (cacheResponse.isMissingSlice(sliceStart, sliceEnd)) {
      fetchMissingSlices(cacheResponse);
    }

    return null;
  }

  private void fetchMissingSlices(ThirdEyeCacheResponse response) {
    long sliceStart = response.getStartInclusive();
    long sliceEnd = response.getEndExclusive();
    if (response.isMissingStartAndEndSlice(sliceStart, sliceEnd)) {
      // refetch whole series?
    } else if (response.isMissingStartSlice(sliceStart)) {
      // add to list
    } else if (response.isMissingEndSlice(sliceEnd)) {

    }
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
    ExecutorService executor = Executors.newCachedThreadPool();

    RelationalThirdEyeResponse thirdEyeResponse = (RelationalThirdEyeResponse)response;

    // need to revise this, since the loop condition looks weird, but the actual code for
    // relationalthirdeyeresponse makes no sense either

//    for (MetricFunction metric : response.getMetricFunctions()) {
//      String metricUrn = MetricEntity.fromMetric(response.getRequest().getFilterSet().asMap(), metric.getMetricId()).getUrn();
//      for (String[] dataPoint : thirdEyeResponse.getRows()) {
//        TimeSeriesDataPoint dp = TimeSeriesDataPoint.from(dataPoint, metricUrn);
//        executor.execute(() -> cacheDAO.insertTimeSeriesDataPoint(dp));
//      }
//    }

    String metricUrn = MetricEntity.fromMetric(response.getRequest().getFilterSet().asMap(), response.getMetricFunctions().get(0).getMetricId()).getUrn();
    for (String[] dataPoint : thirdEyeResponse.getRows()) {
      TimeSeriesDataPoint dp = TimeSeriesDataPoint.from(dataPoint, metricUrn);
      executor.execute(() -> cacheDAO.insertTimeSeriesDataPoint(dp));
    }
  }

  // fill this out later
  private void insertCSVTimeSeries(ThirdEyeResponse response) {
    return;
  }
}
