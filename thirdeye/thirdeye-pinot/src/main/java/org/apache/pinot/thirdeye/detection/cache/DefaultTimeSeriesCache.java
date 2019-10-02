package org.apache.pinot.thirdeye.detection.cache;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.auto.onboard.AutoOnboardUtility;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datasource.RelationalThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
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

  public ThirdEyeResponse fetchTimeSeries(ThirdEyeCacheRequestContainer rc) throws Exception {
    LOG.info("trying to fetch data from cache with id {}...", rc.getDetectionId());

    ThirdEyeResponse response;
    ThirdEyeCacheResponse cacheResponse = cacheDAO.tryFetchExistingTimeSeries(rc);

    DateTime start = rc.getRequest().getStartTimeInclusive();
    DateTime end = rc.getRequest().getEndTimeExclusive();

    if (cacheResponse == null || cacheResponse.isMissingSlice(start, end)) {
      LOG.info("cache miss or bad cache response received");
      response = this.cache.getQueryResult(rc.getRequest());
      // fire and forget
      ExecutorService executor = Executors.newCachedThreadPool();
      executor.execute(() -> insertTimeSeriesIntoCache(rc.getDetectionId(), response));
      //this.insertTimeSeriesIntoCache(rc.getDetectionId(), response);
    } else {
      LOG.info("cache fetch success :)");

      TimeSpec responseSpec = cacheResponse.getTimeSpec();
      Period granularityPeriod = responseSpec.getDataGranularity().toPeriod();

      DateTime cacheStart = new DateTime(Long.valueOf(cacheResponse.getStart()), start.getZone());
      DateTime cacheEnd = new DateTime(Long.valueOf(cacheResponse.getEnd()), end.getZone());

      try {
        // keep adding from beginning
        int startIndexOffset = 0;
        while (!cacheStart.isEqual(start)) {
          cacheStart = cacheStart.withPeriodAdded(granularityPeriod, 1);
          startIndexOffset++;
        }

        // keep subtracting from end
        int endIndexOffset = 0;
        while (!cacheEnd.isEqual(end)) {
          cacheEnd = cacheEnd.withPeriodAdded(granularityPeriod, -1);
          endIndexOffset++;
        }

        List<String[]> rowList = cacheResponse.getMetrics();

        List<String[]> rows = rowList.subList(startIndexOffset, rowList.size() - 1 - endIndexOffset);

        response = new RelationalThirdEyeResponse(rc.getRequest(), rows, responseSpec);
      } catch (Exception e) {
        LOG.info("requested slice between {} and {}", start, end);
        LOG.info("cache contained slice between {} and {}", cacheStart, cacheEnd);
        throw e;
      }
    }

    // TODO: Write logic to grab missing slices and merge rows later.
    // TODO: for now, just fetch the whole series and work on that logic.
    // fetch start to cacheStart - 1 => append to beginning
    // fetch cacheEnd to end => append to end
    return response;
  }


  public void insertTimeSeriesIntoCache(String detectionId, ThirdEyeResponse response) {

    String dataSourceType = response.getClass().getSimpleName();

    switch (dataSourceType) {
      case "RelationalThirdEyeResponse":
        cacheDAO.insertRelationalTimeSeries(detectionId, response);
      case "CSVThirdEyeResponse":
        // do something
    }
  }

  public boolean detectionIdExistsInCache(long detectionId) {
    return cacheDAO.checkIfDetectionIdExistsInCache(String.valueOf(detectionId));
  }
}
