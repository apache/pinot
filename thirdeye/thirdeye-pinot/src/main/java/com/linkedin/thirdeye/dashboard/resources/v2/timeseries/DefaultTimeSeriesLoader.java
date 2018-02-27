package com.linkedin.thirdeye.dashboard.resources.v2.timeseries;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.dataframe.util.TimeSeriesRequestContainer;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultTimeSeriesLoader implements TimeSeriesLoader {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTimeSeriesLoader.class);

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final QueryCache cache;

  public DefaultTimeSeriesLoader(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, QueryCache cache) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.cache = cache;
  }

  /**
   * Default implementation using metricDAO, datasetDAO, and QueryCache
   *
   * @param slice metric slice to fetch
   * @return Dataframe with timestamps and metric values
   * @throws Exception
   */
  @Override
  public DataFrame load(MetricSlice slice) throws Exception {
    LOG.info("Loading timeseries for metric id {} time range {}:{} with filters '{}' and granularity {}",
        slice.getMetricId(), slice.getStart(), slice.getEnd(), slice.getFilters(), slice.getGranularity());

    TimeSeriesRequestContainer rc = DataFrameUtils.makeTimeSeriesRequestAligned(slice, "ref", this.metricDAO, this.datasetDAO);
    ThirdEyeResponse response = this.cache.getQueryResult(rc.getRequest());
    return DataFrameUtils.evaluateResponse(response, rc);
  }
}
