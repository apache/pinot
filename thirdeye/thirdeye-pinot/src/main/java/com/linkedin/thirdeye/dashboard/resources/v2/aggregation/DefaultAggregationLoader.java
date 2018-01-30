package com.linkedin.thirdeye.dashboard.resources.v2.aggregation;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.dataframe.util.RequestContainer;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultAggregationLoader implements AggregationLoader {
  private static Logger LOG = LoggerFactory.getLogger(DefaultAggregationLoader.class);

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final QueryCache cache;

  public DefaultAggregationLoader(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, QueryCache cache) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.cache = cache;
  }

  @Override
  public DataFrame loadBreakdown(MetricSlice slice) throws Exception {
    final long metricId = slice.getMetricId();

    // fetch meta data
    MetricConfigDTO metric = this.metricDAO.findById(metricId);
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", metricId));
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s'", metric.getDataset()));
    }

    List<String> dimensions = new ArrayList<>(dataset.getDimensions());
    dimensions.removeAll(slice.getFilters().keySet());

    LOG.info("Aggregating metric id {} with {} filters for dimensions {}", metricId, slice.getFilters().size(), dimensions);

    DataFrame dfAll = DataFrame
        .builder(COL_DIMENSION_NAME + ":STRING", COL_DIMENSION_VALUE + ":STRING", COL_VALUE + ":DOUBLE").build()
        .setIndex(COL_DIMENSION_NAME, COL_DIMENSION_VALUE);

    List<DataFrame> results = new ArrayList<>();
    for (String dimension : dimensions) {
      RequestContainer rc = DataFrameUtils.makeAggregateRequest(slice, Collections.singletonList(dimension), "ref", this.metricDAO, this.datasetDAO);
      ThirdEyeResponse res = this.cache.getQueryResult(rc.getRequest());
      DataFrame dfRaw = DataFrameUtils.evaluateResponse(res, rc);
      DataFrame dfResult = new DataFrame()
          .addSeries(COL_DIMENSION_NAME, StringSeries.fillValues(dfRaw.size(), dimension))
          .addSeries(COL_DIMENSION_VALUE, dfRaw.get(dimension))
          .addSeries(COL_VALUE, dfRaw.get(COL_VALUE));
      results.add(dfResult);
    }

    return dfAll.append(results);
  }

  @Override
  public double loadAggregate(MetricSlice slice) throws Exception {
    final long metricId = slice.getMetricId();

    // fetch meta data
    MetricConfigDTO metric = this.metricDAO.findById(metricId);
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", metricId));
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s'", metric.getDataset()));
    }

    LOG.info("Summarizing metric id {} with {} filters", metricId, slice.getFilters().size());

    RequestContainer rc = DataFrameUtils.makeAggregateRequest(slice, Collections.<String>emptyList(), "ref", this.metricDAO, this.datasetDAO);
    ThirdEyeResponse res = this.cache.getQueryResult(rc.getRequest());
    DataFrame df = DataFrameUtils.evaluateResponse(res, rc);

    final long maxTime = this.cache.getDataSource(dataset.getDataSource()).getMaxDataTime(dataset.getDataset());
    if (slice.getStart() > maxTime) {
      return Double.NaN;
    }

    return df.getDoubles(COL_VALUE).fillNull().doubleValue();
  }
}
