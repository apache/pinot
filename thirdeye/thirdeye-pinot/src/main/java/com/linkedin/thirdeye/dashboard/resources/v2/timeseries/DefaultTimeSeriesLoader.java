package com.linkedin.thirdeye.dashboard.resources.v2.timeseries;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.resources.v2.TimeSeriesResource;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultTimeSeriesLoader implements TimeSeriesLoader {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTimeSeriesLoader.class);

  private static final String COL_TIME = TimeSeriesResource.COL_TIME;
  private static final String COL_VALUE = TimeSeriesResource.COL_VALUE;

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
   * @param granularity time granularity
   * @return Dataframe with timestamps and metric values
   * @throws Exception
   */
  @Override
  public DataFrame load(MetricSlice slice, TimeGranularity granularity) throws Exception {
    final long metricId = slice.getMetricId();
    final long start = slice.getStart();
    final long end = slice.getEnd();
    final Multimap<String, String> filters = slice.getFilters();

    // fetch meta data
    MetricConfigDTO metric = this.metricDAO.findById(metricId);
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", metricId));
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s'", metric.getDataset()));
    }

    if (granularity == null) {
      throw new IllegalArgumentException("Must provide time granularity");
    }

    List<MetricFunction> functions = new ArrayList<>();
    List<MetricExpression> expressions = Arrays.asList(ThirdEyeUtils.getMetricExpressionFromMetricConfig(metric));
    for(MetricExpression exp : expressions) {
      functions.addAll(exp.computeMetricFunctions());
    }

    // aligned timestamps
    // NOTE: the method over-fetches data in front of the time series to fill-forward correctly
    final long alignedStart = granularity.toMillis(granularity.convertToUnit(start));
    final long dataAlignedStart = dataset.bucketTimeGranularity().toMillis(
        dataset.bucketTimeGranularity().convertToUnit(start));

    // build request
    ThirdEyeRequest.ThirdEyeRequestBuilder builder = ThirdEyeRequest.newBuilder()
        .setStartTimeInclusive(dataAlignedStart)
        .setEndTimeExclusive(end)
        .setMetricFunctions(functions)
        .setGroupBy(dataset.getTimeColumn())
        .setGroupByTimeGranularity(granularity)
        .setDataSource(dataset.getDataSource());

    if (filters != null) {
      builder.setFilterSet(filters);
    }

    ThirdEyeRequest request = builder.build("ref");

    // fetch result
    ThirdEyeResponse response = this.cache.getQueryResult(request);

    DataFrame df = DataFrameUtils.parseResponse(response);
    DataFrameUtils.evaluateExpressions(df, expressions);

    // generate time stamps
    DataFrame output = new DataFrame();
    output.addSeries(COL_TIME, makeTimeRangeIndex(dataAlignedStart, end, granularity));
    output.setIndex(COL_TIME);

    LOG.info("Metric id {} has {} data points for time range {}-{}, need {} for time range {}-{}", metricId, df.size(), dataAlignedStart, end, output.size(), alignedStart, end);

    // handle down-sampling - group by timestamp
    output.addSeries(df.groupByValue(COL_TIME).aggregate(COL_VALUE, getGroupingFunction(dataset)));

    // NOTE: up-sampling handled outside

    // project onto (aligned) start timestamp
    int fromIndex = (int) granularity.convertToUnit(alignedStart - dataAlignedStart);
    if (fromIndex > 0) {
      LOG.info("Metric id {} resampling over-generated {} data points (out of {}). Truncating.", metricId, fromIndex, output.size());
      output = output.sliceFrom(fromIndex);

      LongSeries timestamps = output.getLongs(COL_TIME);
      output.addSeries(COL_TIME, timestamps.subtract(timestamps.min()));
    }

    return output;
  }

  /**
   * Returns a LongSeries with length {@code N}, where {@code N} corresponds to the number of time
   * buckets between {@code start} and {@code end} with the given time granularity.
   *
   * @param start start timestamp (inclusive, in millis)
   * @param end end timestamp (exclusive, in millis)
   * @param granularity time granularity
   * @return long series
   */
  private static LongSeries makeTimeRangeIndex(long start, long end, TimeGranularity granularity) {
    long roundUp = granularity.toMillis(1) - 1;
    int maxCount = (int) granularity.convertToUnit(end - start + roundUp);
    long[] values = new long[maxCount];
    for(int i=0; i<maxCount; i++) {
      values[i] = i;
    }
    return LongSeries.buildFrom(values);
  }

  /**
   * Returns the grouping function based on whether the dataset is additive or not.
   *
   * @param dataset dataset config
   * @return grouping function
   */
  private static Series.DoubleFunction getGroupingFunction(DatasetConfigDTO dataset) {
    if(dataset.isAdditive())
      return DoubleSeries.SUM;
    return DoubleSeries.LAST;
  }
}
