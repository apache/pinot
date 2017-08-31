package com.linkedin.thirdeye.dashboard.resources.v2;

import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.TimeSeriesCompareMetricView;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.ValuesContainer;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewHandler;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewRequest;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewHandler;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewRequest;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewResponse;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Grouping;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(value = "/timeseries")
@Produces(MediaType.APPLICATION_JSON)
public class TimeSeriesResource {
  enum TransformationType {
    CUMULATIVE,
    FORWARDFILL,
    MILLISECONDS
  }

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesResource.class);
  private static final String ALL = "All";

  public static final String DECIMAL_FORMAT = "%+.1f";

  private LoadingCache<String, Long> datasetMaxDataTimeCache = CACHE_REGISTRY_INSTANCE
      .getDatasetMaxDataTimeCache();
  private QueryCache queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();

  private static final long TIMEOUT = 60000;
  private static final String COL_TIME = DataFrameUtils.COL_TIME;
  private static final String COL_VALUE = DataFrameUtils.COL_VALUE;
  private static final String SERIES_PREFIX = "_";

  private final ExecutorService executor;

  public TimeSeriesResource(ExecutorService executor) {
    this.executor = executor;
  }

  @GET
  @Path("/query")
  public Map<String, List<? extends Number>> getTimeSeries(
      @QueryParam("metricIds") String metricIds,
      @QueryParam("start") Long start,
      @QueryParam("end") Long end,
      @QueryParam("filters") String filterString,
      @QueryParam("granularity") String granularityString,
      @QueryParam("transformations") String transformationsString) throws Exception {

    // validate input
    if (metricIds == null) {
      throw new IllegalArgumentException("Must provide metricId");
    }

    if (start == null) {
      throw new IllegalArgumentException("Must provide start timestamp");
    }

    if (end == null) {
      throw new IllegalArgumentException("Must provide end timestamp");
    }

    if (end <= start) {
      throw  new IllegalArgumentException("Start must be greater than end");
    }

    TimeGranularity granularity = null;
    if (granularityString != null) {
      granularity = TimeGranularity.fromString(granularityString.toUpperCase());
    }

    Multimap<String, String> filters = null;
    if (filterString != null) {
      filters = ThirdEyeUtils.convertToMultiMap(filterString);
    }

    Collection<TransformationType> transformations = new ArrayList<>();
    if (transformationsString != null && !transformationsString.isEmpty()) {
      String[] parts = transformationsString.split(",");
      for (String part : parts) {
        transformations.add(TransformationType.valueOf(part.toUpperCase()));
      }
    }

    // make requests
    Map<String, Future<DataFrame>> requests = new HashMap<>();
    String[] ids = metricIds.split(",");
    LOG.info("Requesting {} time series from {} to {} with granularity '{}'", ids.length, start, end, granularity);

    for (String id : ids) {
      long metricId = Long.valueOf(id);
      requests.put(id, fetchMetricTimeSeriesAsync(metricId, start, end, granularity, filters));
    }

    // collect results
    Map<String, DataFrame> results = new HashMap<>();
    for (String id : requests.keySet()) {
      results.put(id, requests.get(id).get(TIMEOUT, TimeUnit.MILLISECONDS));
    }

    // merge results
    DataFrame data = mergeResults(results);

    // transform output
    data = transformTimeSeries(data, transformations, start, end, granularity);

    return convertDataToMap(data);
  }

  /**
   * Returns a DataFrame merging individual query results and aligning them to the same timestamps.
   *
   * @param results query results
   * @return merged aligned dataframe
   */
  private static DataFrame mergeResults(Map<String, DataFrame> results) {
    DataFrame df = new DataFrame();
    if (results.isEmpty())
      return df;

    // TODO move timestamp generation here
    Series timestamp = results.values().iterator().next().get(COL_TIME);
    df.addSeries(COL_TIME, timestamp);
    df.setIndex(COL_TIME);

    for (Map.Entry<String, DataFrame> entry : results.entrySet()) {
      String name = SERIES_PREFIX + entry.getKey();
      DataFrame res = new DataFrame(entry.getValue()).renameSeries(COL_VALUE, name);
      df.addSeries(res);
    }

    return df;
  }

  /**
   * Returns a map of time series (keyed by series name) derived from the results dataframe.
   *
   * @param data (transformed) query results
   * @return map of lists of double (keyed by series name)
   */
  private static Map<String, List<? extends Number>> convertDataToMap(DataFrame data) {
    Map<String, List<? extends Number>> output = new HashMap<>();
    for (String name : data.getSeriesNames()) {
      String outName = name;
      if (outName.startsWith(SERIES_PREFIX)) {
        outName = outName.substring(SERIES_PREFIX.length());
      }

      if (data.getIndexName().equals(name)) {
        output.put(outName, data.getLongs(name).toList());
        continue;
      }

      output.put(outName, data.getDoubles(name).toList());
    }
    return output;
  }

  /**
   * Returns time series transformed by {@code transformations} (in order).
   *
   * @see TimeSeriesResource#transformTimeSeries(DataFrame, TransformationType, long, long, TimeGranularity)
   */
  private DataFrame transformTimeSeries(DataFrame data, Collection<TransformationType> transformations, long start, long end, TimeGranularity granularity) {
    for (TransformationType t : transformations) {
      data = transformTimeSeries(data, t, start, end, granularity);
    }
    return data;
  }

  /**
   * Returns time series transformed by {@code transformation}.
   *
   * @param data query results
   * @param transformation transformation to apply
   * @param start start time stamp
   * @param end end time stamp
   * @param granularity time granularity
   * @return transformed time series
   */
  private DataFrame transformTimeSeries(DataFrame data, TransformationType transformation, long start, long end, TimeGranularity granularity) {
    switch (transformation) {
      case CUMULATIVE:
        return transformTimeSeriesCumulative(data);
      case FORWARDFILL:
        return transformTimeSeriesForwardFill(data);
      case MILLISECONDS:
        return transformTimeSeriesMilliseconds(data, start, granularity);
    }
    throw new IllegalArgumentException(String.format("Unknown transformation type '%s'", transformation));
  }

  /**
   * Returns time series of cumulative values.
   *
   * @param data query results
   * @return cumulative time series
   */
  private DataFrame transformTimeSeriesCumulative(DataFrame data) {
    Grouping.DataFrameGrouping group = data.groupByExpandingWindow();
    for (String id : data.getSeriesNames()) {
      if (data.getIndexName().equals(id))
        continue;
      data.addSeries(id, group.aggregate(id, DoubleSeries.SUM).getValues());
    }
    return data;
  }

  /**
   * Returns time series with nulls filled forward.
   *
   * @param data query results
   * @return filled time series
   */
  private DataFrame transformTimeSeriesForwardFill(DataFrame data) {
    return data.fillNullForward(data.getSeriesNames().toArray(new String[0]));
  }

  /**
   * Returns time series with time stamps in milliseconds (rather than indices)
   *
   * @param data query results
   * @param start start time offset in millis
   * @param granularity time granularity of rows
   * @return data series with millisecond timestamps
   */
  private DataFrame transformTimeSeriesMilliseconds(DataFrame data, long start, final TimeGranularity granularity) {
    final long offset = granularity.toMillis(granularity.convertToUnit(start));
    data.mapInPlace(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return granularity.toMillis(values[0]) + offset;
      }
    }, data.getIndexName());
    return data;
  }

  /**
   * Asynchronous call to {@code fetchMetricTimeSeries}
   * @see TimeSeriesResource#fetchMetricTimeSeries
   */
  private Future<DataFrame> fetchMetricTimeSeriesAsync(final long metricId, final long start,
      final long end, final TimeGranularity granularity, final Multimap<String, String> filters) throws Exception {
    return this.executor.submit(new Callable<DataFrame>() {
      @Override
      public DataFrame call() throws Exception {
        return fetchMetricTimeSeries(metricId, start, end, granularity, filters);
      }
    });
  }

  /**
   * Returns the metric time series for a given time range and filter set, with a specified
   * time granularity. If the underlying time series resolution does not correspond to the desired
   * time granularity, it is up-sampled (via forward fill) or down-sampled (via sum if additive, or
   * last value otherwise) transparently.
   *
   * <br/><b>NOTE:</b> if the start timestamp does not align with the time
   * resolution, it is aligned with the nearest lower time stamp.
   *
   * @param metricId metric id in thirdeye database
   * @param start start time stamp (inclusive, in millis)
   * @param end end time stamp (exclusive, in millis)
   * @param granularity time granularity
   * @param filters filter set
   * @return dataframe with aligned timestamps and values
   */
  private static DataFrame fetchMetricTimeSeries(long metricId, long start, long end,
      TimeGranularity granularity, Multimap<String, String> filters) throws Exception {

    // fetch meta data
    MetricConfigDTO metric = DAO_REGISTRY.getMetricConfigDAO().findById(metricId);
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", metricId));
    }

    DatasetConfigDTO dataset = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s'", metric.getDataset()));
    }

    if (granularity == null) {
      granularity = dataset.bucketTimeGranularity();
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
    ThirdEyeResponse response = CACHE_REGISTRY_INSTANCE.getQueryCache().getQueryResult(request);

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

  @GET
  @Path("/compare/{metricId}/{currentStart}/{currentEnd}/{baselineStart}/{baselineEnd}")
  public TimeSeriesCompareMetricView getTimeseriesCompareData(
      @PathParam("metricId") long metricId, @PathParam("currentStart") long currentStart,
      @PathParam("currentEnd") long currentEnd, @PathParam("baselineStart") long baselineStart,
      @PathParam("baselineEnd") long baselineEnd, @QueryParam("dimension") String dimension,
      @QueryParam("filters") String filters, @QueryParam("granularity") String granularity,
      // Auto Resize tries to limit the number of data points to 20
      @DefaultValue("false")@QueryParam("limitDataPointNum") boolean limitDataPointNum,
      @DefaultValue("20")@QueryParam("dataPointNum") int dataPointNum) {

    try {
      if (Strings.isNullOrEmpty(dimension)) {
        dimension = ALL;
      }

      MetricConfigDTO metricConfigDTO = DAO_REGISTRY.getMetricConfigDAO().findById(metricId);
      String dataset = metricConfigDTO.getDataset();
      DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
      long maxDataTime = datasetMaxDataTimeCache.get(dataset);

      currentEnd = roundEndTimeByGranularity(currentEnd, datasetConfig);
      baselineEnd = roundEndTimeByGranularity(baselineEnd, datasetConfig);
      if (currentEnd > maxDataTime) {
        long delta = currentEnd - maxDataTime;
        currentEnd = currentEnd - delta;
        baselineEnd = baselineStart + (currentEnd - currentStart);
      }

      long analysisDuration = currentEnd - currentStart;
      if (baselineEnd - baselineStart != analysisDuration) {
        baselineEnd = baselineStart + analysisDuration;
      }
      if (baselineEnd > currentEnd) {
        LOG.warn("Baseline time ranges are out of order, resetting as per current time ranges.");
        baselineEnd = currentEnd - TimeUnit.DAYS.toMillis(7);
        baselineStart = currentStart - TimeUnit.DAYS.toMillis(7);
      }

      if (StringUtils.isEmpty(granularity)) {
        granularity = "DAYS";
      }

      if (limitDataPointNum) {
        granularity = Utils.resizeTimeGranularity(analysisDuration, granularity, dataPointNum);
      }

      if (dimension.equalsIgnoreCase(ALL)) {
       return getTabularData(metricId, currentStart, currentEnd, baselineStart, baselineEnd, filters,
                granularity);
      } else {
        // build contributor view request
        return getContributorDataForDimension(metricId, currentStart, currentEnd, baselineStart,
            baselineEnd, dimension, filters, granularity);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new WebApplicationException(e);
    }
  }

  private long roundEndTimeByGranularity(long endTime, DatasetConfigDTO datasetConfig) {
    TimeGranularity bucketTimeGranularity = datasetConfig.bucketTimeGranularity();
    TimeUnit timeUnit = bucketTimeGranularity.getUnit();
    int timeSize = bucketTimeGranularity.getSize();
    DateTimeZone dateTimeZone = Utils.getDataTimeZone(datasetConfig.getDataset());
    DateTime adjustedDateTime = new DateTime(endTime, dateTimeZone);
    switch (timeUnit) {
      case DAYS:
        adjustedDateTime = adjustedDateTime.withTimeAtStartOfDay();
        break;
      case MINUTES:
        int roundedMinutes = (adjustedDateTime.getMinuteOfHour()/timeSize) * timeSize;
        adjustedDateTime = adjustedDateTime.withTime(adjustedDateTime.getHourOfDay(), roundedMinutes, 0, 0);
        break;
      case HOURS:
      default:
        adjustedDateTime = adjustedDateTime.withTime(adjustedDateTime.getHourOfDay(), 0, 0, 0);
        break;
    }
    return adjustedDateTime.getMillis();
  }

  private TimeSeriesCompareMetricView getContributorDataForDimension(long metricId,
      long currentStart, long currentEnd, long baselineStart, long baselineEnd, String dimension,
      String filters, String granularity) {

    MetricConfigDTO metricConfigDTO = DAO_REGISTRY.getMetricConfigDAO().findById(metricId);
    TimeSeriesCompareMetricView timeSeriesCompareMetricView =
        new TimeSeriesCompareMetricView(metricConfigDTO.getName(), metricId, currentStart,
            currentEnd, metricConfigDTO.isInverseMetric());

    try {
      String dataset = metricConfigDTO.getDataset();
      ContributorViewRequest request = new ContributorViewRequest();
      request.setCollection(dataset);

      MetricExpression metricExpression =
          ThirdEyeUtils.getMetricExpressionFromMetricConfig(metricConfigDTO);
      request.setMetricExpressions(Arrays.asList(metricExpression));

      DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(dataset);
      request.setBaselineStart(new DateTime(baselineStart, timeZoneForCollection));
      request.setBaselineEnd(new DateTime(baselineEnd, timeZoneForCollection));
      request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
      request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));

      request.setTimeGranularity(Utils.getAggregationTimeGranularity(granularity, dataset));
      if (filters != null && !filters.isEmpty()) {
        filters = URLDecoder.decode(filters, "UTF-8");
        request.setFilters(ThirdEyeUtils.convertToMultiMap(filters));
      }

      request.setGroupByDimensions(Arrays.asList(dimension));

      ContributorViewHandler handler = new ContributorViewHandler(queryCache);
      ContributorViewResponse response = handler.process(request);

      // Assign the time buckets
      List<Long> timeBucketsCurrent = new ArrayList<>();
      List<Long> timeBucketsBaseline = new ArrayList<>();

      timeSeriesCompareMetricView.setTimeBucketsCurrent(timeBucketsCurrent);
      timeSeriesCompareMetricView.setTimeBucketsBaseline(timeBucketsBaseline);

      Map<String, ValuesContainer> subDimensionValuesMap = new LinkedHashMap<>();
      timeSeriesCompareMetricView.setSubDimensionContributionMap(subDimensionValuesMap);

      int timeBuckets = response.getTimeBuckets().size();
      // this is for over all values
      ValuesContainer vw = new ValuesContainer();
      subDimensionValuesMap.put(ALL, vw);
      vw.setCurrentValues(new double[timeBuckets]);
      vw.setBaselineValues(new double[timeBuckets]);
      vw.setPercentageChange(new String[timeBuckets]);
      vw.setCumulativeCurrentValues(new double[timeBuckets]);
      vw.setCumulativeBaselineValues(new double[timeBuckets]);
      vw.setCumulativePercentageChange(new String[timeBuckets]);

      // lets find the indices
      int subDimensionIndex =
          response.getResponseData().getSchema().getColumnsToIndexMapping().get("dimensionValue");
      int currentValueIndex =
          response.getResponseData().getSchema().getColumnsToIndexMapping().get("currentValue");
      int baselineValueIndex =
          response.getResponseData().getSchema().getColumnsToIndexMapping().get("baselineValue");
      int percentageChangeIndex =
          response.getResponseData().getSchema().getColumnsToIndexMapping().get("percentageChange");
      int cumCurrentValueIndex = response.getResponseData().getSchema().getColumnsToIndexMapping().get("cumulativeCurrentValue");
      int cumBaselineValueIndex = response.getResponseData().getSchema().getColumnsToIndexMapping().get("cumulativeBaselineValue");
      int cumPercentageChangeIndex = response.getResponseData().getSchema().getColumnsToIndexMapping().get("cumulativePercentageChange");

      // populate current and baseline time buckets
      for (int i = 0; i < timeBuckets; i++) {
        TimeBucket tb = response.getTimeBuckets().get(i);
        timeBucketsCurrent.add(tb.getCurrentStart());
        timeBucketsBaseline.add(tb.getBaselineStart());
      }

      // set current and baseline values for sub dimensions
      for (int i = 0; i < response.getResponseData().getResponseData().size(); i++) {
        String[] data = response.getResponseData().getResponseData().get(i);
        String subDimension = data[subDimensionIndex];
        Double currentVal = Double.valueOf(data[currentValueIndex]);
        Double baselineVal = Double.valueOf(data[baselineValueIndex]);
        Double percentageChangeVal = Double.valueOf(data[percentageChangeIndex]);
        Double cumCurrentVal = Double.valueOf(data[cumCurrentValueIndex]);
        Double cumBaselineVal = Double.valueOf(data[cumBaselineValueIndex]);
        Double cumPercentageChangeVal = Double.valueOf(data[cumPercentageChangeIndex]);

        int index = i % timeBuckets;

        // set overAll values
        vw.getCurrentValues()[index] += currentVal;
        vw.getBaselineValues()[index] += baselineVal;
        vw.getCumulativeCurrentValues()[index] += cumCurrentVal;
        vw.getCumulativeBaselineValues()[index] += cumBaselineVal;

        // set individual sub-dimension values
        if (!subDimensionValuesMap.containsKey(subDimension)) {
          ValuesContainer subDimVals = new ValuesContainer();
          subDimVals.setCurrentValues(new double[timeBuckets]);
          subDimVals.setBaselineValues(new double[timeBuckets]);
          subDimVals.setPercentageChange(new String[timeBuckets]);
          subDimVals.setCumulativeCurrentValues(new double[timeBuckets]);
          subDimVals.setCumulativeBaselineValues(new double[timeBuckets]);
          subDimVals.setCumulativePercentageChange(new String[timeBuckets]);
          subDimensionValuesMap.put(subDimension, subDimVals);
        }

        subDimensionValuesMap.get(subDimension).getCurrentValues()[index] = currentVal;
        subDimensionValuesMap.get(subDimension).getBaselineValues()[index] = baselineVal;
        subDimensionValuesMap.get(subDimension).getPercentageChange()[index] = String.format(DECIMAL_FORMAT, percentageChangeVal);
        subDimensionValuesMap.get(subDimension).getCumulativeCurrentValues()[index] = cumCurrentVal;
        subDimensionValuesMap.get(subDimension).getCumulativeBaselineValues()[index] = cumBaselineVal;
        subDimensionValuesMap.get(subDimension).getCumulativePercentageChange()[index] = String.format(DECIMAL_FORMAT, cumPercentageChangeVal);
      }

      // Now compute percentage change for all values
      // TODO : compute cumulative values for all
      for (int i = 0; i < vw.getCurrentValues().length; i++) {
        vw.getPercentageChange()[i] = String.format(DECIMAL_FORMAT,
            getPercentageChange(vw.getCurrentValues()[i], vw.getBaselineValues()[i]));
        vw.getCumulativePercentageChange()[i] = String.format(DECIMAL_FORMAT,
            getPercentageChange(vw.getCumulativeCurrentValues()[i],
                vw.getCumulativeBaselineValues()[i]));
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new WebApplicationException(e);
    }
    return timeSeriesCompareMetricView;
  }

  private double getPercentageChange(double current, double baseline) {
    if (baseline == 0d) {
      if (current == 0d) {
        return 0d;
      } else {
        return 100d;
      }
    } else {
      return 100 * (current - baseline) / baseline;
    }
  }

  /**
   * used when dimension is not passed, i.e. data is requested for all dimensions.
   * @param metricId
   * @param currentStart
   * @param currentEnd
   * @param baselineStart
   * @param baselineEnd
   * @param filters
   * @param granularity
   * @return
   */
  private TimeSeriesCompareMetricView getTabularData(long metricId, long currentStart, long currentEnd,
      long baselineStart, long baselineEnd, String filters, String granularity) {
    TimeSeriesCompareMetricView timeSeriesCompareView = new TimeSeriesCompareMetricView();
    try {
      MetricConfigDTO metricConfigDTO = DAO_REGISTRY.getMetricConfigDAO().findById(metricId);
      if (metricConfigDTO != null) {
        String dataset = metricConfigDTO.getDataset();

        TabularViewRequest request = new TabularViewRequest();
        request.setCollection(dataset);

        MetricExpression metricExpression =
            ThirdEyeUtils.getMetricExpressionFromMetricConfig(metricConfigDTO);
        request.setMetricExpressions(Arrays.asList(metricExpression));

        DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(dataset);
        request.setBaselineStart(new DateTime(baselineStart, timeZoneForCollection));
        request.setBaselineEnd(new DateTime(baselineEnd, timeZoneForCollection));
        request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
        request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));

        request.setTimeGranularity(Utils.getAggregationTimeGranularity(granularity, dataset));
        if (filters != null && !filters.isEmpty()) {
          filters = URLDecoder.decode(filters, "UTF-8");
          request.setFilters(ThirdEyeUtils.convertToMultiMap(filters));
        }
        TabularViewHandler handler = new TabularViewHandler(queryCache);
        TabularViewResponse response = handler.process(request);

        timeSeriesCompareView.setStart(currentStart);
        timeSeriesCompareView.setEnd(currentEnd);
        timeSeriesCompareView.setMetricId(metricConfigDTO.getId());
        timeSeriesCompareView.setMetricName(metricConfigDTO.getName());
        timeSeriesCompareView.setInverseMetric(metricConfigDTO.isInverseMetric());

        List<Long> timeBucketsCurrent = new ArrayList<>();
        List<Long> timeBucketsBaseline = new ArrayList<>();

        int numTimeBuckets = response.getTimeBuckets().size();

        double [] currentValues = new double[numTimeBuckets];
        double [] baselineValues = new double[numTimeBuckets];
        String [] percentageChangeValues = new String[numTimeBuckets];

        double [] cumCurrentValues = new double[numTimeBuckets];
        double [] cumBaselineValues = new double[numTimeBuckets];
        String [] cumPercentageChangeValues = new String[numTimeBuckets];

        int currentValIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("currentValue");
        int baselineValIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("baselineValue");
        int percentageChangeIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("ratio");

        int cumCurrentValIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("cumulativeCurrentValue");
        int cumBaselineValIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("cumulativeBaselineValue");
        int cumPercentageChangeIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("cumulativeRatio");

        for (int i = 0; i < numTimeBuckets; i++) {
          TimeBucket tb = response.getTimeBuckets().get(i);
          timeBucketsCurrent.add(tb.getCurrentStart());
          timeBucketsBaseline.add(tb.getBaselineStart());

          currentValues[i] = Double.valueOf(
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[currentValIndex]);
          baselineValues[i] = Double.valueOf(
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[baselineValIndex]);
          percentageChangeValues[i] =
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[percentageChangeIndex];

          cumCurrentValues[i] = Double.valueOf(
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[cumCurrentValIndex]);
          cumBaselineValues[i] = Double.valueOf(
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[cumBaselineValIndex]);
          cumPercentageChangeValues[i] =
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[cumPercentageChangeIndex];
        }

        timeSeriesCompareView.setTimeBucketsCurrent(timeBucketsCurrent);
        timeSeriesCompareView.setTimeBucketsBaseline(timeBucketsBaseline);
        ValuesContainer values = new ValuesContainer();

        values.setCurrentValues(currentValues);
        values.setBaselineValues(baselineValues);
        values.setPercentageChange(percentageChangeValues);
        values.setCumulativeCurrentValues(cumCurrentValues);
        values.setCumulativeBaselineValues(cumBaselineValues);
        values.setCumulativePercentageChange(cumPercentageChangeValues);

        timeSeriesCompareView.setSubDimensionContributionMap(new LinkedHashMap<String, ValuesContainer>());
        timeSeriesCompareView.getSubDimensionContributionMap().put(ALL, values);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new WebApplicationException(e);
    }
    return timeSeriesCompareView;
  }
}
