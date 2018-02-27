package com.linkedin.thirdeye.dashboard.resources.v2;

import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.TimeSeriesCompareMetricView;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.ValuesContainer;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.TimeSeriesLoader;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewHandler;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewRequest;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewHandler;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewRequest;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewResponse;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Grouping;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(value = "/timeseries")
@Produces(MediaType.APPLICATION_JSON)
public class TimeSeriesResource {
  enum TransformationType {
    CUMULATIVE,
    DIFFERENCE,
    CHANGE,
    FILLFORWARD,
    FILLZERO,
    RELATIVE,
    OFFSET,
    LOG,
  }

  enum AggregationType {
    SUM,
    PRODUCT,
    MEAN,
    MEDIAN,
    STD,
    MIN,
    MAX
  }

  private static final Map<AggregationType, Series.DoubleFunction> aggregation2function = new HashMap<>();
  static {
    aggregation2function.put(AggregationType.SUM, DoubleSeries.SUM);
    aggregation2function.put(AggregationType.PRODUCT, DoubleSeries.PRODUCT);
    aggregation2function.put(AggregationType.MEAN, DoubleSeries.MEAN);
    aggregation2function.put(AggregationType.MEDIAN, DoubleSeries.MEDIAN);
    aggregation2function.put(AggregationType.STD, DoubleSeries.STD);
    aggregation2function.put(AggregationType.MIN, DoubleSeries.MIN);
    aggregation2function.put(AggregationType.MAX, DoubleSeries.MAX);
  }

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesResource.class);
  private static final String ALL = "All";

  private static final String DECIMAL_FORMAT = "%+.1f";

  private LoadingCache<String, Long> datasetMaxDataTimeCache = CACHE_REGISTRY_INSTANCE
      .getDatasetMaxDataTimeCache();
  private QueryCache queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();

  private static final long TIMEOUT = 60000;

  public static final String COL_TIME = DataFrameUtils.COL_TIME;
  public static final String COL_VALUE = DataFrameUtils.COL_VALUE;

  private final ExecutorService executor;
  private final TimeSeriesLoader loader;

  public TimeSeriesResource(ExecutorService executor, TimeSeriesLoader loader) {
    this.executor = executor;
    this.loader = loader;
  }

  /**
   * <p>Returns metric time series for a set of given metrics and time ranges. Optionally allows
   * specification of filters, time granularity, transformations and aggregations.</p>
   *
   * <p>The result is structured hierarchically as follows:
   * <pre>
   * [time_range 0]:
   *   [metric_id 0]:
   *     value 0, value 1, ..., value n
   *   [metric_id 1]:
   *     value 0, value 1, ..., value n
   *   timestamp:
   *     timestamp 0, timestamp 1, ..., timestamp n
   * [time_range 1]:
   *   [metric_id 0]:
   *     value 0, value 1, ..., value n
   *   [metric_id 1]:
   *     value 0, value 1, ..., value n
   *   timestamp:
   *     timestamp 0, timestamp 1, ..., timestamp n
   * </pre>
   * Note that the timestamp column is always present.</p>
   *
   * <p>Transformations are applied to each time series before aggregation in the order they
   * are specified. Aggregations replace time ranges if specified, while metric ids remain
   * nested in a similar way. Note, that if one or more aggregations are specified the
   * time ranges must be of equal length.</p>
   *
   * <p>Available transformations are:
   * <pre>
   *   CUMULATIVE    rolling sum of values
   *   DIFFERENCE    differences from previous to current value
   *   CHANGE        relative changes from previous to current value
   *   FILLFORWARD   fills nulls with last non-null value (first value may still be null)
   *   FILLZERO      fills nulls with 0
   *   RELATIVE      values divided by first value of the series
   *   OFFSET        values as offset from first value of the series
   *   LOG           log10 of values
   * </pre></p>
   *
   * <p>Available aggregations are:
   * <pre>
   *   SUM           sum across time ranges of values at the same offset
   *   PRODUCT       product across time ranges
   *   MEAN          mean across time ranges
   *   MEDIAN        median across time ranges
   *   STD           standard deviation across time ranges
   *   MIN           minimum across time ranges
   *   MAX           maximum across time ranges
   * </pre></p>
   *
   * <p>Sample requests for endpoint:
   * <pre>
   * minimal example:    curl -X GET 'localhost:1426/timeseries/query?metricIds=0&ranges=1504076400000:1504162800000'
   * multiple metrics:   curl -X GET 'localhost:1426/timeseries/query?metricIds=0,1&ranges=1504076400000:1504162800000&granularity=1_HOURS'
   * transformations:    curl -X GET 'localhost:1426/timeseries/query?metricIds=0&ranges=1504076400000:1504162800000transformations=fillforward,log'
   * aggregations:       curl -X GET 'localhost:1426/timeseries/query?metricIds=0&ranges=1503990000000:1504076400000,1504076400000:1504162800000&transformations=fillforward&aggregations=sum,max'
   * </pre></p>
   *
   * @param metricIdsString metric ids separated by ","
   * @param rangesString time ranges with end exclusive "[start]:[end]" separated by ","
   * @param filterString (optional) metric filters
   * @param granularityString (optional) time series granularity "[count]_[unit]"
   * @param transformationsString (optional) transformations to apply to time series, separated by ","
   * @param aggregationsString (optional) aggregations to apply to transformed time series, separated by ","
   * @return Map (keyed by range or aggregation) of maps (keyed by metric id) of list of values
   * @throws Exception
   */
  @GET
  @Path("/query")
  public Map<String, Map<String, List<? extends Number>>> getTimeSeries(
      @QueryParam("metricIds") String metricIdsString,
      @QueryParam("ranges") String rangesString,
      @QueryParam("filters") String filterString,
      @QueryParam("granularity") String granularityString,
      @QueryParam("transformations") String transformationsString,
      @QueryParam("aggregations") String aggregationsString) throws Exception {

    // validate input
    if (StringUtils.isBlank(metricIdsString)) {
      throw new IllegalArgumentException("Must provide metricId");
    }

    List<Interval> ranges = new ArrayList<>();
    for (String range : rangesString.split(",")) {
      String[] parts = range.split(":", 2);
      long start = Long.parseLong(parts[0]);
      long end = Long.parseLong(parts[1]);

      if (end <= start) {
        throw  new IllegalArgumentException(String.format("Start (%d) must be greater than end (%d)", start, end));
      }

      ranges.add(new Interval(start, end, DateTimeZone.UTC));
    }

    if (ranges.isEmpty()) {
      throw new IllegalArgumentException("Must provide at least one time range");
    }

    TimeGranularity granularity = null;
    if (!StringUtils.isBlank(granularityString)) {
      granularity = TimeGranularity.fromString(granularityString.toUpperCase());
    }

    Multimap<String, String> filters = null;
    if (!StringUtils.isBlank(filterString)) {
      filters = ThirdEyeUtils.convertToMultiMap(filterString);
    }

    Collection<TransformationType> transformations = new ArrayList<>();
    if (!StringUtils.isBlank(transformationsString)) {
      for (String part : transformationsString.split(",")) {
        transformations.add(TransformationType.valueOf(part.toUpperCase()));
      }
    }

    Collection<AggregationType> aggregations = new ArrayList<>();
    if (!StringUtils.isBlank(aggregationsString)) {
      for (String part : aggregationsString.split(",")) {
        aggregations.add(AggregationType.valueOf(part.toUpperCase()));
      }

      // require equal-length time ranges
      Interval baseRange = ranges.get(0);
      for (Interval r : ranges) {
        if (r.toDurationMillis() != baseRange.toDurationMillis()) {
          throw new IllegalArgumentException("Must provide equal-length time ranges when using aggregation");
        }
      }
    }

    List<String> metricIds = Arrays.asList(metricIdsString.split(","));

    LOG.info("Requesting {} metrics from {} time ranges with granularity '{}', transformations '{}', aggregations '{}'",
        metricIds.size(), ranges.size(), granularity, transformations, aggregations);

    Map<MetricSlice, Future<DataFrame>> requests = new HashMap<>();
    for (String id : metricIds) {
      for (Interval range : ranges) {
        long metricId = Long.valueOf(id);
        long start = range.getStartMillis();
        long end = range.getEndMillis();
        MetricSlice slice = MetricSlice.from(metricId, start, end, filters, granularity);

        requests.put(slice, fetchMetricTimeSeriesAsync(slice));
      }
    }

    // collect results
    Map<MetricSlice, DataFrame> results = new HashMap<>();
    for (MetricSlice slice : requests.keySet()) {
      results.put(slice, requests.get(slice).get(TIMEOUT, TimeUnit.MILLISECONDS));
    }

    // merge results
    Map<Interval, DataFrame> data = mergeResults(results);

    // transform results
    for (Interval range : data.keySet()) {
      long start = range.getStartMillis();
      long end = range.getEndMillis();
      data.put(range, transformTimeSeries(data.get(range), transformations, start, end, granularity));
    }

    // aggregate results
    Map<String, Map<String, List<? extends Number>>> output = new HashMap<>();

    if (aggregations.isEmpty()) {
      // no aggregation - user time ranges as top-level keys
      for (Interval range : ranges) {
        String key = String.format("%d:%d", range.getStartMillis(), range.getEndMillis());
        output.put(key, convertDataToMap(data.get(range)));
      }

    } else {
      // with aggregation - use aggregation strings as top-level keys
      for (AggregationType aggregation : aggregations) {
        String key = aggregation.name().toLowerCase();
        output.put(key, convertDataToMap(aggregateTimeSeries(data, aggregation)));
      }
    }

    return output;
  }

  /**
   * Helper to convert a metric slice into a jodatime utc interval.
   *
   * @param slice
   * @return
   */
  private static Interval slice2interval(MetricSlice slice) {
    return new Interval(slice.getStart(), slice.getEnd(), DateTimeZone.UTC);
  }

  /**
   * Returns a map of dataframes keyed by time range by merging individual query results and aligning them to the same timestamps.
   *
   * @param results query results
   * @return map with merged dataframes keyed by time range
   */
  private static Map<Interval, DataFrame> mergeResults(Map<MetricSlice, DataFrame> results) {
    if (results.isEmpty()) {
      return Collections.emptyMap();
    }

    // NOTE: pass in intervals from outside?
    Set<Interval> timeRanges = new HashSet<>();
    for (Map.Entry<MetricSlice, DataFrame> entry : results.entrySet()) {
      timeRanges.add(slice2interval(entry.getKey()));
    }

    Map<Interval, DataFrame> output = new HashMap<>();
    for (Interval range : timeRanges) {
      for (Map.Entry<MetricSlice, DataFrame> entry : results.entrySet()) {
        MetricSlice slice = entry.getKey();
        DataFrame dfSlice = entry.getValue();

        if (!slice2interval(slice).equals(range)) {
          continue;
        }

        if (!output.containsKey(range)) {
          DataFrame df = new DataFrame();
          df.addSeries(COL_TIME, dfSlice.get(COL_TIME));
          df.setIndex(COL_TIME);
          output.put(range, df);
        }

        output.get(range).addSeries(dfSlice.renameSeries(COL_VALUE, String.valueOf(slice.getMetricId())));
      }
    }

    return output;
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
      if (data.getIndexNames().contains(name)) {
        output.put(name, data.getLongs(name).toList());
        continue;
      }
      output.put(name, data.getDoubles(name).toList());
    }
    return output;
  }

  /**
   * Returns a dataframe aggregated across intervals for multiple metrics
   *
   * @param data (transformed) query results
   * @param aggregation aggregation function to apply
   * @return dataframe with aggregated series per metric
   */
  private DataFrame aggregateTimeSeries(Map<Interval, DataFrame> data, AggregationType aggregation) {
    final Series.DoubleFunction function = aggregation2function.get(aggregation);

    // transpose interval-metrics to metric-intervals
    Multimap<String, Series> series = ArrayListMultimap.create();
    for (DataFrame df : data.values()) {
      for (String name : df.getSeriesNames()) {
        if (name.equals(COL_TIME)) {
          continue;
        }
        series.put(name, df.get(name));
      }
    }

    // aggregate across intervals
    DataFrame dfOutput = new DataFrame();
    for (String name : series.keySet()) {
      DataFrame df = new DataFrame();
      int count = 0;
      for (Series s : series.get(name)) {
        df.addSeries(String.valueOf(count), s);
        count++;
      }

      DoubleSeries out = df.map(function, df.getSeriesNames().toArray(new String[count]));

      dfOutput.addSeries(name, out);
    }

    // add index back in
    dfOutput.addSeries(COL_TIME, LongSeries.sequence(0, dfOutput.size()));
    dfOutput.setIndex(COL_TIME);

    return dfOutput;
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
      case DIFFERENCE:
        return transformTimeSeriesDifference(data);
      case FILLFORWARD:
        return transformTimeSeriesFillForward(data);
      case FILLZERO:
        return transformTimeSeriesFillZero(data);
      case CHANGE:
        return transformTimeSeriesChange(data);
      case RELATIVE:
        return transformTimeSeriesRelative(data);
      case OFFSET:
        return transformTimeSeriesOffset(data);
      case LOG:
        return transformTimeSeriesLog(data);
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
      if (data.getIndexNames().contains(id))
        continue;
      data.addSeries(id, group.aggregate(id, DoubleSeries.SUM).getValues());
    }
    return data;
  }

  /**
   * Returns time series of differences.
   *
   * @param data query results
   * @return differences time series
   */
  private DataFrame transformTimeSeriesDifference(DataFrame data) {
    for (String id : data.getSeriesNames()) {
      if (data.getIndexNames().contains(id))
        continue;
      DoubleSeries s = data.getDoubles(id);
      data.addSeries(id, s.subtract(s.shift(1)));
    }
    return data;
  }

  /**
   * Returns time series with nulls filled forward.
   *
   * @param data query results
   * @return filled time series
   */
  private DataFrame transformTimeSeriesFillForward(DataFrame data) {
    return data.fillNullForward(data.getSeriesNames().toArray(new String[0]));
  }

  /**
   * Returns time series with nulls filled with zero.
   *
   * @param data query results
   * @return filled time series
   */
  private DataFrame transformTimeSeriesFillZero(DataFrame data) {
    return data.fillNull(data.getSeriesNames().toArray(new String[0]));
  }

  /**
   * Returns time series of changes.
   *
   * @param data query results
   * @return change time series
   */
  private DataFrame transformTimeSeriesChange(DataFrame data) {
    for (String id : data.getSeriesNames()) {
      if (data.getIndexNames().contains(id))
        continue;
      DoubleSeries s = data.getDoubles(id);
      data.addSeries(id, s.divide(s.shift(1).replace(0d, DoubleSeries.NULL)).subtract(1));
    }
    return data;
  }

  /**
   * Returns time series as relative value, divided by the series' first value.
   *
   * @param data query results
   * @return filled time series
   */
  private DataFrame transformTimeSeriesRelative(DataFrame data) {
    for (String id : data.getSeriesNames()) {
      if (data.getIndexNames().contains(id))
        continue;
      DoubleSeries s = data.getDoubles(id);
      DoubleSeries sDrop = s.dropNull();
      if (sDrop.isEmpty())
        continue;
      final double base = sDrop.first().doubleValue();
      if (base != 0.0d) {
        data.addSeries(id, s.divide(base));
      } else {
        data.addSeries(id, DoubleSeries.nulls(s.size()));
      }
    }
    return data;
  }

  /**
   * Returns time series as offset from the series' first value.
   *
   * @param data query results
   * @return filled time series
   */
  private DataFrame transformTimeSeriesOffset(DataFrame data) {
    for (String id : data.getSeriesNames()) {
      if (data.getIndexNames().contains(id))
        continue;
      DoubleSeries s = data.getDoubles(id);
      DoubleSeries sDrop = s.dropNull();
      if (sDrop.isEmpty())
        continue;
      final double base = sDrop.first().doubleValue();
      data.addSeries(id, s.subtract(base));
    }
    return data;
  }

  /**
   * Returns time series log-10 transformed.
   *
   * @param data query results
   * @return filled time series
   */
  private DataFrame transformTimeSeriesLog(DataFrame data) {
    for (String id : data.getSeriesNames()) {
      if (data.getIndexNames().contains(id))
        continue;
      data.mapInPlace(new Series.DoubleFunction() {
        @Override
        public double apply(double... values) {
          if (values[0] > 0) {
            return Math.log10(values[0]);
          }
          return DoubleSeries.NULL;
        }
      }, id);
    }
    return data;
  }

  /**
   * Asynchronous call to {@code fetchMetricTimeSeries}
   * @see TimeSeriesLoader#load
   */
  private Future<DataFrame> fetchMetricTimeSeriesAsync(final MetricSlice slice) throws Exception {
    return this.executor.submit(new Callable<DataFrame>() {
      @Override
      public DataFrame call() throws Exception {
        return TimeSeriesResource.this.loader.load(slice);
      }
    });
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
