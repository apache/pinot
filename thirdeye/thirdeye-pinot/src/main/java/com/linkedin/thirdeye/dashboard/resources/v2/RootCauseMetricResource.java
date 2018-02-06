package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.AggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.TimeSeriesLoader;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineOffset;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>RootCauseMetricResource is a central endpoint for querying different views on metrics as used by the
 * RCA frontend. It delivers metric timeseries, aggregates, and breakdowns (de-aggregations).
 * The endpoint parses metric urns and a unified set of "offsets", i.e. time-warped baseline of the
 * specified metric. It further aligns queried time stamps to sensibly match the raw dataset.</p>
 *
 * @see RootCauseMetricResource#parseOffset(MetricSlice, String, String) supported offsets
 */
@Path(value = "/rootcause/metric")
@Produces(MediaType.APPLICATION_JSON)
public class RootCauseMetricResource {
  private static final Logger LOG = LoggerFactory.getLogger(RootCauseMetricResource.class);

  private static final String COL_TIME = TimeSeriesLoader.COL_TIME;
  private static final String COL_VALUE = TimeSeriesLoader.COL_VALUE;
  private static final String COL_DIMENSION_NAME = AggregationLoader.COL_DIMENSION_NAME;
  private static final String COL_DIMENSION_VALUE = AggregationLoader.COL_DIMENSION_VALUE;

  private static final long TIMEOUT = 60000;

  private static final String OFFSET_DEFAULT = "current";
  private static final String TIMEZONE_DEFAULT = "UTC";
  private static final String GRANULARITY_DEFAULT = MetricSlice.NATIVE_GRANULARITY.toAggregationGranularityString();

  private static final Pattern PATTERN_CURRENT = Pattern.compile("current");
  private static final Pattern PATTERN_WEEK_OVER_WEEK = Pattern.compile("wo([0-9]+)w");
  private static final Pattern PATTERN_MEAN = Pattern.compile("mean([0-9]+)w");
  private static final Pattern PATTERN_MEDIAN = Pattern.compile("median([0-9]+)w");
  private static final Pattern PATTERN_MIN = Pattern.compile("min([0-9]+)w");
  private static final Pattern PATTERN_MAX = Pattern.compile("max([0-9]+)w");

  private final ExecutorService executor;
  private final AggregationLoader aggregationLoader;
  private final TimeSeriesLoader timeSeriesLoader;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;

  public RootCauseMetricResource(ExecutorService executor, AggregationLoader aggregationLoader,
      TimeSeriesLoader timeSeriesLoader, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) {
    this.executor = executor;
    this.aggregationLoader = aggregationLoader;
    this.timeSeriesLoader = timeSeriesLoader;
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
  }

  /**
   * Returns an aggregate value for the specified metric and time range, and (optionally) offset.
   * Aligns time stamps if necessary and returns NaN if no data is available for the given time range.
   *
   * @param urn metric urn
   * @param start start time (in millis)
   * @param end end time (in millis)
   * @param offset offset identifier (e.g. "current", "wo2w")
   * @param timezone timezone identifier (e.g. "America/Los_Angeles")
   *
   * @see RootCauseMetricResource#parseOffset(MetricSlice, String, String) supported offsets
   *
   * @return aggregate value, or NaN if data not available
   * @throws Exception on catch-all execution failure
   */
  @GET
  @Path("/aggregate")
  public double getAggregate(
      @QueryParam("urn") @NotNull String urn,
      @QueryParam("start") @NotNull long start,
      @QueryParam("end") @NotNull long end,
      @QueryParam("offset") String offset,
      @QueryParam("timezone") String timezone) throws Exception {

    if (StringUtils.isBlank(offset)) {
      offset = OFFSET_DEFAULT;
    }

    if (StringUtils.isBlank(timezone)) {
      timezone = TIMEZONE_DEFAULT;
    }

    MetricSlice baseSlice = alignSlice(makeSlice(urn, start, end));
    Baseline range = parseOffset(baseSlice, offset, timezone);

    List<MetricSlice> slices = range.from(baseSlice);
    Map<MetricSlice, DataFrame> data = fetchAggregates(slices);
    LOG.info("aggregate data:\n{}", data);

    DataFrame result = range.compute(baseSlice, data);

    if (result.isEmpty()) {
      return Double.NaN;
    }

    return result.getDouble(COL_VALUE, 0);
  }

  /**
   * Returns a breakdown (de-aggregation) of the specified metric and time range, and (optionally) offset.
   * Aligns time stamps if necessary and omits null values.
   *
   * @param urn metric urn
   * @param start start time (in millis)
   * @param end end time (in millis)
   * @param offset offset identifier (e.g. "current", "wo2w")
   * @param timezone timezone identifier (e.g. "America/Los_Angeles")
   * @param rollup limit results to the top k elements, plus an 'OTHER' rollup element
   *
   * @see RootCauseMetricResource#parseOffset(MetricSlice, String, String) supported offsets
   *
   * @return aggregate value, or NaN if data not available
   * @throws Exception on catch-all execution failure
   */
  @GET
  @Path("/breakdown")
  public Map<String, Map<String, Double>> getBreakdown(
      @QueryParam("urn") @NotNull String urn,
      @QueryParam("start") @NotNull long start,
      @QueryParam("end") @NotNull long end,
      @QueryParam("offset") String offset,
      @QueryParam("timezone") String timezone,
      @QueryParam("rollup") Long rollup) throws Exception {

    if (StringUtils.isBlank(offset)) {
      offset = OFFSET_DEFAULT;
    }

    if (StringUtils.isBlank(timezone)) {
      timezone = TIMEZONE_DEFAULT;
    }

    if (rollup != null) {
      throw new IllegalArgumentException("rollup not implemented yet");
    }

    MetricSlice baseSlice = alignSlice(makeSlice(urn, start, end));
    Baseline range = parseOffset(baseSlice, offset, timezone);

    List<MetricSlice> slices = range.from(baseSlice);
    Map<MetricSlice, DataFrame> data = fetchBreakdowns(slices);
    LOG.info("breakdown data:\n{}", data);

    DataFrame result = range.compute(baseSlice, data);

    return makeBreakdownMap(result);
  }

  /**
   * Returns a time series for the specified metric and time range, and (optionally) offset at an (optional)
   * time granularity. Aligns time stamps if necessary.
   *
   * @param urn metric urn
   * @param start start time (in millis)
   * @param end end time (in millis)
   * @param offset offset identifier (e.g. "current", "wo2w")
   * @param timezone timezone identifier (e.g. "America/Los_Angeles")
   * @param granularityString time granularity (e.g. "5_MINUTES", "1_HOURS")
   *
   * @see RootCauseMetricResource#parseOffset(MetricSlice, String, String) supported offsets
   *
   * @return aggregate value, or NaN if data not available
   * @throws Exception on catch-all execution failure
   */
  @GET
  @Path("/timeseries")
  public Map<String, List<? extends Number>> getTimeSeries(
      @QueryParam("urn") @NotNull String urn,
      @QueryParam("start") @NotNull long start,
      @QueryParam("end") @NotNull long end,
      @QueryParam("offset") String offset,
      @QueryParam("timezone") String timezone,
      @QueryParam("granularity") String granularityString) throws Exception {

    if (StringUtils.isBlank(offset)) {
      offset = OFFSET_DEFAULT;
    }

    if (StringUtils.isBlank(timezone)) {
      timezone = TIMEZONE_DEFAULT;
    }

    if (StringUtils.isBlank(granularityString)) {
      granularityString = GRANULARITY_DEFAULT;
    }

    TimeGranularity granularity = TimeGranularity.fromString(granularityString);
    MetricSlice baseSlice = alignSlice(makeSlice(urn, start, end, granularity));
    Baseline range = parseOffset(baseSlice, offset, timezone);

    List<MetricSlice> slices = range.from(baseSlice);
    Map<MetricSlice, DataFrame> data = fetchTimeSeries(slices);
    LOG.info("timeseries data:\n{}", data);

    DataFrame result = range.compute(baseSlice, data);

    return makeTimeSeriesMap(result);
  }

  /**
   * Returns a map of time series (keyed by series name) derived from the timeseries results dataframe.
   *
   * @param data (transformed) query results
   * @return map of lists of double or long (keyed by series name)
   */
  private static Map<String, List<? extends Number>> makeTimeSeriesMap(DataFrame data) {
    Map<String, List<? extends Number>> output = new HashMap<>();
    output.put(COL_TIME, data.getLongs(COL_TIME).toList());
    output.put(COL_VALUE, data.getDoubles(COL_VALUE).toList());
    return output;
  }

  /**
   * Returns a map of maps (keyed by dimension name, keyed by dimension value) derived from the
   * breakdown results dataframe.
   *
   * @param data (transformed) query results
   * @return map of maps of value (keyed by dimension name, keyed by dimension value)
   */
  private static Map<String, Map<String, Double>> makeBreakdownMap(DataFrame data) {
    Map<String, Map<String, Double>> output = new HashMap<>();

    data = data.dropNull();

    for (int i = 0; i < data.size(); i++) {
      final String dimName = data.getString(COL_DIMENSION_NAME, i);
      final String dimValue = data.getString(COL_DIMENSION_VALUE, i);
      final double value = data.getDouble(COL_VALUE, i);

      if (!output.containsKey(dimName)) {
        output.put(dimName, new HashMap<String, Double>());
      }
      output.get(dimName).put(dimValue, value);
    }
    return output;
  }

  /**
   * Returns aggregates for the given set of metric slices.
   *
   * @param slices metric slices
   * @return map of dataframes (keyed by metric slice, columns: [COL_TIME(1), COL_VALUE])
   * @throws Exception on catch-all execution failure
   */
  private Map<MetricSlice, DataFrame> fetchAggregates(List<MetricSlice> slices) throws Exception {
    Map<MetricSlice, Future<Double>> futures = new HashMap<>();

    for (final MetricSlice slice : slices) {
      futures.put(slice, this.executor.submit(new Callable<Double>() {
        @Override
        public Double call() throws Exception {
          return RootCauseMetricResource.this.aggregationLoader.loadAggregate(slice);
        }
      }));
    }

    Map<MetricSlice, DataFrame> output = new HashMap<>();
    for (Map.Entry<MetricSlice, Future<Double>> entry : futures.entrySet()) {
      MetricSlice slice = entry.getKey();
      double value = entry.getValue().get(TIMEOUT, TimeUnit.MILLISECONDS);

      DataFrame data = new DataFrame(COL_TIME, LongSeries.buildFrom(slice.getStart()))
          .addSeries(COL_VALUE, value);
      output.put(slice, data);
    }

    return output;
  }

  /**
   * Returns breakdowns (de-aggregations) for a given set of metric slices.
   *
   * @param slices metric slices
   * @return map of dataframes (keyed by metric slice,
   *         columns: [COL_TIME(1), COL_DIMENSION_NAME, COL_DIMENSION_VALUE, COL_VALUE])
   * @throws Exception on catch-all execution failure
   */
  private Map<MetricSlice, DataFrame> fetchBreakdowns(List<MetricSlice> slices) throws Exception {
    Map<MetricSlice, Future<DataFrame>> futures = new HashMap<>();

    for (final MetricSlice slice : slices) {
      futures.put(slice, this.executor.submit(new Callable<DataFrame>() {
        @Override
        public DataFrame call() throws Exception {
          return RootCauseMetricResource.this.aggregationLoader.loadBreakdown(slice);
        }
      }));
    }

    Map<MetricSlice, DataFrame> output = new HashMap<>();
    for (Map.Entry<MetricSlice, Future<DataFrame>> entry : futures.entrySet()) {
      MetricSlice slice = entry.getKey();
      DataFrame value = entry.getValue().get(TIMEOUT, TimeUnit.MILLISECONDS);

      DataFrame data = new DataFrame(value)
          .addSeries(COL_TIME, LongSeries.fillValues(value.size(), slice.getStart()))
          .setIndex(COL_TIME, COL_DIMENSION_NAME, COL_DIMENSION_VALUE);
      output.put(slice, data);
    }

    return output;
  }

  /**
   * Returns timeseries for a given set of metric slices.
   *
   * @param slices metric slices
   * @return map of dataframes (keyed by metric slice, columns: [COL_TIME(N), COL_VALUE])
   * @throws Exception on catch-all execution failure
   */
  private Map<MetricSlice, DataFrame> fetchTimeSeries(List<MetricSlice> slices) throws Exception {
    Map<MetricSlice, Future<DataFrame>> futures = new HashMap<>();

    for (final MetricSlice slice : slices) {
      futures.put(slice, this.executor.submit(new Callable<DataFrame>() {
        @Override
        public DataFrame call() throws Exception {
          return RootCauseMetricResource.this.timeSeriesLoader.load(slice);
        }
      }));
    }

    Map<MetricSlice, DataFrame> output = new HashMap<>();
    for (Map.Entry<MetricSlice, Future<DataFrame>> entry : futures.entrySet()) {
      output.put(entry.getKey(), entry.getValue().get(TIMEOUT, TimeUnit.MILLISECONDS));
    }

    return output;
  }

  /**
   * Returns a configured instance of Baseline for the given, named offset. The method uses slice and
   * timezone information to adjust for daylight savings time.
   *
   * <p>Supported offsets:</p>
   * <pre>
   *   current   the time range as specified by start and end)
   *   woXw      week-over-week data points with a lag of X weeks)
   *   meanXw    average of data points from the the past X weeks, with a lag of 1 week)
   *   medianXw  median of data points from the the past X weeks, with a lag of 1 week)
   *   minXw     minimum of data points from the the past X weeks, with a lag of 1 week)
   *   maxXw     maximum of data points from the the past X weeks, with a lag of 1 week)
   * </pre>
   *
   * @param baseSlice metric slice that acts as base
   * @param offset offset identifier
   * @param timezone timezone identifier (location long format)
   * @return Baseline instance
   * @throws IllegalArgumentException if the offset cannot be parsed
   */
  private static Baseline parseOffset(MetricSlice baseSlice, String offset, String timezone) {
    long timestamp = baseSlice.getStart();

    Matcher mCurrent = PATTERN_CURRENT.matcher(offset);
    if (mCurrent.find()) {
      return BaselineOffset.fromOffset(0);
    }

    Matcher mWeekOverWeek = PATTERN_WEEK_OVER_WEEK.matcher(offset);
    if (mWeekOverWeek.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineType.MEAN, Integer.valueOf(mWeekOverWeek.group(1)), 1, timestamp, timezone);
    }

    Matcher mMean = PATTERN_MEAN.matcher(offset);
    if (mMean.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineType.MEAN, Integer.valueOf(mMean.group(1)), 1, timestamp, timezone);
    }

    Matcher mMedian = PATTERN_MEDIAN.matcher(offset);
    if (mMedian.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineType.MEDIAN, Integer.valueOf(mMedian.group(1)), 1, timestamp, timezone);
    }

    Matcher mMin = PATTERN_MIN.matcher(offset);
    if (mMin.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineType.MIN, Integer.valueOf(mMin.group(1)), 1, timestamp, timezone);
    }

    Matcher mMax = PATTERN_MAX.matcher(offset);
    if (mMax.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineType.MAX, Integer.valueOf(mMax.group(1)), 1, timestamp, timezone);
    }

    throw new IllegalArgumentException(String.format("Unknown offset '%s'", offset));
  }

  /**
   * Aligns a metric slice based on its granularity, or the datset granularity.
   *
   * @param slice metric slice
   * @return aligned metric slice
   * @throws Exception if the referenced metric or dataset cannot be found
   */
  // TODO refactor as util. similar to dataframe utils
  private MetricSlice alignSlice(MetricSlice slice) throws Exception {
    MetricConfigDTO metric = this.metricDAO.findById(slice.getMetricId());
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.getMetricId()));
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id %d", metric.getDataset(), slice.getMetricId()));
    }

    TimeGranularity granularity = dataset.bucketTimeGranularity();
    if (!MetricSlice.NATIVE_GRANULARITY.equals(slice.getGranularity())) {
      granularity = slice.getGranularity();
    }

    long timeGranularity = granularity.toMillis();
    long start = (slice.getStart() / timeGranularity) * timeGranularity;
    long end = ((slice.getEnd() + timeGranularity - 1) / timeGranularity) * timeGranularity;

    return slice.withStart(start).withEnd(end).withGranularity(granularity);
  }

  private MetricSlice makeSlice(String urn, long start, long end) {
    return makeSlice(urn, start, end, MetricSlice.NATIVE_GRANULARITY);
  }

  private MetricSlice makeSlice(String urn, long start, long end, TimeGranularity granularity) {
    MetricEntity metric = MetricEntity.fromURN(urn, 1.0);
    return MetricSlice.from(metric.getId(), start, end, metric.getFilters(), granularity);
  }
}
