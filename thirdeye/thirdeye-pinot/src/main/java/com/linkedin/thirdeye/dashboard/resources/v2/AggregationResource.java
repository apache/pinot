package com.linkedin.thirdeye.dashboard.resources.v2;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.AggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.TimeSeriesLoader;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path(value = "/aggregation")
@Produces(MediaType.APPLICATION_JSON)
public class AggregationResource {
  private static final Logger LOG = LoggerFactory.getLogger(AggregationResource.class);

  private static final long TIMEOUT = 60000;

  private static final String COL_DIMENSION_NAME = AggregationLoader.COL_DIMENSION_NAME;
  private static final String COL_DIMENSION_VALUE = AggregationLoader.COL_DIMENSION_VALUE;
  private static final String COL_VALUE = AggregationLoader.COL_VALUE;

  private static final String COL_DIMENSION_VALUE_OTHER = "OTHER";

  private final ExecutorService executor;
  private final AggregationLoader loader;

  public AggregationResource(ExecutorService executor, AggregationLoader loader) {
    this.executor = executor;
    this.loader = loader;
  }

  /**
   * <p>Returns a dimension breakdown for a given set of metrics and time ranges.
   * Uses the metrics' default aggregation functions. Optionally performs a rollup of smaller
   * values per dimension.</p>
   *
   * <p>The result is structured hierarchically as follows:
   * <pre>
   * [time_range 0]:
   *   [metric_id 0]:
   *     [dimension_name 0]:
   *       [dimension_value 0-0]: value 0
   *       [dimension_value 0-1]: value 1
   *     [dimension_name 1]:
   *       [dimension_value 1-0]: value 0
   *       [dimension_value 1-1]: value 1
   *   [metric_id 1]:
   *     [dimension_name 0]:
   *       [dimension_value 0-0]: value 0
   *       [dimension_value 0-1]: value 1
   * [time_range 1]:
   *   [metric_id 0]:
   *     [dimension_name 0]:
   *       [dimension_value 0-0]: value 0
   *       [dimension_value 0-1]: value 1
   * </pre>
   *
   * <p>Sample request for endpoint:
   * <pre>
   * minimal:    curl -X GET 'localhost:1426/aggregation/query?metricIds=1&ranges=1504076400000:1504162800000'
   * rollup:     curl -X GET 'localhost:1426/aggregation/query?metricIds=1&ranges=1504076400000:1504162800000&rollupCount=5'
   * multiple:   curl -X GET 'localhost:1426/aggregation/query?metricIds=1,2&ranges=1504076400000:1504162800000,1504176400000:1504262800000&rollupCount=5'
   * </pre></p>
   *
   * @param metricIdsString metric ids separated by ","
   * @param rangesString time ranges with end exclusive "[start]:[end]" separated by ","
   * @param filterString (optional) metric filters
   * @param rollupCount (optional) max number of values per dimensions for rollup
   * @return Map (keyed by time range) of maps (keyed by metric id) of maps (keyed by dimension name) of maps (keyed by dimension value) of values
   * @throws Exception if the data cannot be retrieved
   */
  @GET
  @Path("/query")
  public Map<String, Map<String, Map<String, Map<String, Double>>>> getBreakdown(
      @QueryParam("metricIds") String metricIdsString,
      @QueryParam("ranges") String rangesString,
      @QueryParam("filters") String filterString,
      @QueryParam("rollupCount") Integer rollupCount) throws Exception {

    // metric ids
    if (StringUtils.isBlank(metricIdsString)) {
      throw new IllegalArgumentException("Must provide metricId");
    }
    List<String> metricIds = Arrays.asList(metricIdsString.split(","));

    // time ranges
    if (StringUtils.isBlank(rangesString)) {
      throw new IllegalArgumentException("Must provide at least one range");
    }
    List<Interval> ranges = extractRanges(rangesString);

    // filters
    Multimap<String, String> filters = ArrayListMultimap.create();
    if (!StringUtils.isBlank(filterString)) {
      filters = ThirdEyeUtils.convertToMultiMap(filterString);
    }

    LOG.info("Requesting {} metrics with {} filters from {} time ranges",
        metricIds.size(), filters.size(), ranges.size());

    // request data
    Map<MetricSlice, Future<DataFrame>> requests = new HashMap<>();
    for (String id : metricIds) {
      for (Interval range : ranges) {
        long metricId = Long.valueOf(id);
        long start = range.getStartMillis();
        long end = range.getEndMillis();
        MetricSlice slice = MetricSlice.from(metricId, start, end, filters);

        requests.put(slice, loadBreakdownAsync(slice));
      }
    }

    // collect results
    Map<String, Map<String, Map<String, Map<String, Double>>>> results = new HashMap<>();
    for (Interval range : ranges) {
      Map<String, Map<String, Map<String, Double>>> rangeResults = new HashMap<>();

      for (MetricSlice slice : filterSlices(requests.keySet(), range)) {
        String mid = String.valueOf(slice.getMetricId());

        try {
          DataFrame df = requests.get(slice).get(TIMEOUT, TimeUnit.MILLISECONDS);

          if (rollupCount != null) {
            df = rollupTail(df, rollupCount);
          }

          rangeResults.put(mid, dataframeToMap(df));

        } catch (Exception e) {
          LOG.warn("Could not retrieve data for {}. Skipping.", slice.getMetricId());
        }
      }

      String rid = String.format("%s:%s", range.getStartMillis(), range.getEndMillis());
      results.put(rid, rangeResults);
    }

    return results;
  }

  /**
   * <p>Returns summary aggregates for a given set of metrics and time ranges.
   * Uses the metrics' default aggregation functions.</p>
   *
   * <p>The result is structured hierarchically as follows:
   * <pre>
   * [time_range 0]:
   *   [metric_id 0]: value 0
   *   [metric_id 1]: value 1
   * [time_range 1]:
   *   [metric_id 0]: value 0
   * </pre>
   *
   * <p>Sample request for endpoint:
   * <pre>
   * minimal:    curl -X GET 'localhost:1426/aggregation/summary?metricIds=1&ranges=1504076400000:1504162800000'
   * multiple:   curl -X GET 'localhost:1426/aggregation/summary?metricIds=1,2&ranges=1504076400000:1504162800000,1504176400000:1504262800000'
   * </pre></p>
   *
   * @param metricIdsString metric ids separated by ","
   * @param rangesString time ranges with end exclusive "[start]:[end]" separated by ","
   * @param filterString (optional) metric filters
   * @return Map (keyed by time range) of maps (keyed by metric id) of maps (keyed by dimension name) of maps (keyed by dimension value) of values
   * @throws Exception if the data cannot be retrieved
   */
  @GET
  @Path("/aggregate")
  public Map<String, Map<String, Double>> getAggregate(
      @QueryParam("metricIds") String metricIdsString,
      @QueryParam("ranges") String rangesString,
      @QueryParam("filters") String filterString) throws Exception {

    // metric ids
    if (StringUtils.isBlank(metricIdsString)) {
      throw new IllegalArgumentException("Must provide metricId");
    }
    List<String> metricIds = Arrays.asList(metricIdsString.split(","));

    // time ranges
    if (StringUtils.isBlank(rangesString)) {
      throw new IllegalArgumentException("Must provide at least one range");
    }
    List<Interval> ranges = extractRanges(rangesString);

    // filters
    Multimap<String, String> filters = ArrayListMultimap.create();
    if (!StringUtils.isBlank(filterString)) {
      filters = ThirdEyeUtils.convertToMultiMap(filterString);
    }

    LOG.info("Requesting {} metrics with {} filters from {} time ranges",
        metricIds.size(), filters.size(), ranges.size());

    // request data
    Map<MetricSlice, Future<Double>> requests = new HashMap<>();
    for (String id : metricIds) {
      for (Interval range : ranges) {
        long metricId = Long.valueOf(id);
        long start = range.getStartMillis();
        long end = range.getEndMillis();
        MetricSlice slice = MetricSlice.from(metricId, start, end, filters);

        requests.put(slice, loadAggregateAsync(slice));
      }
    }

    // collect results
    Map<String, Map<String, Double>> results = new HashMap<>();
    for (Interval range : ranges) {
      Map<String, Double> rangeResults = new HashMap<>();

      for (MetricSlice slice : filterSlices(requests.keySet(), range)) {
        String mid = String.valueOf(slice.getMetricId());

        try {
          rangeResults.put(mid, requests.get(slice).get(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
          LOG.warn("Could not retrieve data for metric id {}. Skipping.", slice.getMetricId());
        }
      }

      String rid = String.format("%s:%s", range.getStartMillis(), range.getEndMillis());
      results.put(rid, rangeResults);
    }

    return results;
  }

  /**
   * Helper to extract intervals from rangesString
   *
   * @param rangesString endpoint ranges string
   * @return list of intervals
   */
  private static List<Interval> extractRanges(String rangesString) {
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
    return ranges;
  }

  /**
   * Helper to roll up dimension values to a fixed count per dimension name. The rolled up column
   * carries the dimension value {@code COL_DIMENSION_VALUE_OTHER}.
   *
   * @param df dimension breakdown dataframe
   * @param numValues max number of dimension values
   * @return dataframe with max {@code numValues} dimension values
   */
  private static DataFrame rollupTail(DataFrame df, int numValues) {
    if (df.isEmpty()) {
      return df;
    }

    List<DataFrame> results = new ArrayList<>();
    for (String dimName : df.getStrings(COL_DIMENSION_NAME).unique().values()) {
      DataFrame dfValues = df.filterEquals(COL_DIMENSION_NAME, dimName).dropNull().sortedBy(COL_VALUE).reverse();

      DataFrame dfPassthru = dfValues.sliceTo(numValues - 1);
      DataFrame dfRollup = dfValues.sliceFrom(numValues - 1);

      // TODO account for non-sum aggregation functions
      if (!dfRollup.isEmpty()) {
        double sum = dfRollup.getDoubles(COL_VALUE).sum().fillNull().doubleValue();
        DataFrame dfAppend = DataFrame
            .builder(COL_DIMENSION_NAME, COL_DIMENSION_VALUE, COL_VALUE)
            .append(dimName, COL_DIMENSION_VALUE_OTHER, sum).build();
        dfPassthru = dfPassthru.append(dfAppend);
      }

      results.add(dfPassthru);
    }

    DataFrame first = results.get(0);
    results.remove(0);

    return first.append(results);
  }

  /**
   * Helper to convert dimension aggregation dataframe to a map of values
   *
   * @param df dimension breakdown dataframe
   * @return Map (keyed by dimension name) of maps (keyed by dimension value) of values
   */
  private static Map<String, Map<String, Double>> dataframeToMap(DataFrame df) {
    Map<String, Map<String, Double>> output = new HashMap<>();
    for (String dimName : df.getStrings(COL_DIMENSION_NAME).unique().values()) {
      Map<String, Double> values = new HashMap<>();
      DataFrame dfValues = df.filterEquals(COL_DIMENSION_NAME, dimName).dropNull();
      for (int i = 0; i < dfValues.size(); i++) {
        String dimValue = dfValues.getString(COL_DIMENSION_VALUE, i);
        double value = dfValues.getDouble(COL_VALUE, i);
        values.put(dimValue, value);
      }
      output.put(dimName, values);
    }
    return output;
  }

  /**
   * Helper to extract slices for a given jodatime utc interval
   *
   * @param slices all metric slices
   * @param range time range to extract
   * @return List of metric slices
   */
  private static List<MetricSlice> filterSlices(Iterable<MetricSlice> slices, Interval range) {
    List<MetricSlice> output = new ArrayList<>();
    for (MetricSlice slice : slices) {
      if (slice.getStart() == range.getStartMillis() && slice.getEnd() == range.getEndMillis()) {
        output.add(slice);
      }
    }
    return output;
  }

  /**
   * Asynchronous call to {@code loadBreakdown}
   * @see TimeSeriesLoader#load
   */
  private Future<DataFrame> loadBreakdownAsync(final MetricSlice slice) throws Exception {
    return this.executor.submit(new Callable<DataFrame>() {
      @Override
      public DataFrame call() throws Exception {
        return AggregationResource.this.loader.loadBreakdown(slice);
      }
    });
  }

  /**
   * Asynchronous call to {@code loadBreakdown}
   * @see TimeSeriesLoader#load
   */
  private Future<Double> loadAggregateAsync(final MetricSlice slice) throws Exception {
    return this.executor.submit(new Callable<Double>() {
      @Override
      public Double call() throws Exception {
        return AggregationResource.this.loader.loadAggregate(slice);
      }
    });
  }
}