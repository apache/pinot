/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.api.Constants;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.loader.AggregationLoader;
import com.linkedin.thirdeye.datasource.loader.TimeSeriesLoader;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.dashboard.resources.v2.BaselineParsingUtils.*;


/**
 * <p>RootCauseMetricResource is a central endpoint for querying different views on metrics as used by the
 * RCA frontend. It delivers metric timeseries, aggregates, and breakdowns (de-aggregations).
 * The endpoint parses metric urns and a unified set of "offsets", i.e. time-warped baseline of the
 * specified metric. It further aligns queried time stamps to sensibly match the raw dataset.</p>
 *
 * @see BaselineParsingUtils#parseOffset(String, String) supported offsets
 */
@Path(value = "/rootcause/metric")
@Produces(MediaType.APPLICATION_JSON)
@Api(tags = {Constants.RCA_TAG})
public class RootCauseMetricResource {
  private static final Logger LOG = LoggerFactory.getLogger(RootCauseMetricResource.class);

  private static final String COL_TIME = TimeSeriesLoader.COL_TIME;
  private static final String COL_VALUE = TimeSeriesLoader.COL_VALUE;
  private static final String COL_DIMENSION_NAME = AggregationLoader.COL_DIMENSION_NAME;
  private static final String COL_DIMENSION_VALUE = AggregationLoader.COL_DIMENSION_VALUE;

  private static final String ROLLUP_NAME = "OTHER";

  private static final long TIMEOUT = 60000;

  private static final String OFFSET_DEFAULT = "current";
  private static final String TIMEZONE_DEFAULT = "UTC";
  private static final String GRANULARITY_DEFAULT = MetricSlice.NATIVE_GRANULARITY.toAggregationGranularityString();
  private static final int LIMIT_DEFAULT = 100;

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
   * @see BaselineParsingUtils#parseOffset(String, String) supported offsets
   *
   * @return aggregate value, or NaN if data not available
   * @throws Exception on catch-all execution failure
   */
  @GET
  @Path("/aggregate")
  @ApiOperation(value = "Returns an aggregate value for the specified metric and time range, and (optionally) offset.")
  public double getAggregate(@ApiParam(value = "metric urn", required = true) @QueryParam("urn") @NotNull String urn,
      @ApiParam(value = "start time (in millis)", required = true) @QueryParam("start") @NotNull long start,
      @ApiParam(value = "end time (in millis)", required = true) @QueryParam("end") @NotNull long end,
      @ApiParam(value = "offset identifier (e.g. \"current\", \"wo2w\")") @QueryParam("offset") String offset,
      @ApiParam(value = "timezone identifier (e.g. \"America/Los_Angeles\")") @QueryParam("timezone") String timezone)
      throws Exception {
    if (StringUtils.isBlank(offset)) {
      offset = OFFSET_DEFAULT;
    }

    if (StringUtils.isBlank(timezone)) {
      timezone = TIMEZONE_DEFAULT;
    }

    MetricSlice baseSlice = alignSlice(makeSlice(urn, start, end), timezone);
    Baseline range = parseOffset(offset, timezone);

    List<MetricSlice> slices = range.scatter(baseSlice);
    logSlices(baseSlice, slices);

    Map<MetricSlice, DataFrame> data = fetchAggregates(slices);

    DataFrame result = range.gather(baseSlice, data);

    if (result.isEmpty()) {
      return Double.NaN;
    }
    return result.getDouble(COL_VALUE, 0);
  }

  /**
   * Returns a list of aggregate value for the specified metric and time range, and (optionally) a list of offset.
   * Aligns time stamps if necessary and returns NaN if no data is available for the given time range.
   *
   * @param urn metric urn
   * @param start start time (in millis)
   * @param end end time (in millis)
   * @param offsets A list of offset identifier (e.g. "current", "wo2w")
   * @param timezone timezone identifier (e.g. "America/Los_Angeles")
   *
   * @see BaselineParsingUtils#parseOffset(String, String) supported offsets
   *
   * @return aggregate value, or NaN if data not available
   * @throws Exception on catch-all execution failure
   */
  @GET
  @Path("/aggregate/batch")
  @ApiOperation(value = "Returns a list of aggregate value for the specified metric and time range, and (optionally) offset.")
  public List<Double> getAggregateBatch(
      @ApiParam(value = "metric urn", required = true) @QueryParam("urn") @NotNull String urn,
      @ApiParam(value = "start time (in millis)", required = true) @QueryParam("start") @NotNull long start,
      @ApiParam(value = "end time (in millis)", required = true) @QueryParam("end") @NotNull long end,
      @ApiParam(value = "A list of offset identifier separated by comma (e.g. \"current\", \"wo2w\")") @QueryParam("offsets") List<String> offsets,
      @ApiParam(value = "timezone identifier (e.g. \"America/Los_Angeles\")") @QueryParam("timezone") String timezone)
      throws Exception {
    List<Double> aggregateValues = new ArrayList<>();

    offsets = ResourceUtils.parseListParams(offsets);
    List<MetricSlice> slices = new ArrayList<>();

    Map<String, MetricSlice> offsetToBaseSlice = new HashMap<>();
    Map<String, Baseline> offsetToRange = new HashMap<>();
    for (String offset : offsets) {
      // Put all metric slices together
      if (StringUtils.isBlank(offset)) {
        offset = OFFSET_DEFAULT;
      }

      if (StringUtils.isBlank(timezone)) {
        timezone = TIMEZONE_DEFAULT;
      }

      MetricSlice baseSlice = alignSlice(makeSlice(urn, start, end), timezone);
      offsetToBaseSlice.put(offset, baseSlice);

      Baseline range = parseOffset(offset, timezone);
      offsetToRange.put(offset, range);

      List<MetricSlice> currentSlices = range.scatter(baseSlice);

      slices.addAll(currentSlices);
      logSlices(baseSlice, currentSlices);
    }

    // Fetch all aggregates
    Map<MetricSlice, DataFrame> data = fetchAggregates(slices);

    // Pick the results
    for (String offset : offsets) {
      DataFrame result = offsetToRange.get(offset).gather(offsetToBaseSlice.get(offset), data);
      if (result.isEmpty()) {
        aggregateValues.add(Double.NaN);
      } else {
        aggregateValues.add(result.getDouble(COL_VALUE, 0));
      }
    }
    return aggregateValues;
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
   * @param limit limit results to the top k elements, plus a rollup element
   *
   * @see BaselineParsingUtils#parseOffset(String, String) supported offsets
   *
   * @return aggregate value, or NaN if data not available
   * @throws Exception on catch-all execution failure
   */
  @GET
  @Path("/breakdown")
  @ApiOperation(value = "Returns a breakdown (de-aggregation) of the specified metric and time range, and (optionally) offset.\n"
      + "Aligns time stamps if necessary and omits null values.")
  public Map<String, Map<String, Double>> getBreakdown(
      @ApiParam(value = "metric urn", required = true)
      @QueryParam("urn") @NotNull String urn,
      @ApiParam(value = "start time (in millis)", required = true)
      @QueryParam("start") @NotNull long start,
      @ApiParam(value = "end time (in millis)", required = true)
      @QueryParam("end") @NotNull long end,
      @ApiParam(value = "offset identifier (e.g. \"current\", \"wo2w\")")
      @QueryParam("offset") String offset,
      @ApiParam(value = "timezone identifier (e.g. \"America/Los_Angeles\")")
      @QueryParam("timezone") String timezone,
      @ApiParam(value = "limit results to the top k elements, plus 'OTHER' rollup element")
      @QueryParam("limit") Integer limit) throws Exception {

    if (StringUtils.isBlank(offset)) {
      offset = OFFSET_DEFAULT;
    }

    if (StringUtils.isBlank(timezone)) {
      timezone = TIMEZONE_DEFAULT;
    }

    if (limit == null) {
      limit = LIMIT_DEFAULT;
    }

    MetricSlice baseSlice = alignSlice(makeSlice(urn, start, end), timezone);
    Baseline range = parseOffset(offset, timezone);

    List<MetricSlice> slices = range.scatter(baseSlice);
    logSlices(baseSlice, slices);

    Map<MetricSlice, DataFrame> dataBreakdown = fetchBreakdowns(slices, limit);
    Map<MetricSlice, DataFrame> dataAggregate = fetchAggregates(slices);

    DataFrame resultBreakdown = range.gather(baseSlice, dataBreakdown);
    DataFrame resultAggregate = range.gather(baseSlice, dataAggregate);

    return makeBreakdownMap(resultBreakdown, resultAggregate);
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
   * @see BaselineParsingUtils#parseOffset(String, String) supported offsets
   *
   * @return aggregate value, or NaN if data not available
   * @throws Exception on catch-all execution failure
   */
  @GET
  @Path("/timeseries")
  @ApiOperation(value = "Returns a time series for the specified metric and time range, and (optionally) offset at an (optional)\n"
      + "time granularity. Aligns time stamps if necessary.")
  public Map<String, List<? extends Number>> getTimeSeries(
      @ApiParam(value = "metric urn", required = true)
      @QueryParam("urn") @NotNull String urn,
      @ApiParam(value = "start time (in millis)", required = true)
      @QueryParam("start") @NotNull long start,
      @ApiParam(value = "end time (in millis)", required = true)
      @QueryParam("end") @NotNull long end,
      @ApiParam(value = "offset identifier (e.g. \"current\", \"wo2w\")")
      @QueryParam("offset") String offset,
      @ApiParam(value = "timezone identifier (e.g. \"America/Los_Angeles\")")
      @QueryParam("timezone") String timezone,
      @ApiParam(value = "limit results to the top k elements, plus an 'OTHER' rollup element")
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
    MetricSlice baseSlice = alignSlice(makeSlice(urn, start, end, granularity), timezone);
    Baseline range = parseOffset(offset, timezone);

    List<MetricSlice> slices = new ArrayList<>(range.scatter(baseSlice));
    logSlices(baseSlice, slices);

    Map<MetricSlice, DataFrame> data = fetchTimeSeries(slices);
    DataFrame rawResult = range.gather(baseSlice, data);

    DataFrame imputedResult = this.imputeExpectedTimestamps(rawResult, baseSlice, timezone);

    return makeTimeSeriesMap(imputedResult);
  }

  /**
   * Generates expected timestamps for the underlying time series and merges them with the
   * actual time series. This allows the front end to distinguish between un-expected and
   * missing data.
   *
   * @param data time series dataframe
   * @param slice metric slice
   * @return time series dataframe with nulls for expected but missing data
   */
  private DataFrame imputeExpectedTimestamps(DataFrame data, MetricSlice slice, String timezone) {
    if (data.size() <= 1) {
      return data;
    }

    MetricConfigDTO metric = this.metricDAO.findById(slice.getMetricId());
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.getMetricId()));
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id %d", metric.getDataset(), slice.getMetricId()));
    }

    TimeGranularity granularity = dataset.bucketTimeGranularity();
    if (!MetricSlice.NATIVE_GRANULARITY.equals(slice.getGranularity())
        && slice.getGranularity().toMillis() >= granularity.toMillis()) {
      granularity = slice.getGranularity();
    }

    DateTimeZone tz = DateTimeZone.forID(timezone);
    long start = data.getLongs(COL_TIME).min().longValue();
    long end = slice.getEnd();
    Period stepSize = granularity.toPeriod();

    DateTime current = new DateTime(start, tz);
    List<Long> timestamps = new ArrayList<>();
    while (current.getMillis() < end) {
      timestamps.add(current.getMillis());
      current = current.plus(stepSize);
    }

    LongSeries sExpected = LongSeries.buildFrom(ArrayUtils.toPrimitive(timestamps.toArray(new Long[timestamps.size()])));
    DataFrame dfExpected = new DataFrame(COL_TIME, sExpected);

    return data.joinOuter(dfExpected).sortedBy(COL_TIME);
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
   * @param dataBreakdown (transformed) breakdown query results
   * @param dataAggregate (transformed) aggregate query results
   * @return map of maps of value (keyed by dimension name, keyed by dimension value)
   */
  private static Map<String, Map<String, Double>> makeBreakdownMap(DataFrame dataBreakdown, DataFrame dataAggregate) {
    Map<String, Map<String, Double>> output = new TreeMap<>();

    dataBreakdown = dataBreakdown.dropNull();
    dataAggregate = dataAggregate.dropNull();

    Map<String, Double> dimensionTotals = new HashMap<>();

    for (int i = 0; i < dataBreakdown.size(); i++) {
      final String dimName = dataBreakdown.getString(COL_DIMENSION_NAME, i);
      final String dimValue = dataBreakdown.getString(COL_DIMENSION_VALUE, i);
      final double value = dataBreakdown.getDouble(COL_VALUE, i);

      // cell
      if (!output.containsKey(dimName)) {
        output.put(dimName, new HashMap<String, Double>());
      }
      output.get(dimName).put(dimValue, value);

      // total
      dimensionTotals.put(dimName, MapUtils.getDoubleValue(dimensionTotals, dimName, 0) + value);
    }

    // add rollup column
    if (!dataAggregate.isEmpty()) {
      double total = dataAggregate.getDouble(COL_VALUE, 0);
      for (Map.Entry<String, Double> entry : dimensionTotals.entrySet()) {
        if (entry.getValue() < total) {
          output.get(entry.getKey()).put(ROLLUP_NAME, total - entry.getValue());
        }
      }
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
          DataFrame df = RootCauseMetricResource.this.aggregationLoader.loadAggregate(slice, Collections.<String>emptyList(), -1);
          if (df.isEmpty()) {
            return Double.NaN;
          }
          return df.getDouble(COL_VALUE, 0);
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
   * @param limit top k elements limit
   * @return map of dataframes (keyed by metric slice,
   *         columns: [COL_TIME(1), COL_DIMENSION_NAME, COL_DIMENSION_VALUE, COL_VALUE])
   * @throws Exception on catch-all execution failure
   */
  private Map<MetricSlice, DataFrame> fetchBreakdowns(List<MetricSlice> slices, final int limit) throws Exception {
    Map<MetricSlice, Future<DataFrame>> futures = new HashMap<>();

    for (final MetricSlice slice : slices) {
      futures.put(slice, this.executor.submit(new Callable<DataFrame>() {
        @Override
        public DataFrame call() throws Exception {
          return RootCauseMetricResource.this.aggregationLoader.loadBreakdown(slice, limit);
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
   * Aligns a metric slice based on its granularity, or the dataset granularity.
   *
   * @param slice metric slice
   * @return aligned metric slice
   */
  // TODO refactor as util. similar to dataframe utils
  private MetricSlice alignSlice(MetricSlice slice, String timezone) {
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

    // align to time buckets and request time zone
    long offset = DateTimeZone.forID(timezone).getOffset(slice.getStart());
    long timeGranularity = granularity.toMillis();
    long start = ((slice.getStart() + offset) / timeGranularity) * timeGranularity - offset;
    long end = ((slice.getEnd() + offset + timeGranularity - 1) / timeGranularity) * timeGranularity - offset;

    return slice.withStart(start).withEnd(end).withGranularity(granularity);
  }

  private MetricSlice makeSlice(String urn, long start, long end) {
    return makeSlice(urn, start, end, MetricSlice.NATIVE_GRANULARITY);
  }

  private MetricSlice makeSlice(String urn, long start, long end, TimeGranularity granularity) {
    MetricEntity metric = MetricEntity.fromURN(urn);
    return MetricSlice.from(metric.getId(), start, end, metric.getFilters(), granularity);
  }

  private static void logSlices(MetricSlice baseSlice, List<MetricSlice> slices) {
    final DateTimeFormatter formatter = DateTimeFormat.forStyle("LL");
    LOG.info("{} - {} (base)", formatter.print(baseSlice.getStart()), formatter.print(baseSlice.getEnd()));
    for (MetricSlice slice : slices) {
      LOG.info("{} - {}", formatter.print(slice.getStart()), formatter.print(slice.getEnd()));
    }
  }
}
