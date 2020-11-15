/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dashboard.resources.v2;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.lang.reflect.Constructor;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.detection.spec.AbstractSpec;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.dashboard.resources.v2.BaselineParsingUtils.*;
import static org.apache.pinot.thirdeye.detection.DetectionUtils.*;


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

  private static final long TIMEOUT = 600000;

  private static final String OFFSET_DEFAULT = "current";
  private static final String TIMEZONE_DEFAULT = "UTC";
  private static final String OFFSET_FORECAST = "forecast";
  private static final String GRANULARITY_DEFAULT = MetricSlice.NATIVE_GRANULARITY.toAggregationGranularityString();
  private static final int LIMIT_DEFAULT = 100;

  private final ExecutorService executor;
  private final AggregationLoader aggregationLoader;
  private final TimeSeriesLoader timeSeriesLoader;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final DataProvider dataProvider;

  public RootCauseMetricResource(ExecutorService executor, AggregationLoader aggregationLoader,
      TimeSeriesLoader timeSeriesLoader, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO,
      EventManager eventDAO, MergedAnomalyResultManager anomalyDAO, EvaluationManager evaluationDAO) {
    this.executor = executor;
    this.aggregationLoader = aggregationLoader;
    this.timeSeriesLoader = timeSeriesLoader;
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(), ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());
    this.dataProvider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, evaluationDAO,
        timeseriesLoader, aggregationLoader, new DetectionPipelineLoader(), TimeSeriesCacheBuilder.getInstance(),
        AnomaliesCacheBuilder.getInstance());
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
   * Returns a list of aggregate value for the specified metric and time range, and a list of offset.
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

    if (StringUtils.isBlank(timezone)) {
      timezone = TIMEZONE_DEFAULT;
    }

    offsets = ResourceUtils.parseListParams(offsets);
    List<MetricSlice> slices = new ArrayList<>();

    Map<String, MetricSlice> offsetToBaseSlice = new HashMap<>();
    Map<String, Baseline> offsetToRange = new HashMap<>();
    for (String offset : offsets) {
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
   * Returns a map of lists of aggregate values for the specified metrics and time range, and a list of offsets.
   * Aligns time stamps if necessary and returns NaN if no data is available for the given time range.
   *
   * @param urns metric urns
   * @param start start time (in millis)
   * @param end end time (in millis)
   * @param offsets A list of offset identifiers (e.g. "current", "wo2w")
   * @param timezone timezone identifier (e.g. "America/Los_Angeles")
   *
   * @see BaselineParsingUtils#parseOffset(String, String) supported offsets
   *
   * @return map of lists (keyed by urn) of aggregate values, or NaN if data not available
   * @throws Exception on catch-all execution failure
   */
  @GET
  @Path("/aggregate/chunk")
  @ApiOperation(value = "Returns a map of lists (keyed by urn) of aggregate value for the specified metrics and time range, and offsets.")
  public Map<String, Collection<Double>> getAggregateChunk(
      @ApiParam(value = "metric urns", required = true) @QueryParam("urns") @NotNull List<String> urns,
      @ApiParam(value = "start time (in millis)", required = true) @QueryParam("start") @NotNull long start,
      @ApiParam(value = "end time (in millis)", required = true) @QueryParam("end") @NotNull long end,
      @ApiParam(value = "A list of offset identifier separated by comma (e.g. \"current\", \"wo2w\")") @QueryParam("offsets") List<String> offsets,
      @ApiParam(value = "timezone identifier (e.g. \"America/Los_Angeles\")") @QueryParam("timezone") String timezone)
      throws Exception {
    ListMultimap<String, Double> aggregateValues = ArrayListMultimap.create();

    if (StringUtils.isBlank(timezone)) {
      timezone = TIMEZONE_DEFAULT;
    }

    urns = ResourceUtils.parseListParams(urns);
    offsets = ResourceUtils.parseListParams(offsets);
    List<MetricSlice> slices = new ArrayList<>();

    Map<Pair<String, String>, MetricSlice> offsetToBaseSlice = new HashMap<>();
    Map<Pair<String, String>, Baseline> tupleToRange = new HashMap<>();
    for (String urn : urns) {
      for (String offset : offsets) {
        Pair<String, String> key = Pair.of(urn, offset);

        MetricSlice baseSlice = alignSlice(makeSlice(urn, start, end), timezone);
        offsetToBaseSlice.put(key, baseSlice);

        Baseline range = parseOffset(offset, timezone);
        tupleToRange.put(key, range);

        List<MetricSlice> currentSlices = range.scatter(baseSlice);

        slices.addAll(currentSlices);
        logSlices(baseSlice, currentSlices);
      }
    }

    // Fetch all aggregates
    Map<MetricSlice, DataFrame> data = fetchAggregates(slices);

    // Pick the results
    for (String urn : urns) {
      for (String offset : offsets) {
        Pair<String, String> key = Pair.of(urn, offset);
        DataFrame result = tupleToRange.get(key).gather(offsetToBaseSlice.get(key), data);
        if (result.isEmpty()) {
          aggregateValues.put(urn, Double.NaN);
        } else {
          aggregateValues.put(urn, result.getDouble(COL_VALUE, 0));
        }
      }
    }

    return aggregateValues.asMap();
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

    if(OFFSET_FORECAST.equals(offset)) {
      String forecastClassName = DetectionRegistry.getInstance().lookup("FORECAST");
      try {
        InputDataFetcher dataFetcher = new DefaultInputDataFetcher(dataProvider, -1);
        Map<String, Object> componentSpec = new HashMap<>();

        Constructor<?> constructor = Class.forName(forecastClassName).getConstructor();
        BaselineProvider forecastProvider = (BaselineProvider) constructor.newInstance();

        Class clazz = Class.forName(forecastClassName);
        Class<AbstractSpec> specClazz = (Class<AbstractSpec>) Class.forName(getSpecClassName(clazz));
        AbstractSpec forecastSpec = AbstractSpec.fromProperties(componentSpec, specClazz);

        forecastProvider.init(forecastSpec, dataFetcher);
        TimeSeries forecast = forecastProvider.computePredictedTimeSeries(baseSlice);
        return makeTimeSeriesMap(forecast.getDataFrame());
      }
      catch (Exception e) {
        LOG.info("Failed to get Forecast baseline");
        offset = OFFSET_DEFAULT;
      }
    }

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
    long start = ((slice.getStart() + offset + timeGranularity - 1) / timeGranularity) * timeGranularity - offset; // round up the start time to time granularity boundary of the requested time zone
    long end = start + (slice.getEnd() - slice.getStart());

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
