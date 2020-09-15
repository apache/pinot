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

package org.apache.pinot.thirdeye.dataframe.util;

import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.StringSeries;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponseRow;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.Partial;
import org.joda.time.Period;
import org.joda.time.PeriodType;


/**
 * Utility class for ThirdEye-specific parsers and transformers of data related to DataFrame.
 *
 */
public class DataFrameUtils {

  /**
   * Returns a Thirdeye response parsed as a DataFrame. The method stores the time values in
   * {@code COL_TIME} by default, and creates columns for each groupBy attribute and for each
   * MetricFunction specified in the request.
   *
   * @param response thirdeye client response
   * @return response as dataframe
   */
  public static DataFrame parseResponse(ThirdEyeResponse response) {
    // builders
    LongSeries.Builder timeBuilder = LongSeries.builder();
    List<StringSeries.Builder> dimensionBuilders = new ArrayList<>();
    List<DoubleSeries.Builder> functionBuilders = new ArrayList<>();

    for(int i=0; i<response.getGroupKeyColumns().size(); i++) {
      dimensionBuilders.add(StringSeries.builder());
    }

    for(int i=0; i<response.getMetricFunctions().size(); i++) {
      functionBuilders.add(DoubleSeries.builder());
    }

    // values
    for(int i=0; i<response.getNumRows(); i++) {
      ThirdEyeResponseRow r = response.getRow(i);
      timeBuilder.addValues(r.getTimeBucketId());

      for(int j=0; j<r.getDimensions().size(); j++) {
        dimensionBuilders.get(j).addValues(r.getDimensions().get(j));
      }

      for(int j=0; j<r.getMetrics().size(); j++) {
        functionBuilders.get(j).addValues(r.getMetrics().get(j));
      }
    }

    // dataframe
    String timeColumn = response.getDataTimeSpec().getColumnName();

    DataFrame df = new DataFrame();
    df.addSeries(DataFrame.COL_TIME, timeBuilder.build());
    df.setIndex(DataFrame.COL_TIME);

    int i = 0;
    for(String n : response.getGroupKeyColumns()) {
      if(!timeColumn.equals(n)) {
        df.addSeries(n, dimensionBuilders.get(i++).build());
      }
    }

    int j = 0;
    for(MetricFunction mf : response.getMetricFunctions()) {
      df.addSeries(mf.toString(), functionBuilders.get(j++).build());
    }

    // compression
    for (String name : df.getSeriesNames()) {
      if (Series.SeriesType.STRING.equals(df.get(name).type())) {
        df.addSeries(name, df.getStrings(name).compress());
      }
    }

    return df.sortedBy(DataFrame.COL_TIME);
  }

  /**
   * Returns the DataFrame augmented with a {@code COL_VALUE} column that contains the
   * evaluation results from computing derived metric expressions. The method performs the
   * augmentation in-place.
   *
   * <br/><b>NOTE:</b> only supports computation of a single MetricExpression.
   *
   * @param df thirdeye response dataframe
   * @param expressions collection of metric expressions
   * @return augmented dataframe
   * @throws Exception if the metric expression cannot be computed
   */
  public static DataFrame evaluateExpressions(DataFrame df, Collection<MetricExpression> expressions) throws Exception {
    if(expressions.size() != 1)
      throw new IllegalArgumentException("Requires exactly one expression");

    MetricExpression me = expressions.iterator().next();
    Collection<MetricFunction> functions = me.computeMetricFunctions();

    Map<String, Double> context = new HashMap<>();
    double[] values = new double[df.size()];

    for(int i=0; i<df.size(); i++) {
      for(MetricFunction f : functions) {
        // TODO check inconsistency between getMetricName() and toString()
        context.put(f.getMetricName(), df.getDouble(f.toString(), i));
      }
      values[i] = MetricExpression.evaluateExpression(me, context);
    }

    // drop intermediate columns
    for(MetricFunction f : functions) {
      df.dropSeries(f.toString());
    }

    return df.addSeries(DataFrame.COL_VALUE, values);
  }

  /**
   * Returns the DataFrame with timestamps aligned to a start offset and an interval.
   *
   * @param df thirdeye response dataframe
   * @param origin start offset
   * @param interval timestep multiple
   * @return dataframe with modified timestamps
   */
  public static DataFrame makeTimestamps(DataFrame df, final DateTime origin, final Period interval) {
    return new DataFrame(df).mapInPlace(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return origin.plus(interval.multipliedBy((int) values[0])).getMillis();
      }
    }, DataFrame.COL_TIME);
  }

  /**
   * Returns a Thirdeye response parsed as a DataFrame. The method stores the time values in
   * {@code COL_TIME} by default, and creates columns for each groupBy attribute and for each
   * MetricFunction specified in the request. It further evaluates expressions for derived
   * metrics.
   * @see DataFrameUtils#makeAggregateRequest(MetricSlice, List, int, String, MetricConfigManager, DatasetConfigManager)
   * @see DataFrameUtils#makeTimeSeriesRequest(MetricSlice, String, MetricConfigManager, DatasetConfigManager)
   *
   * @param response thirdeye client response
   * @param rc RequestContainer
   * @return response as dataframe
   */
  public static DataFrame evaluateResponse(ThirdEyeResponse response, RequestContainer rc) throws Exception {
    return evaluateExpressions(parseResponse(response), rc.getExpressions());
  }

  /**
   * Returns a Thirdeye response parsed as a DataFrame. The method stores the time values in
   * {@code COL_TIME} by default, and creates columns for each groupBy attribute and for each
   * MetricFunction specified in the request. It evaluates expressions for derived
   * metrics and offsets timestamp based on the original timeseries request.
   * @see DataFrameUtils#makeTimeSeriesRequest(MetricSlice, String, MetricConfigManager, DatasetConfigManager)
   *
   * @param response thirdeye client response
   * @param rc TimeSeriesRequestContainer
   * @return response as dataframe
   */
  public static DataFrame evaluateResponse(ThirdEyeResponse response, TimeSeriesRequestContainer rc) throws Exception {
    return makeTimestamps(evaluateExpressions(parseResponse(response), rc.getExpressions()), rc.start, rc.getInterval());
  }

  /**
   * Returns a map-transformation of a given DataFrame, assuming that all values can be converted
   * to Double values. The map is keyed by series names.
   *
   * @param df dataframe
   * @return map transformation of dataframe
   */
  public static Map<String, List<Double>> toMap(DataFrame df) {
    Map<String, List<Double>> map = new HashMap<>();
    for (String series : df.getSeriesNames()) {
      map.put(series, df.getDoubles(series).toList());
    }
    return map;
  }

  /**
   * Returns a DataFrame wrapping the requested time series at the associated dataset's native
   * time granularity.
   * <br/><b>NOTE:</b> this method injects dependencies from the DAO and Cache registries.
   * @see DataFrameUtils#fetchTimeSeries(MetricSlice, MetricConfigManager, DatasetConfigManager, QueryCache)
   *
   * @param slice metric data slice
   * @return DataFrame with time series
   * @throws Exception
   */
  public static DataFrame fetchTimeSeries(MetricSlice slice) throws Exception {
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    QueryCache cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    return fetchTimeSeries(slice, metricDAO, datasetDAO, cache);
  }

  /**
   * Returns a DataFrame wrapping the requested time series at the associated dataset's native
   * time granularity.
   *
   * @param slice metric data slice
   * @param metricDAO metric config DAO
   * @param datasetDAO dataset config DAO
   * @param cache query cache
   * @return DataFrame with time series
   * @throws Exception
   */
  public static DataFrame fetchTimeSeries(MetricSlice slice, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, QueryCache cache) throws Exception {
    String ref = String.format("%s-%d-%d", Thread.currentThread().getName(), slice.metricId, System.nanoTime());
    RequestContainer req = makeTimeSeriesRequest(slice, ref, metricDAO, datasetDAO);
    ThirdEyeResponse resp = cache.getQueryResult(req.request);
    return evaluateExpressions(parseResponse(resp), req.expressions);
  }

  /**
   * Constructs and wraps a request for a metric with derived expressions. Resolves all
   * required dependencies from the Thirdeye database.
   * <br/><b>NOTE:</b> this method injects dependencies from the DAO registry.
   * @see DataFrameUtils#makeTimeSeriesRequest(MetricSlice slice, String, MetricConfigManager, DatasetConfigManager)
   *
   * @param slice metric data slice
   * @param reference unique identifier for request
   * @return RequestContainer
   * @throws Exception
   */
  public static TimeSeriesRequestContainer makeTimeSeriesRequest(MetricSlice slice, String reference) throws Exception {
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    return makeTimeSeriesRequest(slice, reference, metricDAO, datasetDAO);
  }

  /**
   * Constructs and wraps a request for a metric with derived expressions. Resolves all
   * required dependencies from the Thirdeye database. Also aligns start and end timestamps by
   * rounding them down (start) and up (end) to align with metric time granularity boundaries.
   * <br/><b>NOTE:</b> the aligned end timestamp is still exclusive.
   *
   * @param slice metric data slice
   * @param reference unique identifier for request
   * @param metricDAO metric config DAO
   * @param datasetDAO dataset config DAO
   * @return TimeSeriesRequestContainer
   * @throws Exception
   */
  public static TimeSeriesRequestContainer makeTimeSeriesRequestAligned(MetricSlice slice, String reference, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) throws Exception {
    MetricConfigDTO metric = metricDAO.findById(slice.metricId);
    if(metric == null)
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.metricId));

    DatasetConfigDTO dataset = datasetDAO.findByDataset(metric.getDataset());
    if(dataset == null)
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id '%d'", metric.getDataset(), metric.getId()));

    List<MetricExpression> expressions = Utils.convertToMetricExpressions(metric.getName(),
        metric.getDefaultAggFunction(), metric.getDataset());

    TimeGranularity granularity = dataset.bucketTimeGranularity();
    if (!MetricSlice.NATIVE_GRANULARITY.equals(slice.granularity)) {
      granularity = slice.granularity;
    }

    DateTimeZone timezone = DateTimeZone.forID(dataset.getTimezone());
    Period period = granularity.toPeriod();

    DateTime start = new DateTime(slice.start, timezone).withFields(makeOrigin(period.getPeriodType()));
    DateTime end = new DateTime(slice.end, timezone).withFields(makeOrigin(period.getPeriodType()));

    MetricSlice alignedSlice = MetricSlice.from(slice.metricId, start.getMillis(), end.getMillis(), slice.filters, slice.granularity);

    ThirdEyeRequest request = makeThirdEyeRequestBuilder(alignedSlice, metric, dataset, expressions, metricDAO)
        .setGroupByTimeGranularity(granularity)
        .build(reference);

    return new TimeSeriesRequestContainer(request, expressions, start, end, granularity.toPeriod());
  }

  /**
   * Constructs and wraps a request for a metric with derived expressions. Resolves all
   * required dependencies from the Thirdeye database.
   *
   * @param slice metric data slice
   * @param reference unique identifier for request
   * @param metricDAO metric config DAO
   * @param datasetDAO dataset config DAO
   * @return TimeSeriesRequestContainer
   * @throws Exception
   */
  public static TimeSeriesRequestContainer makeTimeSeriesRequest(MetricSlice slice, String reference, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) throws Exception {
    MetricConfigDTO metric = metricDAO.findById(slice.metricId);
    if(metric == null)
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.metricId));

    DatasetConfigDTO dataset = datasetDAO.findByDataset(metric.getDataset());
    if(dataset == null)
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id '%d'", metric.getDataset(), metric.getId()));

    List<MetricExpression> expressions = Utils.convertToMetricExpressions(metric.getName(),
        metric.getDefaultAggFunction(), metric.getDataset());

    TimeGranularity granularity = dataset.bucketTimeGranularity();
    if (!MetricSlice.NATIVE_GRANULARITY.equals(slice.granularity)) {
      granularity = slice.granularity;
    }

    DateTimeZone timezone = DateTimeZone.forID(dataset.getTimezone());
    Period period = granularity.toPeriod();

    DateTime start = new DateTime(slice.start, timezone).withFields(makeOrigin(period.getPeriodType()));
    DateTime end = new DateTime(slice.end, timezone).withFields(makeOrigin(period.getPeriodType()));

    ThirdEyeRequest request = makeThirdEyeRequestBuilder(slice, metric, dataset, expressions, metricDAO)
        .setGroupByTimeGranularity(granularity)
        .build(reference);

    return new TimeSeriesRequestContainer(request, expressions, start, end, granularity.toPeriod());
  }

  /**
   * Constructs and wraps a request for a metric with derived expressions. Resolves all
   * required dependencies from the Thirdeye database.
   *
   * @param slice metric data slice
   * @param dimensions dimensions to group by
   * @param limit top k element limit ({@code -1} for default)
   * @param reference unique identifier for request
   * @return RequestContainer
   * @throws Exception
   */
  public static RequestContainer makeAggregateRequest(MetricSlice slice, List<String> dimensions, int limit, String reference) throws Exception {
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    return makeAggregateRequest(slice, dimensions, limit, reference, metricDAO, datasetDAO);
  }

  /**
   * Constructs and wraps a request for a metric with derived expressions. Resolves all
   * required dependencies from the Thirdeye database.
   *
   * @param slice metric data slice
   * @param dimensions dimensions to group by
   * @param limit top k element limit ({@code -1} for default)
   * @param reference unique identifier for request
   * @param metricDAO metric config DAO
   * @param datasetDAO dataset config DAO
   * @return RequestContainer
   * @throws Exception
   */
  public static RequestContainer makeAggregateRequest(MetricSlice slice, List<String> dimensions, int limit, String reference, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) throws Exception {
    MetricConfigDTO metric = metricDAO.findById(slice.metricId);
    if(metric == null)
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.metricId));

    DatasetConfigDTO dataset = datasetDAO.findByDataset(metric.getDataset());
    if(dataset == null)
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id '%d'", metric.getDataset(), metric.getId()));

    List<MetricExpression> expressions = Utils.convertToMetricExpressions(metric.getName(),
        metric.getDefaultAggFunction(), metric.getDataset());

    ThirdEyeRequest request = makeThirdEyeRequestBuilder(slice, metric, dataset, expressions, metricDAO)
        .setGroupBy(dimensions)
        .setLimit(limit)
        .build(reference);

    return new RequestContainer(request, expressions);
  }

  /**
   * Returns partial to zero out date fields based on period type
   *
   * @return partial
   */
  public static Partial makeOrigin(PeriodType type) {
    List<DateTimeFieldType> fields = new ArrayList<>();

    if (PeriodType.millis().equals(type)) {
      // left blank

    } else if (PeriodType.seconds().equals(type)) {
      fields.add(DateTimeFieldType.millisOfSecond());

    } else if (PeriodType.minutes().equals(type)) {
      fields.add(DateTimeFieldType.secondOfMinute());
      fields.add(DateTimeFieldType.millisOfSecond());

    } else if (PeriodType.hours().equals(type)) {
      fields.add(DateTimeFieldType.minuteOfHour());
      fields.add(DateTimeFieldType.secondOfMinute());
      fields.add(DateTimeFieldType.millisOfSecond());

    } else if (PeriodType.days().equals(type)) {
      fields.add(DateTimeFieldType.hourOfDay());
      fields.add(DateTimeFieldType.minuteOfHour());
      fields.add(DateTimeFieldType.secondOfMinute());
      fields.add(DateTimeFieldType.millisOfSecond());

    } else if (PeriodType.months().equals(type)) {
      fields.add(DateTimeFieldType.dayOfMonth());
      fields.add(DateTimeFieldType.hourOfDay());
      fields.add(DateTimeFieldType.minuteOfHour());
      fields.add(DateTimeFieldType.secondOfMinute());
      fields.add(DateTimeFieldType.millisOfSecond());

    } else {
      throw new IllegalArgumentException(String.format("Unsupported PeriodType '%s'", type));
    }

    int[] zeros = new int[fields.size()];
    Arrays.fill(zeros, 0);

    // workaround for dayOfMonth > 0 constraint
    if (PeriodType.months().equals(type)) {
      zeros[0] = 1;
    }

    return new Partial(fields.toArray(new DateTimeFieldType[fields.size()]), zeros);
  }

  /**
   * Returns the series with the period added, given the timezone
   *
   * @param s series
   * @param period time period
   * @param timezone time zone
   * @return offset time series
   */
  public static LongSeries addPeriod(Series s, final Period period, final DateTimeZone timezone) {
    return s.map(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return new DateTime(values[0], timezone).plus(period).getMillis();
      }
    });
  }

  /**
   * Helper: Returns a pre-populated ThirdeyeRequestBuilder instance. Removes invalid filter values.
   *
   * @param slice metric data slice
   * @param metric metric dto
   * @param dataset dataset dto
   * @param expressions metric expressions
   * @param metricDAO metric config DAO
   * @return ThirdeyeRequestBuilder
   * @throws ExecutionException
   */
  private static ThirdEyeRequest.ThirdEyeRequestBuilder makeThirdEyeRequestBuilder(MetricSlice slice, MetricConfigDTO metric, DatasetConfigDTO dataset, List<MetricExpression> expressions, MetricConfigManager metricDAO) throws ExecutionException {
    List<MetricFunction> functions = new ArrayList<>();
    for(MetricExpression exp : expressions) {
      functions.addAll(exp.computeMetricFunctions());
    }

    return ThirdEyeRequest.newBuilder()
        .setStartTimeInclusive(slice.start)
        .setEndTimeExclusive(slice.end)
        .setFilterSet(slice.filters)
        .setMetricFunctions(functions)
        .setDataSource(dataset.getDataSource());
  }

  /**
   * Reads in a ThirdEyeResultSetGroup and returns it as a DataFrame.
   * <br/><b>NOTE:</b> This code duplicates DataFrame.fromPinotResult() due to lack of interfaces in pinot
   *
   * @param resultSetGroup pinot query result
   * @return Pinot query result as DataFrame
   * @throws IllegalArgumentException if the result cannot be parsed
   */
  public static DataFrame fromThirdEyeResult(ThirdEyeResultSetGroup resultSetGroup) {
    if (resultSetGroup.size() <= 0)
      throw new IllegalArgumentException("Query did not return any results");

    if (resultSetGroup.size() == 1) {
      ThirdEyeResultSet resultSet = resultSetGroup.getResultSets().get(0);

      if (resultSet.getColumnCount() == 1 && resultSet.getRowCount() == 0) {
        // empty result
        return new DataFrame();

      } else if (resultSet.getColumnCount() == 1 && resultSet.getRowCount() == 1 && resultSet.getGroupKeyLength() == 0) {
        // aggregation result

        DataFrame df = new DataFrame();
        String function = resultSet.getColumnName(0);
        String value = resultSet.getString(0, 0);
        df.addSeries(function, DataFrame.toSeries(value));
        return df;

      } else if (resultSet.getColumnCount() >= 1 && resultSet.getGroupKeyLength() == 0) {
        // selection result

        DataFrame df = new DataFrame();
        for (int i = 0; i < resultSet.getColumnCount(); i++) {
          df.addSeries(resultSet.getColumnName(i), makeSelectionSeries(resultSet, i));
        }
        return df;

      }
    }

    // group by result
    ThirdEyeResultSet firstResultSet = resultSetGroup.getResultSets().get(0);
    String[] groupKeyNames = new String[firstResultSet.getGroupKeyLength()];
    for(int i=0; i<firstResultSet.getGroupKeyLength(); i++) {
      groupKeyNames[i] = firstResultSet.getGroupKeyColumnName(i);
    }

    DataFrame df = new DataFrame();
    for (String groupKeyName : groupKeyNames) {
      df.addSeries(groupKeyName, StringSeries.empty());
    }
    df.setIndex(groupKeyNames);

    for(int i=0; i<resultSetGroup.size(); i++) {
      ThirdEyeResultSet resultSet = resultSetGroup.getResultSets().get(i);
      String function = resultSet.getColumnName(0);

      // group keys
      DataFrame dfColumn = new DataFrame();
      for(int j=0; j<resultSet.getGroupKeyLength(); j++) {
        dfColumn.addSeries(groupKeyNames[j], makeGroupByGroupSeries(resultSet, j));
      }
      dfColumn.setIndex(groupKeyNames);

      // values
      dfColumn.addSeries(function, makeGroupByValueSeries(resultSet));

      df = df.joinOuter(dfColumn);
    }

    return df;
  }

  private static Series makeSelectionSeries(ThirdEyeResultSet resultSet, int colIndex) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return StringSeries.empty();

    String[] values = new String[rowCount];
    for(int i=0; i<rowCount; i++) {
      values[i] = resultSet.getString(i, colIndex);
    }

    return DataFrame.toSeries(values);
  }

  private static Series makeGroupByValueSeries(ThirdEyeResultSet resultSet) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return StringSeries.empty();

    String[] values = new String[rowCount];
    for(int i=0; i<rowCount; i++) {
      values[i] = resultSet.getString(i, 0);
    }

    return DataFrame.toSeries(values);
  }

  private static Series makeGroupByGroupSeries(ThirdEyeResultSet resultSet, int keyIndex) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return StringSeries.empty();

    String[] values = new String[rowCount];
    for(int i=0; i<rowCount; i++) {
      values[i] = resultSet.getGroupKeyColumnValue(i, keyIndex);
    }

    return DataFrame.toSeries(values);
  }
}
