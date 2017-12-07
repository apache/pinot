package com.linkedin.thirdeye.dataframe.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.ThirdEyeResponseRow;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * Utility class for ThirdEye-specific parsers and transformers of data related to DataFrame.
 *
 */
public class DataFrameUtils {
  public static final String COL_TIME = "timestamp";
  public static final String COL_VALUE = "value";

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
    df.addSeries(COL_TIME, timeBuilder.build());
    df.setIndex(COL_TIME);

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

    return df.sortedBy(COL_TIME);
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

    return df.addSeries(COL_VALUE, values);
  }

  /**
   * Returns the DataFrame with timestamps aligned to a start offset and an interval. Also, missing
   * rows (between {@code start} and {@code end} in {@code interval} steps) are fill in with NULL
   * values.
   *
   * @param df thirdeye response dataframe
   * @param start start offset
   * @param end end offset
   * @param interval timestep multiple
   * @return dataframe with modified timestamps
   */
  public static DataFrame alignTimestamps(DataFrame df, final long start, final long end, final long interval) {
    int count = (int)((end - start) / interval);
    LongSeries timestamps = LongSeries.sequence(start, count, interval);
    DataFrame dfTime = new DataFrame(COL_TIME, timestamps);
    DataFrame dfOffset = new DataFrame(df).mapInPlace(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return (values[0] * interval) + start;
      }
    }, COL_TIME);

    return dfTime.joinLeft(dfOffset);
  }

  /**
   * Returns a Thirdeye response parsed as a DataFrame. The method stores the time values in
   * {@code COL_TIME} by default, and creates columns for each groupBy attribute and for each
   * MetricFunction specified in the request. It further evaluates expressions for derived
   * metrics.
   * @see DataFrameUtils#makeAggregateRequest(MetricSlice, List, String, MetricConfigManager, DatasetConfigManager)
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
    long start = ((rc.start + rc.interval - 1) / rc.interval) * rc.interval;
    long end = ((rc.end + rc.interval - 1) / rc.interval) * rc.interval;
    return alignTimestamps(evaluateExpressions(parseResponse(response), rc.getExpressions()), start, end, rc.getInterval());
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

    long timeGranularity = granularity.toMillis();
    long start = (slice.start / timeGranularity) * timeGranularity;
    long end = ((slice.end + timeGranularity - 1) / timeGranularity) * timeGranularity;

    MetricSlice alignedSlice = MetricSlice.from(slice.metricId, start, end, slice.filters, slice.granularity);

    ThirdEyeRequest request = makeThirdEyeRequestBuilder(alignedSlice, metric, dataset, expressions)
        .setGroupByTimeGranularity(granularity)
        .build(reference);

    return new TimeSeriesRequestContainer(request, expressions, start, end, timeGranularity);
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

    ThirdEyeRequest request = makeThirdEyeRequestBuilder(slice, metric, dataset, expressions)
        .setGroupByTimeGranularity(granularity)
        .build(reference);

    return new TimeSeriesRequestContainer(request, expressions, slice.start, slice.end, granularity.toMillis());
  }

  /**
   * Constructs and wraps a request for a metric with derived expressions. Resolves all
   * required dependencies from the Thirdeye database.
   *
   * @param slice metric data slice
   * @param dimensions dimensions to group by
   * @param reference unique identifier for request
   * @param metricDAO metric config DAO
   * @param datasetDAO dataset config DAO
   * @return RequestContainer
   * @throws Exception
   */
  public static RequestContainer makeAggregateRequest(MetricSlice slice, List<String> dimensions, String reference, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) throws Exception {
    MetricConfigDTO metric = metricDAO.findById(slice.metricId);
    if(metric == null)
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.metricId));

    DatasetConfigDTO dataset = datasetDAO.findByDataset(metric.getDataset());
    if(dataset == null)
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id '%d'", metric.getDataset(), metric.getId()));

    List<MetricExpression> expressions = Utils.convertToMetricExpressions(metric.getName(),
        metric.getDefaultAggFunction(), metric.getDataset());

    ThirdEyeRequest request = makeThirdEyeRequestBuilder(slice, metric, dataset, expressions)
        .setGroupBy(dimensions)
        .build(reference);

    return new RequestContainer(request, expressions);
  }

  /**
   * Helper: Returns a pre-populated ThirdeyeRequestBuilder instance.
   *
   * @param slice metric data slice
   * @param metric metric dto
   * @param dataset dataset dto
   * @param expressions metric expressions
   * @return ThirdeyeRequestBuilder
   * @throws ExecutionException
   */
  private static ThirdEyeRequest.ThirdEyeRequestBuilder makeThirdEyeRequestBuilder(MetricSlice slice, MetricConfigDTO metric, DatasetConfigDTO dataset, List<MetricExpression> expressions) throws ExecutionException {
    List<MetricFunction> functions = new ArrayList<>();
    for(MetricExpression exp : expressions) {
      functions.addAll(exp.computeMetricFunctions());
    }

    Multimap<String, String> effectiveFilters = ArrayListMultimap.create();
    for (String dimName : slice.filters.keySet()) {
      if (dataset.getDimensions().contains(dimName)) {
        effectiveFilters.putAll(dimName, slice.filters.get(dimName));
      }
    }

    return ThirdEyeRequest.newBuilder()
        .setStartTimeInclusive(slice.start)
        .setEndTimeExclusive(slice.end)
        .setFilterSet(effectiveFilters)
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
