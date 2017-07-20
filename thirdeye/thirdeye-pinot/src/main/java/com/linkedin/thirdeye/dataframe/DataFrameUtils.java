package com.linkedin.thirdeye.dataframe;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.Utils;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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

    df.addSeries(COL_VALUE, values);

    return df;
  }

  /**
   * Returns a Thirdeye response parsed as a DataFrame. The method stores the time values in
   * {@code COL_TIME} by default, and creates columns for each groupBy attribute and for each
   * MetricFunction specified in the request. It further evaluates expressions for derived
   * metrics.
   * @see DataFrameUtils#makeAggregateRequest(long, long, long, List, String, MetricConfigManager, DatasetConfigManager)
   * @see DataFrameUtils#makeTimeSeriesRequest(long, long, long, String, MetricConfigManager, DatasetConfigManager)
   *
   * @param response thirdeye client response
   * @param rc RequestContainer
   * @return response as dataframe
   */
  public static DataFrame evaluateResponse(ThirdEyeResponse response, RequestContainer rc) throws Exception {
    return evaluateExpressions(parseResponse(response), rc.getExpressions());
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
   * @see DataFrameUtils#fetchTimeSeries(long, long, long, MetricConfigManager, DatasetConfigManager, QueryCache)
   *
   * @param metricId metric id
   * @param start start time in millis (inclusive)
   * @param end end time in millis (exclusive)
   * @return DataFrame with time series
   * @throws Exception
   */
  public static DataFrame fetchTimeSeries(long metricId, long start, long end) throws Exception {
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    QueryCache cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    return fetchTimeSeries(metricId, start, end, metricDAO, datasetDAO, cache);
  }

  /**
   * Returns a DataFrame wrapping the requested time series at the associated dataset's native
   * time granularity.
   *
   * @param metricId metric id
   * @param start start time in millis (inclusive)
   * @param end end time in millis (exclusive)
   * @param metricDAO metric config DAO
   * @param datasetDAO dataset config DAO
   * @param cache query cache
   * @return DataFrame with time series
   * @throws Exception
   */
  public static DataFrame fetchTimeSeries(long metricId, long start, long end, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, QueryCache cache) throws Exception {
    String ref = String.format("%s-%d-%d", Thread.currentThread().getName(), metricId, System.nanoTime());
    RequestContainer req = makeTimeSeriesRequest(metricId, start, end, ref, metricDAO, datasetDAO);
    ThirdEyeResponse resp = cache.getQueryResult(req.request);
    return evaluateExpressions(parseResponse(resp), req.expressions);
  }

  /**
   * Constructs and wraps a request for a metric with derived expressions. Resolves all
   * required dependencies from the Thirdeye database.
   * <br/><b>NOTE:</b> this method injects dependencies from the DAO registry.
   * @see DataFrameUtils#makeTimeSeriesRequest(long, long, long, String, MetricConfigManager, DatasetConfigManager)
   *
   * @param metricId metric id
   * @param start start time in millis (inclusive)
   * @param end end time in millis (exclusive)
   * @param reference unique identifier for request
   * @return RequestContainer
   * @throws Exception
   */
  public static RequestContainer makeTimeSeriesRequest(long metricId, long start, long end, String reference) throws Exception {
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    return makeTimeSeriesRequest(metricId, start, end, reference, metricDAO, datasetDAO);
  }

  /**
   * Constructs and wraps a request for a metric with derived expressions. Resolves all
   * required dependencies from the Thirdeye database.
   *
   * @param metricId metric id
   * @param start start time in millis (inclusive)
   * @param end end time in millis (exclusive)
   * @param reference unique identifier for request
   * @param metricDAO metric config DAO
   * @param datasetDAO dataset config DAO
   * @return RequestContainer
   * @throws Exception
   */
  public static RequestContainer makeTimeSeriesRequest(long metricId, long start, long end, String reference, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) throws Exception {
    MetricConfigDTO metric = metricDAO.findById(metricId);
    if(metric == null)
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", metricId));

    DatasetConfigDTO dataset = datasetDAO.findByDataset(metric.getDataset());
    if(dataset == null)
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id '%d'", metric.getDataset(), metric.getId()));

    List<MetricFunction> functions = new ArrayList<>();
    List<MetricExpression> expressions = Utils.convertToMetricExpressions(metric.getName(),
        metric.getDefaultAggFunction(), metric.getDataset());
    for(MetricExpression exp : expressions) {
      functions.addAll(exp.computeMetricFunctions());
    }

    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .setStartTimeInclusive(start)
        .setEndTimeExclusive(end)
        .setMetricFunctions(functions)
        .setGroupByTimeGranularity(dataset.bucketTimeGranularity())
        .setDataSource(dataset.getDataSource())
        .build(reference);

    return new RequestContainer(request, expressions);
  }

  /**
   * Constructs and wraps a request for a metric with derived expressions. Resolves all
   * required dependencies from the Thirdeye database.
   *
   * @param metricId metric id
   * @param start start time in millis (inclusive)
   * @param end end time in millis (exclusive)
   * @param reference unique identifier for request
   * @param metricDAO metric config DAO
   * @param datasetDAO dataset config DAO
   * @return RequestContainer
   * @throws Exception
   */
  public static RequestContainer makeAggregateRequest(long metricId, long start, long end, List<String> dimensions, String reference, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) throws Exception {
    MetricConfigDTO metric = metricDAO.findById(metricId);
    if(metric == null)
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", metricId));

    DatasetConfigDTO dataset = datasetDAO.findByDataset(metric.getDataset());
    if(dataset == null)
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id '%d'", metric.getDataset(), metric.getId()));

    List<MetricFunction> functions = new ArrayList<>();
    List<MetricExpression> expressions = Utils.convertToMetricExpressions(metric.getName(),
        metric.getDefaultAggFunction(), metric.getDataset());
    for(MetricExpression exp : expressions) {
      functions.addAll(exp.computeMetricFunctions());
    }

    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .setStartTimeInclusive(start)
        .setEndTimeExclusive(end)
        .setMetricFunctions(functions)
        .setDataSource(dataset.getDataSource())
        .setGroupBy(dimensions)
        .build(reference);

    return new RequestContainer(request, expressions);
  }

  /**
   * Wrapper for ThirdEye request with derived metric expressions
   */
  public static final class RequestContainer {
    final ThirdEyeRequest request;
    final List<MetricExpression> expressions;

    RequestContainer(ThirdEyeRequest request, List<MetricExpression> expressions) {
      this.request = request;
      this.expressions = expressions;
    }

    public ThirdEyeRequest getRequest() {
      return request;
    }

    public List<MetricExpression> getExpressions() {
      return expressions;
    }
  }
}
