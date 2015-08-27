package com.linkedin.thirdeye.anomaly.builtin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.anomaly.api.function.exception.IllegalFunctionException;
import com.linkedin.thirdeye.anomaly.lib.util.MetricTimeSeriesUtils;
import com.linkedin.thirdeye.anomaly.lib.util.STLDecompositionUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

/**
 *
 */
public class ScanStatisticsAnomalyDetectionFunction implements AnomalyDetectionFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScanStatisticsAnomalyDetectionFunction.class);

  private static final String PROP_DEFAULT_SEASONAL = "168";
  private static final String PROP_DEFAULT_P_VALUE_THRESHOLD = "0.05";

  private String metric;
  private double pValueThreshold;

  private int trainSize;
  private TimeUnit trainUnit;

  private int bucketSize;
  private TimeUnit bucketUnit;

  private int seasonal;

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction#init(com.linkedin.thirdeye.api.StarTreeConfig, com.linkedin.thirdeye.anomaly.api.FunctionProperties)
   */
  @Override
  public void init(StarTreeConfig starTreeConfig, FunctionProperties functionConfig) throws IllegalFunctionException {
    metric = functionConfig.getProperty("metric");

    pValueThreshold = Double.parseDouble(functionConfig.getProperty("pValueThreshold", PROP_DEFAULT_P_VALUE_THRESHOLD));
    if (pValueThreshold <= 0) {
      throw new IllegalFunctionException("pValueThreshold must be > 0");
    }

    trainSize = Integer.parseInt(functionConfig.getProperty("trainSize"));
    trainUnit = TimeUnit.valueOf(functionConfig.getProperty("trainUnit"));
    if (trainSize < 0) {
      throw new IllegalFunctionException("trainSize must be >= 0");
    }

    bucketSize = Integer.parseInt(functionConfig.getProperty("bucketSize"));
    bucketUnit = TimeUnit.valueOf(functionConfig.getProperty("bucketUnit"));
    if (bucketSize < 0) {
      throw new IllegalFunctionException("bucketSize must be >= 0");
    }

    seasonal = Integer.parseInt(functionConfig.getProperty("seasonal", PROP_DEFAULT_SEASONAL));

    // TODO add the remaining arguments
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction#getTrainingWindowTimeGranularity()
   */
  @Override
  public TimeGranularity getTrainingWindowTimeGranularity() {
    return new TimeGranularity(trainSize, trainUnit);
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction#getAggregationTimeGranularity()
   */
  @Override
  public TimeGranularity getAggregationTimeGranularity() {
    return new TimeGranularity(bucketSize, bucketUnit);
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction#getMetrics()
   */
  @Override
  public Set<String> getMetrics() {
    Set<String> ret = new HashSet<>();
    ret.add(metric);
    return ret;
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction#analyze(com.linkedin.thirdeye.api.DimensionKey, com.linkedin.thirdeye.api.MetricTimeSeries, com.linkedin.thirdeye.api.TimeRange, java.util.List)
   */
  @Override
  public List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries series, TimeRange detectionInterval,
      List<AnomalyResult> anomalyHistory) {
    long startTimeStamp = Collections.min(series.getTimeWindowSet());
    long endTimeStamp = Collections.max(series.getTimeWindowSet());
    long bucketMillis = bucketUnit.toMillis(bucketSize);

    // convert the data to arrays
    Pair<long[], double[]> arraysFromSeries = MetricTimeSeriesUtils.toArray(series, metric, bucketMillis,
        null, Double.NaN);
    long[] timestamps = arraysFromSeries.getFirst();
    double[] observations = arraysFromSeries.getSecond();
    removeHolesByAveragingNeighbors(observations);

    // call stl library
    double[] observationsMinusSeasonality = STLDecompositionUtils.removeSeasonality(timestamps, observations, seasonal);

    List<AnomalyResult> anomalyResults = new ArrayList<AnomalyResult>();

    /*
     * TODO : do scan statistics here
     */

    return anomalyResults;
  }

  /**
   * @param arr
   *  The array whose NaN values will be removed
   *
   * Note, the first data point cannot be a hole by construction from the conversion of MetricTimeSeries with
   * (min, max) times.
   */
  private void removeHolesByAveragingNeighbors(double[] arr) {
    for (int i = 0; i < arr.length; i++) {
      if (Double.isNaN(arr[i])) {
        double sum = 0.0;
        int count = 0;
        if (i - 1 >= 0 && !Double.isNaN(arr[i - 1])) {
          sum += arr[i - 1];
          count++;
        }
        if (i + 1 < arr.length && !Double.isNaN(arr[i + 1])) {
          sum += arr[i + 1];
          count++;
        }
        arr[i] = sum / count;
      }
    }
  }

}
