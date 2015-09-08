package com.linkedin.thirdeye.anomaly.builtin;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.util.LRUMap;
import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.ResultProperties;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.anomaly.api.function.exception.FunctionDidNotEvaluateException;
import com.linkedin.thirdeye.anomaly.api.function.exception.IllegalFunctionException;
import com.linkedin.thirdeye.anomaly.lib.kalman.StateSpaceAnomalyDetector;
import com.linkedin.thirdeye.anomaly.lib.kalman.StateSpaceDataPoint;
import com.linkedin.thirdeye.anomaly.lib.util.MetricTimeSeriesUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

/**
 * This class wraps around StateSpaceAnomalyDetector
 */
public class KalmanAnomalyDetectionFunction implements AnomalyDetectionFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(KalmanAnomalyDetectionFunction.class);

  private static final String PROP_DEFAULT_SEASONAL = "0";
  private static final String PROP_DEFAULT_KNOB = "10000";
  private static final String PROP_DEFAULT_ORDER = "1";
  private static final String PROP_DEFAULT_P_VALUE_THRESHOLD = "0.05";

  /**
   * Previous function state by kv. TODO : this is a hack...
   *
   * Save the EstimatedStateNoise with keys identified by dimensionKey and the functionConfig.
   *  - This doesn't improve worst case performance (cold-start)
   *  - Seeding optimization with a previous EstimatedStateNoise speeds up convergence by 3-5x.
   *  - This is kept in memory to avoid bad state from persisting between runs.
   *  - If there are 10000 series being analyzed on a single machine every hour, then we probably need more machines.
   */
  private static final LRUMap<String, Double> NON_DURABLE_STATE_KV_PAIRS = new LRUMap<>(100, 10000);

  /**
   * @param functionProperties
   * @param dimensionKey
   * @return
   *  Key uniquely identifying this function configuration and the dimension key it is run on.
   */
  private static final String getKVMapKeyString(FunctionProperties functionProperties, DimensionKey dimensionKey) {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOGGER.error("no md5 available", e);
    }
    md.update(functionProperties.toString().getBytes());
    md.update(dimensionKey.toString().getBytes());
    byte[] digest = md.digest();
    StringBuffer sb = new StringBuffer();
    for (byte b : digest) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }

  private String metric;
  private double pValueThreshold;

  private int trainSize;
  private TimeUnit trainUnit;

  private int bucketSize;
  private TimeUnit bucketUnit;

//  private double stateNoise;
  private double r;
//  private double startingStateValue;
//  private double observationNoise;
  private int order;
//  private int bootStraps;
  private int seasonal;
//  private double seasonalLevel;

  private FunctionProperties functionConfig;

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction#init(com.linkedin.thirdeye.anomaly.api.function.StarTreeConfig, com.linkedin.thirdeye.anomaly.api.FunctionProperties)
   */
  @Override
  public void init(StarTreeConfig starTreeConfig, FunctionProperties functionConfig) throws IllegalFunctionException {
    metric = functionConfig.getProperty("metric");

    pValueThreshold = Double.parseDouble(functionConfig.getProperty("pValueThreshold", PROP_DEFAULT_P_VALUE_THRESHOLD));

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

    order = Integer.valueOf(functionConfig.getProperty("order", PROP_DEFAULT_ORDER));
//    bootStraps = Integer.parseInt(functionConfig.getProperty("bootstrap"));

    r = Double.parseDouble(functionConfig.getProperty("knob", PROP_DEFAULT_KNOB));

//    startingStateValue = Double.parseDouble(functionConfig.getProperty("startingStateValue"));
//    stateNoise = Double.parseDouble(functionConfig.getProperty("stateNoise"));

    seasonal = Integer.parseInt(functionConfig.getProperty("seasonal", PROP_DEFAULT_SEASONAL));

    System.out.println(functionConfig.toString());
//    observationNoise = r * stateNoise;

//    if (seasonal > 0) {
//      seasonalLevel = Double.parseDouble(functionConfig.getProperty("seasonalLevel"));
//    }

    this.functionConfig = functionConfig;
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
    Set<String> ret = new HashSet<String>();
    ret.add(metric);
    return ret;
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction#getMinimumMonitoringIntervalTimeGranularity()
   */
  @Override
  public TimeGranularity getMinimumMonitoringIntervalTimeGranularity() {
    return null;
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction#analyze(com.linkedin.thirdeye.anomaly.api.function.DimensionKey, com.linkedin.thirdeye.anomaly.api.function.MetricTimeSeries, com.linkedin.thirdeye.anomaly.api.function.TimeRange, java.util.List)
   */
  @Override
  public List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries series, TimeRange monitoringWindow,
      List<AnomalyResult> anomalyHistory) {

    long trainStartInput = Collections.min(series.getTimeWindowSet());
    long trainEndInput = Collections.max(series.getTimeWindowSet());
    long bucketMillis = bucketUnit.toMillis(bucketSize);

    // data to leave out from training
    Set<Long> omitTimestamps = new HashSet<Long>();

    // convert the data to arrays
    Pair<long[], double[]> arraysFromSeries = MetricTimeSeriesUtils.toArray(series, metric, bucketMillis,
        omitTimestamps, 0.0);
    long[] timestamps = arraysFromSeries.getFirst();
    double[] observations = arraysFromSeries.getSecond();

    // omit previously detected anomalies from training
    for (AnomalyResult ar : anomalyHistory) {
      omitTimestamps.add(ar.getTimeWindow());
    }

    StateSpaceAnomalyDetector stateSpaceDetector = new StateSpaceAnomalyDetector(
        trainStartInput,
        trainEndInput,
        -1, // stepsAhead
        bucketUnit.toMillis(bucketSize),
        omitTimestamps,
        seasonal,
        order,
        order + seasonal - 1, // numStates
        1, // outputStates
        r);

    // cached state hack
    final String FUNCTION_INVOCATION_STATE_KEY = getKVMapKeyString(functionConfig, dimensionKey);
    Double initialEstimatedStateNoise = NON_DURABLE_STATE_KV_PAIRS.get(FUNCTION_INVOCATION_STATE_KEY);
    if (initialEstimatedStateNoise != null) {
      stateSpaceDetector.setInitialEstimatedStateNoise(initialEstimatedStateNoise);
    }

    Map<Long, StateSpaceDataPoint> resultsByTimeWindow;
    try {
      LOGGER.info("detecting anomalies using kalman filter");
      long offset = TimeUnit.MILLISECONDS.convert(bucketSize, bucketUnit);
      long startTime = System.nanoTime();
      resultsByTimeWindow = stateSpaceDetector.detectAnomalies(observations, timestamps, offset);
      LOGGER.info("algorithm took {} seconds", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

      // cached state
      NON_DURABLE_STATE_KV_PAIRS.put(FUNCTION_INVOCATION_STATE_KEY, stateSpaceDetector.getEstimatedStateNoise());

    } catch (Exception e) {
      throw new FunctionDidNotEvaluateException("something went wrong", e);
    }

    // translate FanomalyDataPoints to AnomalyResults (sort them to make the anomaly ids in the db easier to look at)
    List<AnomalyResult> anomalyResults = new LinkedList<AnomalyResult>();
    for (long timeWindow : new TreeSet<>(resultsByTimeWindow.keySet()))
    {
      StateSpaceDataPoint stateSpaceDataPoint = resultsByTimeWindow.get(timeWindow);

      if (stateSpaceDataPoint.pValue < pValueThreshold)
      {
        // inserting non strings into properties messes with store method
        ResultProperties resultProperties = new ResultProperties();
        resultProperties.put("actualValue", "" + stateSpaceDataPoint.actualValue);
        resultProperties.put("predictedValue", "" + stateSpaceDataPoint.predictedValue);
        resultProperties.put("stdError", "" + stateSpaceDataPoint.stdError);
        resultProperties.put("pValue", "" + stateSpaceDataPoint.pValue);
        resultProperties.put("predictedDate", "" + stateSpaceDataPoint.predictedDate);
        resultProperties.setProperty("timestamp", new DateTime(timeWindow).toString());
        anomalyResults.add(new AnomalyResult(
            true, timeWindow, stateSpaceDataPoint.pValue, stateSpaceDataPoint.actualValue, resultProperties));
      }
    }

    return anomalyResults;
  }

}
