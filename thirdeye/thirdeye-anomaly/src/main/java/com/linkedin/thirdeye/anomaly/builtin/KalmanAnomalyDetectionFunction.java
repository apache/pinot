package com.linkedin.thirdeye.anomaly.builtin;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.ResultProperties;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyResult;
import com.linkedin.thirdeye.anomaly.exception.FunctionDidNotEvaluateException;
import com.linkedin.thirdeye.anomaly.exception.IllegalFunctionException;
import com.linkedin.thirdeye.anomaly.lib.fanomaly.FanomalyDataPoint;
import com.linkedin.thirdeye.anomaly.lib.fanomaly.StateSpaceAnomalyDetector;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

/**
 * This class wraps around the fanomaly StateSpaceAnomalyDetector
 */
public class KalmanAnomalyDetectionFunction implements AnomalyDetectionFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(KalmanAnomalyDetectionFunction.class);

  private static final String PROP_DEFAULT_SEASONAL = "0";
  private static final String PROP_DEFAULT_KNOB = "1000";
  private static final String PROP_DEFAULT_ORDER = "1";
  private static final String PROP_DEFAULT_P_VALUE_THRESHOLD = "0.05";

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

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction#init(com.linkedin.thirdeye.anomaly.api.external.StarTreeConfig, com.linkedin.thirdeye.anomaly.api.FunctionProperties)
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
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction#getTrainingWindowTimeGranularity()
   */
  @Override
  public TimeGranularity getTrainingWindowTimeGranularity() {
    return new TimeGranularity(trainSize, trainUnit);
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction#getAggregationTimeGranularity()
   */
  @Override
  public TimeGranularity getAggregationTimeGranularity() {
    return new TimeGranularity(bucketSize, bucketUnit);
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction#getMetrics()
   */
  @Override
  public Set<String> getMetrics() {
    Set<String> ret = new HashSet<String>();
    ret.add(metric);
    return ret;
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction#analyze(com.linkedin.thirdeye.anomaly.api.external.DimensionKey, com.linkedin.thirdeye.anomaly.api.external.MetricTimeSeries, com.linkedin.thirdeye.anomaly.api.external.TimeRange, java.util.List)
   */
  @Override
  public List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries series, TimeRange detectionInterval,
      List<AnomalyResult> anomalyHistory) {

    /*
     * Convert data input to arrays
     */
    int numObservations = series.getTimeWindowSet().size();
    double[] observations = new double[numObservations];
    long[] timestamps = new long[numObservations];

    int observationIndex = 0;
    TreeSet<Long> sortedTimestamps = new TreeSet<Long>(series.getTimeWindowSet()); // sort it
    for (long observationTimeStamp : sortedTimestamps) {
      observations[observationIndex] = series.get(observationTimeStamp, metric).doubleValue();
      timestamps[observationIndex] = observationTimeStamp;
      observationIndex++;
    }
    /*
     * Done converting data input
     */

    /*
     * Configure omit timestamps TODO: this causes a null pointer
     */
    Set<Long> omitTimestamps = new HashSet<Long>();
    for (AnomalyResult ar : anomalyHistory) {
//      omitTimestamps.add(ar.getTimeWindow());
    }

    long trainStartInput = sortedTimestamps.first();
    long trainEndInput = sortedTimestamps.last();

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

    Map<Long, FanomalyDataPoint> fAnomalyDataPoints;
    try {
      LOGGER.info("detecting anomalies using fanomaly");
      long offset = TimeUnit.MILLISECONDS.convert(bucketSize, bucketUnit);
      long startTime = System.nanoTime();
      fAnomalyDataPoints = stateSpaceDetector.DetectAnomaly(observations, timestamps, offset);
      LOGGER.info("jblas took {} seconds", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

    } catch (Exception e) {
      throw new FunctionDidNotEvaluateException("something went wrong", e);
    }

    // translate FanomalyDataPoints to AnomalyResults (sort them to make the anomaly ids in the db easier to look at)
    List<AnomalyResult> anomalyResults = new LinkedList<AnomalyResult>();
    for (long timeWindow : new TreeSet<>(fAnomalyDataPoints.keySet()))
    {
      FanomalyDataPoint fAnomalyDataPoint = fAnomalyDataPoints.get(timeWindow);

//      System.err.println("RAWOUTPUT " + dimensionKey + "\t" + entry.getKey() + "\t" + fAnomalyDataPoint.actualValue
//          + "\t" + fAnomalyDataPoint.predictedValue + "\t" + fAnomalyDataPoint.stdError + "\t"
//          + fAnomalyDataPoint.pValue);

      if (fAnomalyDataPoint.pValue < pValueThreshold)
      {
        // inserting non strings into properties messes with store method
        ResultProperties resultProperties = new ResultProperties();
        resultProperties.put("actualValue", "" + fAnomalyDataPoint.actualValue);
        resultProperties.put("predictedValue", "" + fAnomalyDataPoint.predictedValue);
        resultProperties.put("stdError", "" + fAnomalyDataPoint.stdError);
        resultProperties.put("pValue", "" + fAnomalyDataPoint.pValue);
        resultProperties.put("predictedDate", "" + fAnomalyDataPoint.predictedDate);
        anomalyResults.add(new AnomalyResult(
            true, timeWindow, fAnomalyDataPoint.pValue, fAnomalyDataPoint.actualValue, resultProperties));
      }
    }

    return anomalyResults;
  }

}
