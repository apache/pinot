package com.linkedin.thirdeye.anomaly.builtin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.ResultProperties;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.anomaly.api.function.exception.IllegalFunctionException;
import com.linkedin.thirdeye.anomaly.lib.scanstatistics.ScanStatistics;
import com.linkedin.thirdeye.anomaly.lib.scanstatistics.ScanStatistics.Pattern;
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
  private static final String PROP_DEFAULT_MIN_WINDOW_LEN = "1";
  private static final String PROP_DEFAULT_NUM_SIMULATIONS = "1000";
  private static final String PROP_DEFAULT_MIN_INCREMENT = "1";
  private static final String PROP_DEFAULT_MAX_WINDOW_LEN = "" + Integer.MAX_VALUE;

  private String metric;
  private double pValueThreshold;

  private int trainSize;
  private TimeUnit trainUnit;

  private int bucketSize;
  private TimeUnit bucketUnit;

  private int seasonal;

  private int numSimulation;
  private int minWindowLength;
  private int maxWindowLength;
  private int minIncrement;

  private Pattern pattern;

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

    numSimulation = Integer.parseInt(functionConfig.getProperty("numSimulations", PROP_DEFAULT_NUM_SIMULATIONS));
    minWindowLength = Integer.parseInt(functionConfig.getProperty("minWindowLength", PROP_DEFAULT_MIN_WINDOW_LEN));
    maxWindowLength = Integer.parseInt(functionConfig.getProperty("maxWindowLength", PROP_DEFAULT_MAX_WINDOW_LEN));
    minIncrement = Integer.parseInt(functionConfig.getProperty("minIncrement", PROP_DEFAULT_MIN_INCREMENT));

    pattern = Pattern.valueOf(functionConfig.getProperty("pattern"));
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
    long bucketMillis = bucketUnit.toMillis(bucketSize);

    // convert the data to arrays
    Pair<long[], double[]> arraysFromSeries = MetricTimeSeriesUtils.toArray(series, metric, bucketMillis,
        null, Double.NaN);
    long[] timestamps = arraysFromSeries.getFirst();
    double[] observations = arraysFromSeries.getSecond();
    removeMissingValuesByAveragingNeighbors(observations);

    // call stl library
    double[] observationsMinusSeasonality = STLDecompositionUtils.removeSeasonality(timestamps, observations, seasonal);

    int effectiveMaxWindowLength = (int) (detectionInterval.totalBuckets() / bucketMillis);
    effectiveMaxWindowLength = Math.min(effectiveMaxWindowLength, maxWindowLength);

    // instantiate model
    ScanStatistics scanStatistics = new ScanStatistics(
        numSimulation,
        minWindowLength,
        effectiveMaxWindowLength,
        pValueThreshold,
        pattern,
        minIncrement);

    int numBucketsToScan = (int) ((detectionInterval.getEnd() - detectionInterval.getStart()) / bucketMillis);
    int totalNumBuckets = observationsMinusSeasonality.length;
    int numTrain = totalNumBuckets - numBucketsToScan;

    // set of timestamps with anomalies
    Set<Long> anomalousTimestamps = new HashSet<Long>();
    for (AnomalyResult ar : anomalyHistory) {
      anomalousTimestamps.add(ar.getTimeWindow());
    }

    // partition the data into train and monitoring
    double[] trainingData = Arrays.copyOfRange(observationsMinusSeasonality, 0, numTrain);
    long[] trainingTimestamps = Arrays.copyOfRange(timestamps, 0, numTrain);
    double[] trainingDataWithOutAnomalies = removeAnomalies(trainingTimestamps, trainingData, anomalousTimestamps);
    double[] monitoringData = Arrays.copyOfRange(observationsMinusSeasonality, numTrain,
        observationsMinusSeasonality.length);
    long[] monitoringTimestamps = Arrays.copyOfRange(timestamps, numTrain, observationsMinusSeasonality.length);

    // get anomalous interval if any
    LOGGER.info("detecting anomalies using scan statistics");
    long startTime = System.nanoTime();
    Range<Integer> anomalousInterval = scanStatistics.getInterval(trainingDataWithOutAnomalies, monitoringData);
    LOGGER.info("scan statistics took {} seconds", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

    // convert interval result to points
    List<AnomalyResult> anomalyResults = new ArrayList<AnomalyResult>();
    if (anomalousInterval != null) {
      for (int i = anomalousInterval.lowerEndpoint(); i < anomalousInterval.upperEndpoint(); i++) {
        ResultProperties properties = new ResultProperties();
        properties.setProperty("anomalyStart", new DateTime(
            monitoringTimestamps[anomalousInterval.lowerEndpoint()]).toString());
        properties.setProperty("anomalyEnd", new DateTime(
            monitoringTimestamps[anomalousInterval.upperEndpoint()]).toString());
        anomalyResults.add(new AnomalyResult(true, timestamps[i], pValueThreshold, observations[i], properties));
      }
    }
    return anomalyResults;
  }

  /**
   * @param timestamps
   * @param data
   * @param anomalousTimestamps
   * @return
   *  The data with anomalies removed. Timestamps will no longer match this array.
   */
  private double[] removeAnomalies(long[] timestamps, double[] data, Set<Long> anomalousTimestamps) {
    int collapsedIdx = 0;
    double[] dataWithAnomaliesRemoved = new double[timestamps.length];
    for (int i = 0; i < timestamps.length; i++) {
      if (anomalousTimestamps.contains(timestamps[i])) {
        continue;
      } else {
        dataWithAnomaliesRemoved[collapsedIdx] = data[i];
        collapsedIdx++;
      }
    }
    return Arrays.copyOf(dataWithAnomaliesRemoved, collapsedIdx);
  }

  /**
   * @param arr
   *  The array whose NaN values will be removed
   *
   * Note, the first data point cannot be a hole by construction from the conversion of MetricTimeSeries with
   * (min, max) times.
   */
  private void removeMissingValuesByAveragingNeighbors(double[] arr) {
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
