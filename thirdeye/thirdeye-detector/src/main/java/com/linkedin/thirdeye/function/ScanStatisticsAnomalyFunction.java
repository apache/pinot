package com.linkedin.thirdeye.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.api.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.lib.scanstatistics.ScanStatistics;
import com.linkedin.thirdeye.lib.util.MetricTimeSeriesUtils;
import com.linkedin.thirdeye.lib.util.STLDecomposition;

/**
 * Warning: this function is no longer actively maintained by ThirdEye.
 */
public class ScanStatisticsAnomalyFunction extends BaseAnomalyFunction {
  private static Logger LOGGER = LoggerFactory.getLogger(ScanStatisticsAnomalyFunction.class);
  private static final Joiner CSV = Joiner.on(",");

  private static final String PROP_DEFAULT_SEASONAL = "168";
  private static final String PROP_DEFAULT_P_VALUE_THRESHOLD = "0.05";
  private static final String PROP_DEFAULT_MIN_WINDOW_LEN = "1";
  private static final String PROP_DEFAULT_NUM_SIMULATIONS = "1000";
  private static final String PROP_DEFAULT_MIN_INCREMENT = "1";
  private static final String PROP_DEFAULT_MAX_WINDOW_LEN = "" + TimeUnit.DAYS.toHours(7);
  private static final String PROP_DEFAULT_BOOTSTRAP = "false";
  private static final String PROP_DEFAULT_STL_TREND_BANDWIDTH = "0.5";
  private static final String PROP_DEFAULT_MONITORING_WINDOW_SIZE = "168"; // 1 week in hours
  private static final String PROP_DEFAULT_ONLY_ALERT_BOUNDARIES = "false";
  private static final String PROP_DEFAULT_DOUBLE_NOT_EQUAL_EPSILON = "0.1";

  public static final String SEASONAL = "seasonal";
  public static final String MAX_WINDOW_LENGTH = "maxWindowLength";
  public static final String NUM_SIMULATIONS = "numSimulations";
  public static final String MIN_WINDOW_LENGTH = "minWindowLength";
  public static final String P_VALUE_THRESHOLD = "pValueThreshold";
  public static final String PATTERN = "pattern";
  public static final String MIN_INCREMENT = "minIncrement";
  public static final String BOOTSTRAP = "bootstrap";
  public static final String STL_TREND_BANDWIDTH = "stlTrendBandwidth";
  public static final String MONITORING_WINDOW = "monitoringWindow";
  public static final String NOT_EQUAL_EPSILON = "notEqualEpsilon";

  private int seasonal;
  private int maxWindowLength;
  private int numSimulations;
  private int minWindowLength;
  private double pValueThreshold;
  private ScanStatistics.Pattern pattern;
  private int minIncrement;
  private boolean bootstrap;
  private double stlTrendBandwidth;
  private int monitoringWindow;
  private double notEqualEpsilon;

  @Override
  public void init(AnomalyFunctionSpec spec) throws Exception {
    super.init(spec);
    Properties props = getProperties();
    this.seasonal = Integer.valueOf(props.getProperty(SEASONAL, PROP_DEFAULT_SEASONAL));
    this.maxWindowLength =
        Integer.valueOf(props.getProperty(MAX_WINDOW_LENGTH, PROP_DEFAULT_MAX_WINDOW_LEN));
    this.numSimulations =
        Integer.valueOf(props.getProperty(NUM_SIMULATIONS, PROP_DEFAULT_NUM_SIMULATIONS));
    this.minWindowLength =
        Integer.valueOf(props.getProperty(MIN_WINDOW_LENGTH, PROP_DEFAULT_MIN_WINDOW_LEN));
    this.pValueThreshold =
        Double.valueOf(props.getProperty(P_VALUE_THRESHOLD, PROP_DEFAULT_P_VALUE_THRESHOLD));
    this.pattern = ScanStatistics.Pattern.valueOf(props.getProperty(PATTERN));
    this.minIncrement =
        Integer.valueOf(props.getProperty(MIN_INCREMENT, PROP_DEFAULT_MIN_INCREMENT));
    this.bootstrap = Boolean.valueOf(props.getProperty(BOOTSTRAP, PROP_DEFAULT_BOOTSTRAP));
    this.stlTrendBandwidth =
        Double.valueOf(props.getProperty(STL_TREND_BANDWIDTH, PROP_DEFAULT_STL_TREND_BANDWIDTH));
    this.monitoringWindow =
        Integer.valueOf(props.getProperty(MONITORING_WINDOW, PROP_DEFAULT_MONITORING_WINDOW_SIZE));
    this.notEqualEpsilon =
        Double.valueOf(props.getProperty(NOT_EQUAL_EPSILON, PROP_DEFAULT_DOUBLE_NOT_EQUAL_EPSILON));
  }

  @Override
  public List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<AnomalyResult> knownAnomalies)
          throws Exception {
    long bucketMillis = getSpec().getBucketUnit().toMillis(getSpec().getBucketSize());
    String metric = getSpec().getMetric();

    // Compute the weight of this time series (average across whole)
    double averageValue = 0;
    for (Long time : timeSeries.getTimeWindowSet()) {
      averageValue += timeSeries.get(time, metric).doubleValue();
    }
    long numBuckets = (windowEnd.getMillis() - windowStart.getMillis()) / bucketMillis;
    averageValue /= numBuckets;

    // convert the data to arrays
    Pair<long[], double[]> arraysFromSeries =
        MetricTimeSeriesUtils.toArray(timeSeries, metric, bucketMillis, null, Double.NaN);
    long[] timestamps = arraysFromSeries.getFirst();
    double[] observations = arraysFromSeries.getSecond();
    removeMissingValuesByAveragingNeighbors(observations);

    // call stl library
    double[] observationsMinusSeasonality = removeSeasonality(timestamps, observations, seasonal);

    int effectiveMaxWindowLength =
        (int) ((windowEnd.getMillis() - windowStart.getMillis()) / bucketMillis);
    effectiveMaxWindowLength = Math.min(effectiveMaxWindowLength, maxWindowLength);

    // instantiate model
    ScanStatistics scanStatistics =
        new ScanStatistics(numSimulations, minWindowLength, effectiveMaxWindowLength,
            pValueThreshold, pattern, minIncrement, bootstrap, notEqualEpsilon);
    LOGGER.info("Created {}", scanStatistics);

    int totalNumBuckets = observationsMinusSeasonality.length;
    int numTrain = totalNumBuckets - monitoringWindow;

    // set of timestamps with anomalies
    Set<Long> anomalousTimestamps = new HashSet<Long>();
    for (AnomalyResult ar : knownAnomalies) {
      anomalousTimestamps.add(ar.getStartTimeUtc()); // TODO: Shouldn't this be an interval?
    }

    // partition the data into train and monitoring
    double[] trainingData = Arrays.copyOfRange(observationsMinusSeasonality, 0, numTrain);
    long[] trainingTimestamps = Arrays.copyOfRange(timestamps, 0, numTrain);

    double[] trainingDataWithOutAnomalies =
        removeAnomalies(trainingTimestamps, trainingData, anomalousTimestamps);

    double[] monitoringData = Arrays.copyOfRange(observationsMinusSeasonality, numTrain,
        observationsMinusSeasonality.length);
    double[] monitoringDataOrig = Arrays.copyOfRange(observations, numTrain, observations.length);
    long[] monitoringTimestamps =
        Arrays.copyOfRange(timestamps, numTrain, observationsMinusSeasonality.length);

    // get anomalous interval if any
    LOGGER.info("detecting anomalies using scan statistics");
    long startTime = System.nanoTime();
    Range<Integer> anomalousInterval =
        scanStatistics.getInterval(trainingDataWithOutAnomalies, monitoringData);
    LOGGER.info("scan statistics took {} seconds",
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

    if (anomalousInterval != null) {
      LOGGER.info("found interval : {}", anomalousInterval);
    }

    // convert interval result to points
    List<AnomalyResult> anomalyResults = new ArrayList<AnomalyResult>();
    if (anomalousInterval != null) {
      anomalyResults = getAnomalyResultsForBoundsOnly(dimensionKey, monitoringDataOrig,
          monitoringTimestamps, anomalousInterval, averageValue);
    }
    return anomalyResults;
  }

  /**
   * @return
   *         Only anomaly results for the start and end of the interval.
   */
  private List<AnomalyResult> getAnomalyResultsForBoundsOnly(DimensionKey dimensionKey,
      double[] monitoringData, long[] monitoringTimestamps, Range<Integer> anomalousInterval,
      double averageValue) {
    long startTimeUtc = monitoringTimestamps[anomalousInterval.lowerEndpoint()];
    long endTimeUtc = monitoringTimestamps[anomalousInterval.upperEndpoint() - 1 /* inclusive */];

    List<AnomalyResult> anomalyResults = new ArrayList<AnomalyResult>();
    Properties startProperties = new Properties();
    startProperties.setProperty("anomalyStart",
        new DateTime(startTimeUtc, DateTimeZone.UTC).toString());
    startProperties.setProperty("anomalyEnd",
        new DateTime(endTimeUtc, DateTimeZone.UTC).toString());
    startProperties.setProperty("bound", "START");

    AnomalyResult startResult = new AnomalyResult();
    startResult.setScore(pValueThreshold); // TODO: is this right?
    startResult.setProperties(AnomalyResult.encodeCompactedProperties(startProperties));
    startResult.setWeight(averageValue);
    startResult.setStartTimeUtc(startTimeUtc);
    startResult.setEndTimeUtc(endTimeUtc);
    startResult.setCollection(getSpec().getCollection());
    startResult.setDimensions(CSV.join(dimensionKey.getDimensionValues()));
    startResult.setFunctionId(getSpec().getId());
    startResult.setMetric(getSpec().getMetric());
    anomalyResults.add(startResult);

    return anomalyResults;
  }

  /**
   * @return
   *         The data with anomalies removed. Timestamps will no longer match this array.
   */
  private double[] removeAnomalies(long[] timestamps, double[] data,
      Set<Long> anomalousTimestamps) {
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
   *          The array whose NaN values will be removed
   *          Note, the first data point cannot be a hole by construction from the conversion of
   *          MetricTimeSeries with
   *          (min, max) times.
   *          (TODO): linear interpolation for missing data later.
   */
  public static void removeMissingValuesByAveragingNeighbors(double[] arr) {
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

  private double[] removeSeasonality(long[] timestamps, double[] series, int seasonality) {
    STLDecomposition.Config config = new STLDecomposition.Config();
    config.setNumberOfObservations(seasonality);
    /*
     * InnerLoopPasses set to 1 and RobustnessIterations set to 15 matches the stl using robust
     * option in R implementation
     * For reference: https://stat.ethz.ch/R-manual/R-devel/library/stats/html/stl.html
     */
    config.setNumberOfInnerLoopPasses(1);
    config.setNumberOfRobustnessIterations(15);

    /*
     * There isn't a particularly good reason to use these exact values other than that the results
     * closely match the
     * stl R library results. It would appear than setting these anywhere between [0.5, 1.0}
     * produces similar results.
     */
    config.setLowPassFilterBandwidth(0.5);
    config.setTrendComponentBandwidth(stlTrendBandwidth); // default is 0.5

    config.setPeriodic(true);
    config.setNumberOfDataPoints(series.length);
    STLDecomposition stl = new STLDecomposition(config);

    STLDecomposition.STLResult res = stl.decompose(timestamps, series);

    double[] trend = res.getTrend();
    double[] remainder = res.getRemainder();
    double[] seasonalityRemoved = new double[trend.length];
    for (int i = 0; i < trend.length; i++) {
      seasonalityRemoved[i] = trend[i] + remainder[i];
    }
    return seasonalityRemoved;
  }
}
