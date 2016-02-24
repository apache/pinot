package com.linkedin.thirdeye.function;

import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.util.LRUMap;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.lib.kalman.StateSpaceAnomalyDetector;
import com.linkedin.thirdeye.lib.kalman.StateSpaceDataPoint;
import com.linkedin.thirdeye.lib.util.MetricTimeSeriesUtils;

/**
 * Warning: this function is no longer actively maintained by ThirdEye.
 */
public class KalmanFilterAnomalyFunction extends BaseAnomalyFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(KalmanFilterAnomalyFunction.class);
  private static final Joiner CSV = Joiner.on(",");

  private static final String PROP_DEFAULT_SEASONAL = "0";
  private static final String PROP_DEFAULT_KNOB = "10000";
  private static final String PROP_DEFAULT_ORDER = "1";
  private static final String PROP_DEFAULT_P_VALUE_THRESHOLD = "0.05";

  public static final String SEASONAL = "seasonal";
  public static final String ORDER = "order";
  public static final String R = "r"; // TODO: make this more clearly named
  public static final String P_VALUE_THRESHOLD = "pValueThreshold";

  /**
   * Previous function state by kv. TODO : this is a hack...
   * Save the EstimatedStateNoise with keys identified by dimensionKey and the functionConfig.
   * - This doesn't improve worst case performance (cold-start)
   * - Seeding optimization with a previous EstimatedStateNoise speeds up convergence by 3-5x.
   * - This is kept in memory to avoid bad state from persisting between runs.
   * - If there are 10000 series being analyzed on a single machine every hour, then we probably
   * need more machines.
   */
  private static final LRUMap<String, Double> NON_DURABLE_STATE_KV_PAIRS = new LRUMap<>(100, 10000);

  @Override
  public List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<AnomalyResult> knownAnomalies)
          throws Exception {
    Properties props = getProperties();
    int seasonal = Integer.valueOf(props.getProperty(SEASONAL, PROP_DEFAULT_SEASONAL));
    int order = Integer.valueOf(props.getProperty(ORDER, PROP_DEFAULT_ORDER));
    int r = Integer.valueOf(props.getProperty(R, PROP_DEFAULT_KNOB));
    double pValueThreshold =
        Double.valueOf(props.getProperty(P_VALUE_THRESHOLD, PROP_DEFAULT_P_VALUE_THRESHOLD));

    long trainStartInput = Collections.min(timeSeries.getTimeWindowSet());
    long trainEndInput = Collections.max(timeSeries.getTimeWindowSet());
    long bucketMillis = getSpec().getBucketUnit().toMillis(getSpec().getBucketSize());
    String metric = getSpec().getMetric();

    // Compute the weight of this time series (average across whole)
    double averageValue = 0;
    for (Long time : timeSeries.getTimeWindowSet()) {
      averageValue += timeSeries.get(time, metric).doubleValue();
    }
    long numBuckets = (windowEnd.getMillis() - windowStart.getMillis()) / bucketMillis;
    averageValue /= numBuckets;

    // data to leave out from training
    Set<Long> omitTimestamps = new HashSet<Long>();

    // convert the data to arrays
    Pair<long[], double[]> arraysFromSeries =
        MetricTimeSeriesUtils.toArray(timeSeries, metric, bucketMillis, omitTimestamps, 0.0);
    long[] timestamps = arraysFromSeries.getFirst();
    double[] observations = arraysFromSeries.getSecond();

    // omit previously detected anomalies from training
    for (AnomalyResult ar : knownAnomalies) {
      omitTimestamps.add(ar.getStartTimeUtc());
    }

    StateSpaceAnomalyDetector stateSpaceDetector =
        new StateSpaceAnomalyDetector(trainStartInput, trainEndInput, -1, // stepsAhead
            bucketMillis, omitTimestamps, seasonal, order, order + seasonal - 1, // numStates
            1, // outputStates
            r);

    // cached state hack
    final String FUNCTION_INVOCATION_STATE_KEY = getKVMapKeyString(props, dimensionKey);
    Double initialEstimatedStateNoise =
        NON_DURABLE_STATE_KV_PAIRS.get(FUNCTION_INVOCATION_STATE_KEY);
    if (initialEstimatedStateNoise != null) {
      stateSpaceDetector.setInitialEstimatedStateNoise(initialEstimatedStateNoise);
    }

    Map<Long, StateSpaceDataPoint> resultsByTimeWindow;
    LOGGER.info("detecting anomalies using kalman filter");
    long startTime = System.nanoTime();
    resultsByTimeWindow =
        stateSpaceDetector.detectAnomalies(observations, timestamps, bucketMillis);
    LOGGER.info("algorithm took {} seconds",
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

    // cached state
    NON_DURABLE_STATE_KV_PAIRS.put(FUNCTION_INVOCATION_STATE_KEY,
        stateSpaceDetector.getEstimatedStateNoise());

    // translate FanomalyDataPoints to AnomalyResults (sort them to make the anomaly ids in the db
    // easier to look at)
    List<AnomalyResult> anomalyResults = new LinkedList<AnomalyResult>();
    for (long timeWindow : new TreeSet<>(resultsByTimeWindow.keySet())) {
      StateSpaceDataPoint stateSpaceDataPoint = resultsByTimeWindow.get(timeWindow);

      if (stateSpaceDataPoint.pValue < pValueThreshold) {
        Properties resultProperties = new Properties();
        resultProperties.put("actualValue", "" + stateSpaceDataPoint.actualValue);
        resultProperties.put("predictedValue", "" + stateSpaceDataPoint.predictedValue);
        resultProperties.put("stdError", "" + stateSpaceDataPoint.stdError);
        resultProperties.put("pValue", "" + stateSpaceDataPoint.pValue);
        resultProperties.put("predictedDate", "" + stateSpaceDataPoint.predictedDate);
        resultProperties.setProperty("timestamp",
            new DateTime(timeWindow, DateTimeZone.UTC).toString());

        AnomalyResult anomalyResult = new AnomalyResult();
        anomalyResult.setScore(stateSpaceDataPoint.pValue);
        anomalyResult.setProperties(AnomalyResult.encodeCompactedProperties(resultProperties));
        anomalyResult.setWeight(averageValue);
        anomalyResult.setStartTimeUtc(timeWindow);
        anomalyResult.setEndTimeUtc(null); // point-in-time
        anomalyResult.setCollection(getSpec().getCollection());
        anomalyResult.setDimensions(CSV.join(dimensionKey.getDimensionValues()));
        anomalyResult.setFunctionId(getSpec().getId());
        anomalyResult.setMetric(getSpec().getMetric());

        anomalyResults.add(anomalyResult);
      }
    }

    return anomalyResults;
  }

  /**
   * @return
   *         Key uniquely identifying this function configuration and the dimension key it is run
   *         on.
   */
  private static String getKVMapKeyString(Properties functionProperties, DimensionKey dimensionKey)
      throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    md.update(functionProperties.toString().getBytes());
    md.update(dimensionKey.toString().getBytes());
    byte[] digest = md.digest();
    StringBuffer sb = new StringBuffer();
    for (byte b : digest) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }
}
