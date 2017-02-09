package com.linkedin.thirdeye.detector.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * See params for property configuration.
 * <p/>
 * min - lower threshold limit for average (inclusive). Will trigger alert if datapoint < min
 * (strictly less than)
 * <p/>
 * max - upper threshold limit for average (inclusive). Will trigger alert if datapoint > max
 * (strictly greater than)
 */
public class RatioOutlierFunction extends BaseAnomalyFunction {
  public static final String DEFAULT_MESSAGE_TEMPLATE = "change : %.2f %%, currentVal : %.2f, min : %.2f, max : %.2f";
  public static final String MIN_VAL = "min";
  public static final String MAX_VAL = "max";

  final static Logger LOG = LoggerFactory.getLogger(RatioOutlierFunction.class);

  public static String[] getPropertyKeys() {
    return new String [] {MIN_VAL, MAX_VAL};
  }

  @Override
  public List<Pair<Long, Long>> getDataRangeIntervals(Long monitoringWindowStartTime,
      Long monitoringWindowEndTime) {
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    startEndTimeIntervals.add(new Pair<>(monitoringWindowStartTime, monitoringWindowEndTime));
    return startEndTimeIntervals;
  }

  @Override
  public List<RawAnomalyResultDTO> analyze(DimensionMap exploredDimensions,
      MetricTimeSeries timeSeries, DateTime windowStart, DateTime windowEnd,
      List<MergedAnomalyResultDTO> knownAnomalies) throws Exception {
    List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();
    // Parse function properties
    Properties props = getProperties();

    // Get min / max props
    Double min = null;
    if (props.containsKey(MIN_VAL)) {
      min = Double.valueOf(props.getProperty(MIN_VAL));
    }

    Double max = null;
    if (props.containsKey(MAX_VAL)) {
      max = Double.valueOf(props.getProperty(MAX_VAL));
    }

    // Metric
    String topicMetric = getSpec().getTopicMetric();

    // This function only detects anomalies on one metric, i.e., metrics[0]
    assert (getSpec().getMetrics().size() == 2);

    LOG.info("Testing ratios {} for outliers", String.join(", ", getSpec().getMetrics()));

    // Compute the bucket size, so we can iterate in those steps
    long bucketMillis = TimeUnit.MILLISECONDS.convert(getSpec().getBucketSize(), getSpec().getBucketUnit());

    long numBuckets = (windowEnd.getMillis() - windowStart.getMillis()) / bucketMillis;

    Map<String, Double> averages = new HashMap<String, Double>();
    for (String m : getSpec().getMetrics()) {
      // Compute the weight of this time series (average across whole)
      double averageValue = 0;
      for (Long time : timeSeries.getTimeWindowSet()) {
        averageValue += timeSeries.get(time, m).doubleValue();
      }

      // avg value of this time series
      averageValue /= numBuckets;

      averages.put(m, averageValue);
    }

    // TODO generate pairs
    Collection<MetricPair> pairs = new ArrayList<MetricPair>();
    pairs.add(new MetricPair(getSpec().getMetrics().get(0), getSpec().getMetrics().get(1)));

    for (Long timeBucket : timeSeries.getTimeWindowSet()) {
      boolean isAnomaly = false;
      double worst_case_ratio = 0.0;
      double worst_case_deviation = 0.0;

      for (MetricPair p : pairs) {
        double value_a = timeSeries.get(timeBucket, p.a).doubleValue();
        double value_b = timeSeries.get(timeBucket, p.b).doubleValue();

        if (value_b == 0.0d) continue;

        double ratio = value_a / value_b;
        double deviationFromThreshold = getDeviationFromThreshold(ratio, min, max);

        LOG.info("{}={}, {}={}, ratio={}, min={}, max={}, deviation={}", p.a, value_a, p.b, value_b, ratio, min, max, deviationFromThreshold);

        if (Math.abs(deviationFromThreshold) > Math.abs(worst_case_deviation)) {
          worst_case_ratio = ratio;
          worst_case_deviation = deviationFromThreshold;
        }

        if (deviationFromThreshold != 0.0) {
          isAnomaly = true;
          // keep going to find worst case
        }
      }

      if (isAnomaly) {
        RawAnomalyResultDTO anomalyResult = new RawAnomalyResultDTO();
        anomalyResult.setProperties(getSpec().getProperties());
        anomalyResult.setStartTime(timeBucket);
        anomalyResult.setEndTime(timeBucket + bucketMillis); // point-in-time
        anomalyResult.setDimensions(exploredDimensions);
        anomalyResult.setScore(worst_case_ratio);
        anomalyResult.setWeight(Math.abs(worst_case_deviation)); // higher change, higher the severity
        String message = String.format(DEFAULT_MESSAGE_TEMPLATE, worst_case_deviation, worst_case_ratio, min, max);
        anomalyResult.setMessage(message);
        anomalyResults.add(anomalyResult);
      }
    }
    return anomalyResults;
  }

  @Override
  public void updateMergedAnomalyInfo(MergedAnomalyResultDTO anomalyToUpdated, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies)
      throws Exception {

    // TODO: implement

  }

  private double getDeviationFromThreshold(double currentValue, Double min, Double max) {
    if ((min != null && currentValue < min && min != 0d)) {
      return calculateChange(currentValue, min);
    } else if (max != null && currentValue > max && max != 0d) {
      return calculateChange(currentValue, max);
    }
    return 0;
  }

  static class MetricPair {
    final String a;
    final String b;

    public MetricPair(String a, String b) {
      this.a = a;
      this.b = b;
    }
  }
}
