package com.linkedin.thirdeye.detector.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

/**
 * See params for property configuration.
 * <p/>
 * min - lower threshold limit for average (inclusive). Will trigger alert if datapoint < min
 * (strictly less than)
 * <p/>
 * max - upper threshold limit for average (inclusive). Will trigger alert if datapoint > max
 * (strictly greater than)
 */
public class MinMaxThresholdFunction extends BaseAnomalyFunction {
  public static final String DEFAULT_MESSAGE_TEMPLATE = "change : %.2f %%, currentVal : %.2f, min : %.2f, max : %.2f";
  public static final String MIN_VAL = "min";
  public static final String MAX_VAL = "max";

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

    // Metric
    String metric = getSpec().getTopicMetric();
    // This function only detects anomalies on one metric, i.e., metrics[0]
    assert (getSpec().getMetrics().size() == 1);

    // Get min / max props
    Double min = null;
    if (props.containsKey(MIN_VAL)) {
      min = Double.valueOf(props.getProperty(MIN_VAL));
    }

    Double max = null;
    if (props.containsKey(MAX_VAL)) {
      max = Double.valueOf(props.getProperty(MAX_VAL));
    }

    // Compute the weight of this time series (average across whole)
    double averageValue = 0;
    for (Long time : timeSeries.getTimeWindowSet()) {
      averageValue += timeSeries.get(time, metric).doubleValue();
    }
    // Compute the bucket size, so we can iterate in those steps
    long bucketMillis =
        TimeUnit.MILLISECONDS.convert(getSpec().getBucketSize(), getSpec().getBucketUnit());

    long numBuckets = (windowEnd.getMillis() - windowStart.getMillis()) / bucketMillis;

    // avg value of this time series
    averageValue /= numBuckets;

    for (Long timeBucket : timeSeries.getTimeWindowSet()) {
      Double value = timeSeries.get(timeBucket, metric).doubleValue();
      double deviationFromThreshold = getDeviationFromThreshold(value, min, max);

      if (deviationFromThreshold != 0) {
        RawAnomalyResultDTO anomalyResult = new RawAnomalyResultDTO();
        anomalyResult.setProperties(getSpec().getProperties());
        anomalyResult.setStartTime(timeBucket);
        anomalyResult.setEndTime(timeBucket + bucketMillis); // point-in-time
        anomalyResult.setDimensions(exploredDimensions);
        anomalyResult.setScore(averageValue);
        anomalyResult.setWeight(Math.abs(deviationFromThreshold)); // higher change, higher the severity
        String message =
            String.format(DEFAULT_MESSAGE_TEMPLATE, deviationFromThreshold, value, min, max);
        anomalyResult.setMessage(message);
        if (value == 0.0) {
          anomalyResult.setDataMissing(true);
        }
        anomalyResults.add(anomalyResult);
      }
    }
    return anomalyResults;
  }

  @Override
  public void updateMergedAnomalyInfo(MergedAnomalyResultDTO anomalyToUpdated, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies)
      throws Exception {

    // Get min / max props
    Properties props = getProperties();
    Double min = null;
    if (props.containsKey(MIN_VAL)) {
      min = Double.valueOf(props.getProperty(MIN_VAL));
    }

    Double max = null;
    if (props.containsKey(MAX_VAL)) {
      max = Double.valueOf(props.getProperty(MAX_VAL));
    }

    String metric = getSpec().getTopicMetric();
    assert (getSpec().getMetrics().size() == 1);

    long windowStartInMillis = windowStart.getMillis();
    long windowEndInMillis = windowEnd.getMillis();

    double currentAverageValue = 0d;
    int currentBucketCount = 0;
    double deviationFromThreshold = 0d;
    for (long time : timeSeries.getTimeWindowSet()) {
      double value = timeSeries.get(time, metric).doubleValue();
      if (value != 0d) {
        if (windowStartInMillis <= time && time <= windowEndInMillis) {
          currentAverageValue += value;
          ++currentBucketCount;
          deviationFromThreshold += getDeviationFromThreshold(value, min, max);
        } // else ignore unknown time key
      }
    }

    if (currentBucketCount != 0d) {
      currentAverageValue /= currentBucketCount;
      deviationFromThreshold /= currentBucketCount;
    }
    anomalyToUpdated.setScore(currentAverageValue);
    anomalyToUpdated.setWeight(deviationFromThreshold);

    String message =
        String.format(DEFAULT_MESSAGE_TEMPLATE, deviationFromThreshold, currentAverageValue, min, max);
    anomalyToUpdated.setMessage(message);
  }

  private double getDeviationFromThreshold(double currentValue, Double min, Double max) {
    if ((min != null && currentValue < min && min != 0d)) {
      return calculateChange(currentValue, min);
    } else if (max != null && currentValue > max && max != 0d) {
      return calculateChange(currentValue, max);
    }
    return 0;
  }
}
