package com.linkedin.thirdeye.detector.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

/**
 * See params for property configuration.
 * <p/>
 * baseline : baseline comparison period. Value should be one of 'w/w', 'w/2w', 'w/3w'. The anomaly
 * function spec should also be configured to provide a sufficient window size + additional time
 * buckets for comparison. For example, monitoring one hour of w/w data would require a window size
 * of 168 (1 week) + an additional 1 (for the monitoring window), for a total of 169 hours window
 * size.
 * <p/>
 * changeThreshold : detection threshold for percent change relative to baseline, defined as
 * (current - baseline) / baseline. Positive values detect an increase, while negative values detect
 * a decrease. This value should be a decimal, eg a 50% increase would have a threshold of 0.50. Any
 * triggered anomalies must strictly exceed this threshold (no equality).
 * <p/>
 * averageVolumeThreshold : minimum average threshold across the entire input window. If the average
 * value does not meet or exceed the threshold, no anomaly results will be generated. This value
 * should be a double.
 */
public class WeekOverWeekRuleFunction extends BaseAnomalyFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(WeekOverWeekRuleFunction.class);
  public static final String BASELINE = "baseline";
  public static final String CHANGE_THRESHOLD = "changeThreshold";
  public static final String AVERAGE_VOLUME_THRESHOLD = "averageVolumeThreshold";
  public static final String DEFAULT_MESSAGE_TEMPLATE = "change : %.2f %%, currentVal : %.2f, baseLineVal : %.2f, threshold : %s, baseLineProp : %s";

  public static String[] getPropertyKeys() {
    return new String [] {BASELINE, CHANGE_THRESHOLD, AVERAGE_VOLUME_THRESHOLD};
  }

  public List<Pair<Long, Long>> getDataRangeIntervals(Long monitoringWindowStartTime, Long monitoringWindowEndTime) {
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    startEndTimeIntervals.add(new Pair<>(monitoringWindowStartTime, monitoringWindowEndTime));

    try {
      Properties anomalyProps = getProperties();
      // Compute baseline for comparison
      String baselineProp = anomalyProps.getProperty(BASELINE);
      long baselineMillis = getBaselineOffsetInMillis(baselineProp);
      startEndTimeIntervals
          .add(new Pair<>(monitoringWindowStartTime - baselineMillis, monitoringWindowEndTime - baselineMillis));
    } catch (Exception e) {
      LOGGER.error("Error reading the properties", e);
    }
    return startEndTimeIntervals;
  }

  @Override
  public List<RawAnomalyResultDTO> analyze(DimensionMap exploredDimensions, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<RawAnomalyResultDTO> knownAnomalies)
      throws Exception {
    List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();

    // Parse function properties
    Properties props = getProperties();

    // Metric
    String metric = getSpec().getMetric();
    // Get thresholds
    double changeThreshold = Double.valueOf(props.getProperty(CHANGE_THRESHOLD));
    double volumeThreshold = 0;
    if (props.containsKey(AVERAGE_VOLUME_THRESHOLD)) {
      volumeThreshold = Double.valueOf(props.getProperty(AVERAGE_VOLUME_THRESHOLD));
    }

    // Compute baseline for comparison
    String baselineProp = props.getProperty(BASELINE);
    long baselineMillis = getBaselineOffsetInMillis(baselineProp);

    // Compute the bucket size, so we can iterate in those steps
    long bucketMillis =
        TimeUnit.MILLISECONDS.convert(getSpec().getBucketSize(), getSpec().getBucketUnit());

    // Compute the weight of this time series (average across whole)
    double averageValue = 0;
    for (Long time : timeSeries.getTimeWindowSet()) {
      averageValue += timeSeries.get(time, metric).doubleValue();
    }
    long numBuckets = (windowEnd.getMillis() - windowStart.getMillis()) / bucketMillis;
    averageValue /= numBuckets;

    // Check if this time series even meets our volume threshold
    if (averageValue < volumeThreshold) {
      LOGGER.info("{} does not meet volume threshold {}: {}", exploredDimensions, volumeThreshold, averageValue);
      return anomalyResults; // empty list
    }
    // iterate through baseline keys
    Set<Long> filteredBaselineTimes = filterTimeWindowSet(timeSeries.getTimeWindowSet(),
        windowStart.minus(baselineMillis).getMillis(), windowEnd.minus(baselineMillis).getMillis());
    List<Double> currentValues = new ArrayList<>();
    List<Double> baselineValues = new ArrayList<>();
    for (Long baselineKey : filteredBaselineTimes) {
      Long currentKey = baselineKey + baselineMillis;
      double currentValue = timeSeries.get(currentKey, metric).doubleValue();
      double baselineValue = timeSeries.get(baselineKey, metric).doubleValue();
      if (isAnomaly(currentValue, baselineValue, changeThreshold)) {
        RawAnomalyResultDTO anomalyResult = new RawAnomalyResultDTO();
        anomalyResult.setDimensions(exploredDimensions);
        anomalyResult.setProperties(getSpec().getProperties());
        anomalyResult.setStartTime(currentKey);
        anomalyResult.setEndTime(currentKey + bucketMillis); // point-in-time
        anomalyResult.setScore(averageValue);
        anomalyResult.setWeight(calculateChange(currentValue, baselineValue));
        String message = getAnomalyResultMessage(changeThreshold, baselineProp, currentValue, baselineValue);
        anomalyResult.setMessage(message);
        anomalyResults.add(anomalyResult);
        currentValues.add(currentValue);
        baselineValues.add(baselineValue);
        if (currentValue == 0.0 || baselineValue == 0.0) {
          anomalyResult.setDataMissing(true);
        }
      }
    }
    return anomalyResults;
  }

  private String getAnomalyResultMessage(double threshold, String baselineProp,
      double currentValue, double baselineValue) {
    double change = calculateChange(currentValue, baselineValue);
    NumberFormat percentInstance = NumberFormat.getPercentInstance();
    percentInstance.setMaximumFractionDigits(2);
    String thresholdPercent = percentInstance.format(threshold);
    String message = String
        .format(DEFAULT_MESSAGE_TEMPLATE, change * 100, currentValue, baselineValue,
            thresholdPercent, baselineProp);
    return message;
  }

  /**
   * Returns a new ordered set of all timestamps in the provided set that fit within
   * [windowStart, windowEnd)
   */
  SortedSet<Long> filterTimeWindowSet(Set<Long> timeWindowSet, long windowStart, long windowEnd) {
    TreeSet<Long> treeSet = new TreeSet<Long>(timeWindowSet);
    SortedSet<Long> subSet = treeSet.subSet(windowStart, windowEnd);
    return subSet;
  }

  long getBaselineOffsetInMillis(String baselineProp) throws IllegalArgumentException {
    // TODO: Expand options here and use regex to extract numeric parameter
    long baselineMillis;
    if ("w/w".equals(baselineProp)) {
      baselineMillis = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
    } else if ("w/2w".equals(baselineProp)) {
      baselineMillis = TimeUnit.MILLISECONDS.convert(14, TimeUnit.DAYS);
    } else if ("w/3w".equals(baselineProp)) {
      baselineMillis = TimeUnit.MILLISECONDS.convert(21, TimeUnit.DAYS);
    } else {
      throw new IllegalArgumentException("Unsupported baseline " + baselineProp);
    }
    return baselineMillis;
  }

  boolean isAnomaly(double currentValue, double baselineValue, double changeThreshold) {
    if (baselineValue > 0) {
      double percentChange = calculateChange(currentValue, baselineValue);
      if (changeThreshold > 0 && percentChange > changeThreshold || changeThreshold < 0
          && percentChange < changeThreshold) {
        return true;
      }
    }
    return false;
  }
}
