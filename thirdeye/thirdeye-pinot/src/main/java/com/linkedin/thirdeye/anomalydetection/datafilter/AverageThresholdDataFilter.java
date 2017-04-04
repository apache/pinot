package com.linkedin.thirdeye.anomalydetection.datafilter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.api.DimensionMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;

public class AverageThresholdDataFilter extends BaseDataFilter {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String METRIC_NAME_KEY = "metricName";
  private static final String THRESHOLD_KEY = "threshold";
  // If the value of a bucket is smaller than MIN_DEAD_ZONE_KEY, then that bucket is omitted
  private static final String MIN_DEAD_ZONE_KEY = "minDeadZone";
  // If the value of a bucket is larger than MIN_DEAD_ZONE_KEY, then that bucket is omitted
  private static final String MAX_DEAD_ZONE_KEY = "maxDeadZone";

  private static final double DEFAULT_THRESHOLD = Double.NEGATIVE_INFINITY;
  private static final double DEFAULT_MIN_DEAD_ZONE = Double.NEGATIVE_INFINITY;
  private static final double DEFAULT_MAX_DEAD_ZONE = Double.POSITIVE_INFINITY;

  // Override threshold to different dimension map
  private static final String OVERRIDE_THRESHOLD_KEY = "overrideThreshold";
  // The override threshold for different dimension maps, which could form a hierarchy.
  private NavigableMap<DimensionMap, Double> overrideThreshold = new TreeMap<>();


  @Override
  public void setParameters(Map<String, String> props) {
    super.setParameters(props);
    if (props.containsKey(OVERRIDE_THRESHOLD_KEY)) {
      String overrideJsonPayLoad = props.get(OVERRIDE_THRESHOLD_KEY);
      try {
        overrideThreshold = OBJECT_MAPPER.readValue(overrideJsonPayLoad, NavigableMap.class);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Returns if the average value of a time series, which is specified by metricName and dimensionMap, passes the
   * threshold.
   *
   * Advanced Usage: A bucket is taken into consider only if its value is located inside live zone, which is specified
   * by minDeadZone and maxDeadZone. In other words, if a value is smaller than minDeadZone or is larger than
   * maxDeadZone, then this value is ignored when calculating the average value.
   *
   * @param context the context for retrieving the time series.
   * @param dimensionMap the dimension map of the time series.
   * @return if the average value of the given time series passes the threshold.
   */
  @Override
  public boolean isQualified(AnomalyDetectionContext context, DimensionMap dimensionMap) {
    // Initialize threshold from users' setting
    double threshold = DEFAULT_THRESHOLD;
    if (props.containsKey(THRESHOLD_KEY)) {
      threshold = Double.parseDouble(props.get(THRESHOLD_KEY));
    }
    if (Double.isNaN(threshold)) {
      throw new IllegalStateException("Threshold cannot be NaN.");
    }
    // Read the override threshold for the dimension of this time series
    threshold = overrideThresholdForDimensions(dimensionMap, threshold);
    if (threshold == Double.NEGATIVE_INFINITY) {
      return true;
    } else if (threshold == Double.POSITIVE_INFINITY) {
      return false;
    }

    // Initialize metricName from users' setting
    String metricName = props.get(METRIC_NAME_KEY);
    if (StringUtils.isBlank(metricName)) {
      throw new IllegalArgumentException("metric name cannot be a blank String.");
    }

    // Initialize minDeadZone from users' setting
    double minDeadZone = DEFAULT_MIN_DEAD_ZONE;
    if (props.containsKey(MIN_DEAD_ZONE_KEY)) {
      minDeadZone = Double.parseDouble(props.get(MIN_DEAD_ZONE_KEY));
      if (Double.isNaN(minDeadZone)) {
        minDeadZone = Double.NEGATIVE_INFINITY;
      }
    }
    // Initialize maxDeadZone from users' setting
    double maxDeadZone = DEFAULT_MAX_DEAD_ZONE;
    if (props.containsKey(MAX_DEAD_ZONE_KEY)) {
      maxDeadZone = Double.parseDouble(props.get(MAX_DEAD_ZONE_KEY));
      if (Double.isNaN(maxDeadZone)) {
        maxDeadZone = Double.POSITIVE_INFINITY;
      }
    }

    // Compute average values among all buckets and check if it passes the threshold
    List<TimeSeries> timeSeries = context.getBaselines(metricName);
    double sum = 0d;
    int count = 0;
    for (TimeSeries series : timeSeries) {
      for (long timestamp : series.timestampSet()) {
        double value = series.get(timestamp);
        if (isLiveBucket(value, minDeadZone, maxDeadZone)) {
          sum += value;
          ++count;
        }
      }
    }
    return count > 0 && (sum / count) > threshold;
  }

  /**
   * The value of a bucket is considered during the calculation of average bucket value only if the value is not a NaN
   * and it is not located in the dead zone, which is defined by minDeadZone and maxDeadZone.
   *
   * @param value the value to be tested
   * @param minDeadZone if the given value is smaller than minDeadZone, then the value is located in the dead zone.
   * @param maxDeadZone if the given value is larger than maxDeadZone, then the value is located in the dead zone.
   * @return true is the value should be considered when calculating the average of bucket values.
   */
  private boolean isLiveBucket(double value, double minDeadZone, double maxDeadZone) {
    if (!Double.isNaN(value)) {
      return false;
    } else if (Double.compare(minDeadZone, value) > 0) {
      return false;
    } else if (Double.compare(maxDeadZone, value) < 0) {
      return false;
    } else return true;
  }

  /**
   * Find the override threshold based on the given dimension map. The override threshold could given in a hierarchical
   * dimension structure. Assume that the dimension map contains two dimensions: country and pageName. We could override
   * the threshold in country level by specifying: overrideDimensionMap {country=US}, overrideThreshold=100. In this
   * case, any dimensions that contain {country=US}, e.g., {country=US, pageName=homePage}, would use the override
   * threshold.
   *
   * @param dimensionMap the dimension map to be used to search the override threshold.
   * @param defaultThreshold the default threshold if override threshold does not exist.
   *
   * @return the threshold for the given dimension map.
   */
  private double overrideThresholdForDimensions(DimensionMap dimensionMap, double defaultThreshold) {
    for (DimensionMap overrideDimensionMap : overrideThreshold.descendingKeySet()) {
      if (dimensionMap.isChild(overrideDimensionMap)) {
        return overrideThreshold.get(overrideDimensionMap);
      }
    }
    return defaultThreshold;
  }
}
