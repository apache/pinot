package com.linkedin.thirdeye.anomalydetection.datafilter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The data filter determines whether the average value of a time series passes the threshold.
 *
 * Advance Usage 1: A bucket of the time series is taken into consider only if its value is located inside the live
 * zone, which is specified by minLiveZone and maxLiveZone. In other words, if a bucket's value is smaller than
 * minLiveZone or is larger than maxLiveZone, then this bucket is ignored when calculating the average value.
 *
 * Advance Usage 2: The threshold could be overridden for different dimensions. For instance, the default threshold
 * for any combination of dimensions could be 1000. However, we could override the threshold for any sub-dimensions that
 * belong to this dimension {country=US}, e.g., {country=US, pageName=homePage} is a sub-dimension of {country=US}.
 */
public class AverageThresholdDataFilter extends BaseDataFilter {
  private static final Logger LOG = LoggerFactory.getLogger(AverageThresholdDataFilter.class);
  private static final Double NULL_DOUBLE = Double.NaN;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String METRIC_NAME_KEY = "metricName";
  public static final String THRESHOLD_KEY = "threshold";
  // If the value of a bucket is smaller than MIN_LIVE_ZONE_KEY, then that bucket is omitted
  public static final String MIN_LIVE_ZONE_KEY = "minLiveZone";
  // If the value of a bucket is larger than MAX_LIVE_ZONE_KEY, then that bucket is omitted
  public static final String MAX_LIVE_ZONE_KEY = "maxLiveZone";
  // Threshold to the percentage of live buckets among all buckets
  public static final String LIVE_BUCKETS_PERCENTAGE_KEY = "liveBucketsPctThreshold";
  // Override threshold to different dimension map
  public static final String OVERRIDE_THRESHOLD_KEY = "overrideThreshold";

  private static final double DEFAULT_THRESHOLD = Double.NEGATIVE_INFINITY;
  private static final double DEFAULT_MIN_LIVE_ZONE = Double.NEGATIVE_INFINITY;
  private static final double DEFAULT_MAX_LIVE_ZONE = Double.POSITIVE_INFINITY;
  private static final double DEFAULT_LIVE_BUCKETS_PERCENTAGE = 0.5d;

  private String metricName;
  private double threshold;
  private double minLiveZone;
  private double maxLiveZone;
  private double liveBucketPercentageThreshold;

  // The override threshold for different dimension maps, which could form a hierarchy.
  private NavigableMap<DimensionMap, Double> overrideThreshold = new TreeMap<>();

  // For testing purpose
  NavigableMap<DimensionMap, Double> getOverrideThreshold() {
    return overrideThreshold;
  }

  @Override
  public void setParameters(Map<String, String> props) {
    super.setParameters(props);

    // Initialize threshold from users' setting
    threshold = DEFAULT_THRESHOLD;
    if (props.containsKey(THRESHOLD_KEY)) {
      threshold = Double.parseDouble(props.get(THRESHOLD_KEY));
    }
    if (Double.isNaN(threshold)) {
      throw new IllegalStateException("Threshold cannot be NaN.");
    }

    // Initialize metricName from users' setting
    metricName = props.get(METRIC_NAME_KEY);
    if (StringUtils.isBlank(metricName)) {
      throw new IllegalArgumentException("metric name for average threshold data filter cannot be a blank String.");
    }

    // Initialize minLiveZone from users' setting
    minLiveZone = parseDoubleFromUserInput(MIN_LIVE_ZONE_KEY, DEFAULT_MIN_LIVE_ZONE);
    // Initialize maxLiveZone from users' setting
    maxLiveZone = parseDoubleFromUserInput(MAX_LIVE_ZONE_KEY, DEFAULT_MAX_LIVE_ZONE);
    // Initialize liveBucketPercentageThreshold from users' setting
    liveBucketPercentageThreshold = parseDoubleFromUserInput(LIVE_BUCKETS_PERCENTAGE_KEY, DEFAULT_LIVE_BUCKETS_PERCENTAGE);

    // Initialize the lookup table for overriding thresholds
    if (props.containsKey(OVERRIDE_THRESHOLD_KEY)) {
      String overrideJsonPayLoad = props.get(OVERRIDE_THRESHOLD_KEY);
      try {
        Map<String, Double> rawOverrideThresholdMap = OBJECT_MAPPER.readValue(overrideJsonPayLoad, HashMap.class);
        for (Map.Entry<String, Double> overrideThresholdEntry : rawOverrideThresholdMap.entrySet()) {
          DimensionMap dimensionMap = new DimensionMap(overrideThresholdEntry.getKey());
          Double threshold = overrideThresholdEntry.getValue();
          overrideThreshold.put(dimensionMap, threshold);
        }
      } catch (IOException e) {
        LOG.error("Failed to reconstruct override threshold mappings from this json string: {}", overrideJsonPayLoad);
      }
    }
  }

  @Override
  public boolean isQualified(MetricTimeSeries metricTimeSeries, DimensionMap dimensionMap) {
    return isQualified(metricTimeSeries, dimensionMap, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public boolean isQualified(MetricTimeSeries metricTimeSeries, DimensionMap dimensionMap, long windowStart,
      long windowEnd) {
    double threshold = this.threshold;
    // Read the override threshold for the dimension of this time series
    if (MapUtils.isNotEmpty(overrideThreshold)) {
      threshold = overrideThresholdForDimensions(dimensionMap, threshold);
    }

    if (threshold == Double.NEGATIVE_INFINITY) {
      return true;
    } else if (threshold == Double.POSITIVE_INFINITY) {
      return false;
    }

    // Compute average values among all buckets and check if it passes the threshold
    double sum = 0d;
    int count = 0;
    int totalCount = 0;
    for (long timestamp : metricTimeSeries.getTimeWindowSet()) {
      if (timestamp < windowStart || timestamp >= windowEnd) {
        continue;
      }
      double value = metricTimeSeries.getOrDefault(timestamp, metricName, NULL_DOUBLE).doubleValue();
      ++totalCount;
      if (Double.compare(NULL_DOUBLE, value) == 0) {
        continue;
      }
      if (isLiveBucket(value, minLiveZone, maxLiveZone)) {
        sum += value;
        ++count;
      }
    }

    if (count > 0) {
      double liveBucketPercentage = (double) count / (double) totalCount;
      if (Double.compare(liveBucketPercentage, liveBucketPercentageThreshold) >= 0) {
        double average = sum / count;
        return Double.compare(average, threshold) >= 0;
      }
    }

    return false;
  }

  /**
   * Returns the parsed double value from users' given value that is stored in the properties.
   *
   * @param propKey the key to retrieve users' value from the properties.
   * @param defaultValue the default value if users' value is unreadable (e.g., NaN, NumberFormatException)
   *
   * @return the parsed double value that is stored in the properties.
   */
  private double parseDoubleFromUserInput(String propKey, double defaultValue) {
    double value = defaultValue;

    if (props.containsKey(propKey)) {
      try {
        value = Double.parseDouble(props.get(propKey));
        if (Double.isNaN(value)) {
          LOG.warn("This value {} for the property key {} is unreadable; default value {} is used.", value, propKey,
              defaultValue);
          value = defaultValue;
        }
      } catch (NumberFormatException e) {
        LOG.warn("Failed to parse this value {} for the property key {}; default value {} is used.", props.get(propKey),
            propKey, defaultValue);
        value = defaultValue;
      }
    }

    return value;
  }

  /**
   * The value of a bucket is considered during the calculation of average bucket value only if the value is not a NaN
   * and it is not located in the live zone, which is defined by minLiveZone and maxLiveZone.
   *
   * @param value the value to be tested
   * @param minLiveZone if the given value is smaller than minLiveZone, then the value is located in the live zone.
   * @param maxLiveZone if the given value is larger than maxLiveZone, then the value is located in the live zone.
   * @return true is the value should be considered when calculating the average of bucket values.
   */
  private boolean isLiveBucket(double value, double minLiveZone, double maxLiveZone) {
    if (Double.isNaN(value)) {
      return false;
    } else if (Double.compare(minLiveZone, value) > 0) {
      return false;
    } else if (Double.compare(maxLiveZone, value) < 0) {
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
      if (dimensionMap.equalsOrChildOf(overrideDimensionMap)) {
        return overrideThreshold.get(overrideDimensionMap);
      }
    }
    return defaultThreshold;
  }
}
