package com.linkedin.thirdeye.realtime;

import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.impl.NumberUtils;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * TODO: The logic to parse / extract derived dimension values should probably be somewhere else,
 * but we can put it in here for now until we refactor (maybe into a thirdeye-avro module that
 * can be used by both thirdeye-realtime and thirdeye-bootstrap).
 */
public class ThirdEyeAvroUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeAvroUtils.class);
  private static final String NAME_SEPARATOR = "\\.";
  private static final String NULL_DIMENSION_VALUE = "";
  private static final String INVALID_DIMENSION_VALUE = "INVALID";
  private static final String AUTO_METRIC_COUNT = "__COUNT";

  private enum TernaryOperators {
    EQ,
    LT,
    LE,
    GT,
    GE
  }

  private static String getRecordValue(String fieldName, GenericRecord record) {
    String[] fieldNames = fieldName.split(NAME_SEPARATOR);

    // Traverse to last field
    GenericRecord current = record;
    for (int i = 0; i < fieldNames.length - 1; i++) {
      String name = fieldNames[i];
      current = (GenericRecord) current.get(name);
    }

    // Get the terminal field value
    String terminalField = fieldNames[fieldNames.length - 1];
    Object dimensionObj = current.get(terminalField);
    if (dimensionObj == null) {
      return NULL_DIMENSION_VALUE;
    }
    return dimensionObj.toString();
  }

  private static Number getMetricValue(MetricSpec metricSpec, GenericRecord record) {
    // Automatic count metric
    if (AUTO_METRIC_COUNT.equals(metricSpec.getName())) {
      return 1;
    }

    // Extract from record
    String metricStr = getRecordValue(metricSpec.getName(), record);
    return NumberUtils.valueOf(metricStr, metricSpec.getType());
  }

  // Attempt to group into email domains
  private static String parseEmailDomain(String emailAddress, Properties normalize) {
    if (!emailAddress.contains("@")) {
      return INVALID_DIMENSION_VALUE;
    }

    String[] tokens = emailAddress.split("@");
    if (tokens.length != 2) {
      return INVALID_DIMENSION_VALUE;
    }

    // Rudimentary normalization
    String domain = tokens[1];
    if (normalize != null) {
      for (String pattern : normalize.stringPropertyNames()) {
        if (domain.toLowerCase().contains(pattern)) {
          return normalize.getProperty(pattern);
        }
      }
    }

    return domain;
  }

  /**
   * Returns a new value based on some condition.
   *
   * @param dimensionValue
   *  An integral numeric dimension value
   * @param config
   *  Contains a "condition", "operator" (LT, LE, GT, GE, EQ), "true" value if true, "false" value if false
   */
  private static String parseTernaryValue(String dimensionValue, Properties config) {
    String trueValue = config.getProperty("trueValue");
    if (trueValue == null) {
      throw new IllegalArgumentException("Ternary operator must provide true value");
    }

    String falseValue = config.getProperty("falseValue");
    if (falseValue == null) {
      throw new IllegalArgumentException("Ternary operator must provide false value");
    }

    TernaryOperators operator = TernaryOperators.valueOf(config.getProperty("operator"));
    long value = Long.valueOf(dimensionValue);
    long conditionValue = Long.valueOf(config.getProperty("condition"));
    switch (operator) {
      case EQ: return value == conditionValue ? trueValue : falseValue;
      case LT: return value < conditionValue ? trueValue : falseValue;
      case LE: return value <= conditionValue ? trueValue : falseValue;
      case GT: return value > conditionValue ? trueValue : falseValue;
      case GE: return value >= conditionValue ? trueValue : falseValue;
    }

    throw new IllegalStateException("Could not apply " + config + " to value " + dimensionValue);
  }

  public static StarTreeRecord convert(StarTreeConfig config, GenericRecord record) {
    // Dimensions
    String[] dimensionValues = new String[config.getDimensions().size()];
    for (int i = 0; i < config.getDimensions().size(); i++) {
      DimensionSpec dimensionSpec = config.getDimensions().get(i);
      String dimensionValue = getRecordValue(dimensionSpec.getName(), record);
      if (!NULL_DIMENSION_VALUE.equals(dimensionValue) && dimensionSpec.getType() != null) {
        switch (dimensionSpec.getType()) {
          case EMAIL_DOMAIN:
            dimensionValue = parseEmailDomain(dimensionValue, dimensionSpec.getConfig());
            break;
          case TERNARY:
            dimensionValue = parseTernaryValue(dimensionValue, dimensionSpec.getConfig());
            break;
          case IP_GEO:
            dimensionValue = ThirdEyeIPUtils.getCountry(dimensionValue);
            break;
        }
      }
      dimensionValues[i] = dimensionValue;
    }
    DimensionKey dimensionKey = new DimensionKey(dimensionValues);

    // Time
    String timeColumnName = config.getTime().getColumnName();
    String timeStr = getRecordValue(timeColumnName, record);
    if (timeStr == null) {
      throw new IllegalArgumentException("Record has null time " + timeColumnName + ": " + record);
    }
    Long time = Long.valueOf(timeStr);

    if (time <= 0) {
      LOGGER.warn("Skipping because zero or negative time {}", record);
      return null;
    }

    // Convert time to storage granularity
    TimeGranularity inputGranularity = config.getTime().getInput();
    TimeGranularity bucketGranularity = config.getTime().getBucket();
    time = bucketGranularity.getUnit().convert(
        time * inputGranularity.getSize(), inputGranularity.getUnit()) / bucketGranularity.getSize();

    // Metrics
    MetricTimeSeries timeSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(config.getMetrics()));
    for (int i = 0; i < config.getMetrics().size(); i++) {
      MetricSpec metricSpec = config.getMetrics().get(i);
      Number metricValue = getMetricValue(metricSpec, record);
      timeSeries.increment(time, metricSpec.getName(), metricValue);
    }

    return new StarTreeRecordImpl(config, dimensionKey, timeSeries);
  }
}
