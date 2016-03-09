package com.linkedin.thirdeye.bootstrap.util;

import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.impl.NumberUtils;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;

import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * TODO: The logic to parse / extract derived dimension values should probably be somewhere else,
 * but we can put it in here for now until we refactor (maybe into a thirdeye-avro module that
 * can be used by both thirdeye-realtime and thirdeye-bootstrap).
 * n.b. This (roughly) same class exists in the thirdeye-realtime module, but due to Avro version
 * mismatch
 * with other transitive dependencies in the deployment framework at LinkedIn, refactoring the logic
 * into
 * a common place will potentially cause trouble.
 */
public class ThirdEyeAvroUtils implements ThirdeyeConverter<GenericRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeAvroUtils.class);
  private static final String NAME_SEPARATOR = "\\.";
  private static final String NULL_VALUE = "";
  private static final String INVALID_DIMENSION_VALUE = "INVALID";

  public ThirdEyeAvroUtils() {

  }

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
      if (current == null) {
        return NULL_VALUE;
      }
    }

    // Get the terminal field value
    String terminalField = fieldNames[fieldNames.length - 1];
    Object dimensionObj = current.get(terminalField);

    if (dimensionObj == null) {
      return NULL_VALUE;
    }
    return dimensionObj.toString();
  }

  private static Number getMetricValue(MetricSpec metricSpec, GenericRecord record) {
    // Automatic count metric
    if (StarTreeConstants.AUTO_METRIC_COUNT.equals(metricSpec.getName())) {
      return 1;
    }

    // Extract from record
    String metricStr = getRecordValue(metricSpec.getName(), record);
    if (NULL_VALUE.equals(metricStr)) {
      return 0;
    }
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
   * @param dimensionValue
   *          An integral numeric dimension value
   * @param config
   *          Contains a "condition", "operator" (LT, LE, GT, GE, EQ), "true" value if true, "false"
   *          value if false
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
    case EQ:
      return value == conditionValue ? trueValue : falseValue;
    case LT:
      return value < conditionValue ? trueValue : falseValue;
    case LE:
      return value <= conditionValue ? trueValue : falseValue;
    case GT:
      return value > conditionValue ? trueValue : falseValue;
    case GE:
      return value >= conditionValue ? trueValue : falseValue;
    }

    throw new IllegalStateException("Could not apply " + config + " to value " + dimensionValue);
  }

  public StarTreeRecord convert(StarTreeConfig config, GenericRecord record) {
    return convert(config, record, null);
  }

  public static StarTreeRecord convert(StarTreeConfig config, GenericRecord record,
      DateTimeFormatter dateTimeFormatter) {
    // Dimensions
    String[] dimensionValues = new String[config.getDimensions().size()];
    for (int i = 0; i < config.getDimensions().size(); i++) {
      DimensionSpec dimensionSpec = config.getDimensions().get(i);
      String dimensionValue = getRecordValue(dimensionSpec.getName(), record);
      if (!NULL_VALUE.equals(dimensionValue) && dimensionSpec.getType() != null) {
        switch (dimensionSpec.getType()) {
        case EMAIL_DOMAIN:
          dimensionValue = parseEmailDomain(dimensionValue, dimensionSpec.getConfig());
          break;
        case TERNARY:
          dimensionValue = parseTernaryValue(dimensionValue, dimensionSpec.getConfig());
          break;
        }
      }
      dimensionValues[i] = dimensionValue;
    }
    DimensionKey dimensionKey = new DimensionKey(dimensionValues);

    // Check time column
    String timeStr = getRecordValue(config.getTime().getColumnName(), record);
    if (timeStr == null) {
      LOGGER.warn("Skipping because null time {}", record);
      return null;
    }

    // Parse time column
    TimeGranularity input = config.getTime().getInput();
    TimeGranularity bucket = config.getTime().getBucket();
    Long time;
    if (dateTimeFormatter == null) {
      // Time is raw long
      Long sourceTimeWindow = Long.parseLong(timeStr);
      time = bucket.getUnit().convert(sourceTimeWindow * input.getSize(), input.getUnit())
          / bucket.getSize();
    } else {
      // We parse time into milliseconds, then convert to aggregation granularity
      DateTime sourceTimeWindow = dateTimeFormatter.parseDateTime(timeStr);
      time = bucket.getUnit().convert(sourceTimeWindow.getMillis(), TimeUnit.MILLISECONDS)
          / bucket.getSize();
    }

    if (time <= 0) {
      LOGGER.warn("Skipping because zero or negative time {}", record);
      return null;
    }

    // Metrics
    MetricTimeSeries timeSeries =
        new MetricTimeSeries(MetricSchema.fromMetricSpecs(config.getMetrics()));
    for (int i = 0; i < config.getMetrics().size(); i++) {
      MetricSpec metricSpec = config.getMetrics().get(i);
      Number metricValue = getMetricValue(metricSpec, record);
      timeSeries.increment(time, metricSpec.getName(), metricValue);
    }

    return new StarTreeRecordImpl(config, dimensionKey, timeSeries);
  }

}
