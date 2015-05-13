package com.linkedin.thirdeye.realtime;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.impl.NumberUtils;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThirdEyeAvroUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeAvroUtils.class);
  private static final String NAME_SEPARATOR = "\\.";
  private static final String NULL_DIMENSION_VALUE = "";
  private static final String AUTO_METRIC_COUNT = "__COUNT";

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

  public static StarTreeRecord convert(StarTreeConfig config, GenericRecord record) {
    // Dimensions
    String[] dimensionValues = new String[config.getDimensions().size()];
    for (int i = 0; i < config.getDimensions().size(); i++) {
      String dimensionName = config.getDimensions().get(i).getName();
      dimensionValues[i] = getRecordValue(dimensionName, record);
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
