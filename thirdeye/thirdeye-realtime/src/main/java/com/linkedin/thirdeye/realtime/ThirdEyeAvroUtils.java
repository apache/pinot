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

public class ThirdEyeAvroUtils
{
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeAvroUtils.class);

  public static StarTreeRecord convert(StarTreeConfig config, GenericRecord record)
  {
    // Dimensions
    String[] dimensionValues = new String[config.getDimensions().size()];
    for (int i = 0; i < config.getDimensions().size(); i++)
    {
      String dimensionName = config.getDimensions().get(i).getName();
      Object dimensionObj = record.get(dimensionName);
      if (dimensionObj == null)
      {
        dimensionObj = "";
      }
      dimensionValues[i] = dimensionObj.toString();
    }
    DimensionKey dimensionKey = new DimensionKey(dimensionValues);

    // Time
    Object timeObj = record.get(config.getTime().getColumnName());
    if (timeObj == null)
    {
      throw new IllegalArgumentException("Record has null time " + config.getTime().getColumnName() + ": " + record);
    }
    if (!(timeObj instanceof Number))
    {
      throw new IllegalArgumentException("Time must be numeric (it is " + timeObj.getClass() + ")");
    }
    Long time = ((Number) timeObj).longValue();

    if (time <= 0)
    {
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
    for (int i = 0; i < config.getMetrics().size(); i++)
    {
      MetricSpec metricSpec = config.getMetrics().get(i);
      Object metricObj = record.get(metricSpec.getName());
      if (metricObj == null)
      {
        metricObj = 0;
      }
      Number metricValue = NumberUtils.valueOf(metricObj.toString(), metricSpec.getType());
      timeSeries.increment(time, metricSpec.getName(), metricValue);
    }

    return new StarTreeRecordImpl(config, dimensionKey, timeSeries);
  }
}
