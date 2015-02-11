package com.linkedin.thirdeye.anomaly;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.impl.NumberUtils;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AnomalyDetectionFunctionSimpleThresholdImpl implements AnomalyDetectionFunction
{
  private static final String PROP_TIME_SIZE = "time.size";
  private static final String PROP_TIME_UNIT = "time.unit";
  private static final String PROP_METRIC_NAME = "metric.name";
  private static final String PROP_METRIC_THRESHOLD = "metric.threshold";
  private static final String PROP_METRIC_VALUE = "metric.value";

  private TimeGranularity timeGranularity;
  private String metricName;
  private MetricType metricType;
  private Number metricThreshold;

  @Override
  public void init(StarTreeConfig starTreeConfig, Properties functionConfig)
  {
    int timeSize = Integer.valueOf(functionConfig.getProperty(PROP_TIME_SIZE));
    TimeUnit timeUnit = TimeUnit.valueOf(functionConfig.getProperty(PROP_TIME_UNIT));
    timeGranularity = new TimeGranularity(timeSize, timeUnit);
    metricName = functionConfig.getProperty(PROP_METRIC_NAME);

    for (MetricSpec metricSpec : starTreeConfig.getMetrics())
    {
      if (metricSpec.getName().equals(metricName))
      {
        metricType = metricSpec.getType();
        metricThreshold = NumberUtils.valueOf(functionConfig.getProperty(PROP_METRIC_THRESHOLD), metricSpec.getType());
      }
    }

    if (metricThreshold == null)
    {
      throw new IllegalStateException("Unrecognized metric " + metricName);
    }
  }

  @Override
  public TimeGranularity getWindowTimeGranularity()
  {
    return timeGranularity;
  }

  @Override
  public AnomalyResult analyze(DimensionKey dimensionKey, MetricTimeSeries metricTimeSeries)
  {
    Number sum = 0;
    for (Long time : metricTimeSeries.getTimeWindowSet())
    {
      sum = NumberUtils.sum(sum, metricTimeSeries.get(time, metricName), metricType);
    }

    if (NumberUtils.difference(sum, metricThreshold, metricType).intValue() < 0)
    {
      Properties props = new Properties();
      props.setProperty(PROP_METRIC_VALUE, sum.toString());
      return new AnomalyResult(true, props);
    }

    return new AnomalyResult(false);
  }
}
