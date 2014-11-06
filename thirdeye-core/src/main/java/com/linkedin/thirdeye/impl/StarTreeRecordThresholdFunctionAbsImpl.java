package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StarTreeRecordThresholdFunctionAbsImpl implements StarTreeRecordThresholdFunction
{
  private Properties config;

  private Map<String, Long> metricThresholdValues = new HashMap<String, Long>();

  @Override
  public void init(Properties config)
  {
    this.config = config;
    if (config != null)
    {
      for (Map.Entry<Object, Object> entry : config.entrySet())
      {
        String metricName = (String) entry.getKey();
        Long thresholdValue = Long.valueOf((String) entry.getValue());
        metricThresholdValues.put(metricName, thresholdValue);
      }
    }
  }

  @Override
  public Properties getConfig()
  {
    return config;
  }

  @Override
  public boolean passesThreshold(Iterable<StarTreeRecord> records)
  {
    Map<String, Long> aggValues = new HashMap<String, Long>();
    for (Map.Entry<String, Long> entry : metricThresholdValues.entrySet())
    {
      aggValues.put(entry.getKey(), 0L);
    }

    for (StarTreeRecord record : records)
    {
      for (String metricName : metricThresholdValues.keySet())
      {
        Long metricValue = record.getMetricValues().get(metricName);
        if (metricValue == null)
        {
          throw new IllegalArgumentException("Record must contain metric " + metricName
                                                     + " if there's a threshold that must be passed: " + record);
        }
        Long aggValue = aggValues.get(metricName);
        aggValues.put(metricName, aggValue + metricValue);
      }
    }

    for (Map.Entry<String, Long> entry : metricThresholdValues.entrySet())
    {
      Long aggValue = aggValues.get(entry.getKey());
      Long thresholdValue = entry.getValue();
      if (aggValue < thresholdValue)
      {
        return false;
      }
    }

    return true;
  }
}
