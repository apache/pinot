package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
  public Set<String> apply(Map<String, List<StarTreeRecord>> sample)
  {
    Set<String> passingValues = new HashSet<String>();

    for (Map.Entry<String, List<StarTreeRecord>> sampleEntry : sample.entrySet())
    {
      // Compute aggregates
      Map<String, Long> aggregates = new HashMap<String, Long>();
      for (StarTreeRecord record : sampleEntry.getValue())
      {
        for (String metricName : metricThresholdValues.keySet())
        {
          Long value = aggregates.get(metricName);
          if (value == null)
          {
            value = 0L;
          }
          aggregates.put(metricName, value + record.getMetricValues().get(metricName));
        }
      }

      // Check if passes threshold
      boolean passes = true;
      for (Map.Entry<String, Long> thresholdEntry : metricThresholdValues.entrySet())
      {
        Long aggregateValue = aggregates.get(thresholdEntry.getKey());

        if (aggregateValue < thresholdEntry.getValue())
        {
          passes = false;
          break;
        }
      }

      if (passes)
      {
        passingValues.add(sampleEntry.getKey());
      }
    }

    return passingValues;
  }
}
