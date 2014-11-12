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
      StarTreeRecord aggregate = StarTreeUtils.merge(sampleEntry.getValue());

      boolean passes = true;

      for (Map.Entry<String, Long> thresholdEntry : metricThresholdValues.entrySet())
      {
        Long aggregateValue = aggregate.getMetricValues().get(thresholdEntry.getKey());

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
