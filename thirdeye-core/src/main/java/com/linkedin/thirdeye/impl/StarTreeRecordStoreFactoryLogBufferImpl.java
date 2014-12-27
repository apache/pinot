package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryLogBufferImpl implements StarTreeRecordStoreFactory
{
  private List<String> dimensionNames;
  private List<String> metricNames;
  private Properties config;

  private int bufferSize = 1024 * 1;
  private double targetLoadFactor = 0.8;
  private boolean useDirect = true;

  @Override
  public void init(List<String> dimensionNames, List<String> metricNames, List<String> metricTypes,Properties config)
  {
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.config = config;

    if (config != null)
    {
      String bufferSizeString = config.getProperty("bufferSize");
      if (bufferSizeString != null)
      {
        bufferSize = Integer.valueOf(bufferSizeString);
      }

      String useDirectString = config.getProperty("useDirect");
      if (useDirectString != null)
      {
        useDirect = Boolean.valueOf(useDirectString);
      }

      String targetLoadFactorString = config.getProperty("targetLoadFactor");
      if (targetLoadFactorString != null)
      {
        targetLoadFactor = Double.valueOf(targetLoadFactorString);
      }
    }
  }

  @Override
  public List<String> getDimensionNames()
  {
    return dimensionNames;
  }

  @Override
  public List<String> getMetricNames()
  {
    return metricNames;
  }

  @Override
  public Properties getConfig()
  {
    return config;
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId)
  {
    return new StarTreeRecordStoreLogBufferImpl(
            nodeId, dimensionNames, metricNames, bufferSize, useDirect, targetLoadFactor);
  }
}
