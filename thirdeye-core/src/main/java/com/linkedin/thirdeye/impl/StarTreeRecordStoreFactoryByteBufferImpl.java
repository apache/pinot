package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryByteBufferImpl implements StarTreeRecordStoreFactory
{
  private List<String> dimensionNames;
  private List<String> metricNames;
  private Properties config;

  private int bufferSize = 1024 * 1024;
  private double targetLoadFactor = 0.8;
  private boolean useDirect = true;

  @Override
  public void init(List<String> dimensionNames, List<String> metricNames, Properties config)
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

      String targetCompressionRatioString = config.getProperty("targetLoadFactor");
      if (targetCompressionRatioString != null)
      {
        targetLoadFactor = Double.valueOf(targetCompressionRatioString);
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
    return new StarTreeRecordStoreByteBufferImpl(
            nodeId, dimensionNames, metricNames, bufferSize, useDirect, targetLoadFactor);
  }
}
