package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryLogBufferImpl implements StarTreeRecordStoreFactory
{
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;

  private Properties recordStoreConfig;

  private int bufferSize = 1024 * 1;
  private double targetLoadFactor = 0.8;
  private boolean useDirect = true;

  @Override
  public void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig)
  {
    this.dimensionNames = starTreeConfig.getDimensionNames();
    this.metricNames = starTreeConfig.getMetricNames();
    this.metricTypes = starTreeConfig.getMetricTypes();
    this.recordStoreConfig = recordStoreConfig;

    if (recordStoreConfig != null)
    {
      String bufferSizeString = recordStoreConfig.getProperty("bufferSize");
      if (bufferSizeString != null)
      {
        bufferSize = Integer.valueOf(bufferSizeString);
      }

      String useDirectString = recordStoreConfig.getProperty("useDirect");
      if (useDirectString != null)
      {
        useDirect = Boolean.valueOf(useDirectString);
      }

      String targetLoadFactorString = recordStoreConfig.getProperty("targetLoadFactor");
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
  public List<String> getMetricTypes()
  {
    return metricTypes;
  }

  @Override
  public Properties getRecordStoreConfig()
  {
    return recordStoreConfig;
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId)
  {
    return new StarTreeRecordStoreLogBufferImpl(
            nodeId, dimensionNames, metricNames, metricTypes, bufferSize, useDirect, targetLoadFactor);
  }
}
