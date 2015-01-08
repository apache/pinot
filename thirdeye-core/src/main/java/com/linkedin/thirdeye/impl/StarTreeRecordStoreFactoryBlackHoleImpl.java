package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryBlackHoleImpl implements StarTreeRecordStoreFactory
{
  private List<String> dimensionNames;
  private List<String> metricNames;
  private Properties config;
  private List<String> metricTypes;

  @Override
  public void init(File rootDir, List<String> dimensionNames, List<String> metricNames, List<String> metricTypes, Properties config)
  {
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.config = config;
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
  public List<String> getMetricTypes() {
    return metricTypes;
  }
  @Override
  public Properties getConfig()
  {
    return config;
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId)
  {
    return new StarTreeRecordStoreBlackHoleImpl(dimensionNames, metricNames);
  }

  
}
