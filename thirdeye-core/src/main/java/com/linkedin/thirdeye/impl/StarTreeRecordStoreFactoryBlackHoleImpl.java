package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryBlackHoleImpl implements StarTreeRecordStoreFactory
{
  private List<String> dimensionNames;
  private List<String> metricNames;
  private Properties config;

  @Override
  public void init(List<String> dimensionNames, List<String> metricNames, Properties config)
  {
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
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
