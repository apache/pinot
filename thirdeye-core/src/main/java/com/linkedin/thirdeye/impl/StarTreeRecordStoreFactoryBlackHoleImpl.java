package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConfig;
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
  private Properties recordStoreConfig;
  private List<String> metricTypes;

  @Override
  public void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig)
  {
    this.dimensionNames = starTreeConfig.getDimensionNames();
    this.metricNames = starTreeConfig.getMetricNames();
    this.metricTypes = starTreeConfig.getMetricTypes();
    this.recordStoreConfig = recordStoreConfig;
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
  public Properties getRecordStoreConfig()
  {
    return recordStoreConfig;
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId)
  {
    return new StarTreeRecordStoreBlackHoleImpl(dimensionNames, metricNames);
  }
}
