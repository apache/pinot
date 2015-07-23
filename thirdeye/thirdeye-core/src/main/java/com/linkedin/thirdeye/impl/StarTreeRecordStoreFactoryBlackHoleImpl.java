package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryBlackHoleImpl implements StarTreeRecordStoreFactory
{
  private List<MetricSpec> metricSpecs;

  @Override
  public void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig)
  {
    this.metricSpecs = starTreeConfig.getMetrics();
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId)
  {
    return new StarTreeRecordStoreBlackHoleImpl(metricSpecs);
  }

  @Override
  public void close() throws IOException {
    // NOP
  }
}
