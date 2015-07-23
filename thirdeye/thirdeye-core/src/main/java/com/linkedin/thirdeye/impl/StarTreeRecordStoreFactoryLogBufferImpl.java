package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryLogBufferImpl implements StarTreeRecordStoreFactory
{
  private StarTreeConfig config;

  private int bufferSize = 1024 * 1;
  private double targetLoadFactor = 0.8;
  private boolean useDirect = true;

  @Override
  public void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig)
  {
    this.config = starTreeConfig;

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
  public StarTreeRecordStore createRecordStore(UUID nodeId)
  {
    return new StarTreeRecordStoreLogBufferImpl(
            nodeId, config, bufferSize, useDirect, targetLoadFactor);
  }

  @Override
  public void close() throws IOException {
    // NOP
  }
}
