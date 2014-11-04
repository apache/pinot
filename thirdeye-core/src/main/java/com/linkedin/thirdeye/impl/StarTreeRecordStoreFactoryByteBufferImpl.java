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
  private int bufferSize = 1024 * 1024;
  private double targetCompressionRatio = 0.8;
  private boolean useDirect = true;

  @Override
  public void init(List<String> dimensionNames, List<String> metricNames, Properties config)
  {
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;

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

      String targetCompressionRatioString = config.getProperty("targetCompressionRatio");
      if (targetCompressionRatioString != null)
      {
        targetCompressionRatio = Double.valueOf(targetCompressionRatioString);
      }
    }
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId)
  {
    return new StarTreeRecordStoreByteBufferImpl(
            nodeId, dimensionNames, metricNames, bufferSize, useDirect, targetCompressionRatio);
  }
}
