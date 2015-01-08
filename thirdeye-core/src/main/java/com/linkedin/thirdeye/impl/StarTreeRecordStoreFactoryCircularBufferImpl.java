package com.linkedin.thirdeye.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryCircularBufferImpl implements StarTreeRecordStoreFactory
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference TYPE_REFERENCE = new TypeReference<Map<String, Map<String, Integer>>>(){};

  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;
  private Properties recordStoreConfig;

  private File rootDir;
  private int numTimeBuckets;

  @Override
  public void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig)
  {
    this.dimensionNames = starTreeConfig.getDimensionNames();
    this.metricNames = starTreeConfig.getMetricNames();
    this.metricTypes = starTreeConfig.getMetricTypes();
    this.recordStoreConfig = recordStoreConfig;
    this.rootDir = rootDir;
    this.numTimeBuckets = (int) starTreeConfig.getTime().getBucket().getUnit().convert(
            starTreeConfig.getTime().getRetention().getSize(),
            starTreeConfig.getTime().getRetention().getUnit());
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
    File indexFile = new File(rootDir, nodeId.toString() + StarTreeConstants.INDEX_FILE_SUFFIX);

    Map<String, Map<String, Integer>> forwardIndex;
    try
    {
      forwardIndex = OBJECT_MAPPER.readValue(new FileInputStream(indexFile), TYPE_REFERENCE);
    }
    catch (Exception e)
    {
      throw new IllegalStateException(e);
    }

    File bufferFile = new File(rootDir, nodeId.toString() + StarTreeConstants.BUFFER_FILE_SUFFIX);

    return new StarTreeRecordStoreCircularBufferImpl(nodeId,
                                                     bufferFile,
                                                     dimensionNames,
                                                     metricNames,
                                                     metricTypes,
                                                     forwardIndex,
                                                     numTimeBuckets);
  }
}
