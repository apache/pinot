package com.linkedin.thirdeye.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
  private Properties config;

  private File rootDir;
  private int numTimeBuckets;

  @Override
  public void init(File rootDir, List<String> dimensionNames, List<String> metricNames, List<String> metricTypes, Properties config)
  {
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.config = config;
    this.rootDir = rootDir;

    String numTimeBucketsString = config.getProperty("numTimeBuckets");
    if (numTimeBucketsString == null)
    {
      throw new IllegalStateException("numTimeBuckets must be specified in configuration");
    }
    this.numTimeBuckets = Integer.valueOf(numTimeBucketsString);
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
  public Properties getConfig()
  {
    return config;
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
