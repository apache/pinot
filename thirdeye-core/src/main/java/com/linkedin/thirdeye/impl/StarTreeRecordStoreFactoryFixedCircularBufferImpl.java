package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryFixedCircularBufferImpl implements StarTreeRecordStoreFactory
{
  public static final String BUFFER_SUFFIX = ".buffer";
  public static final String INDEX_SUFFIX = ".index";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference TYPE_REFERENCE = new TypeReference<Map<String, Map<String, Integer>>>(){};

  private List<String> dimensionNames;
  private List<String> metricNames;

  private File rootDir;

  @Override
  public void init(List<String> dimensionNames, List<String> metricNames, Properties config)
  {
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;

    String rootDirString = config.getProperty("rootDir");
    if (rootDirString == null)
    {
      throw new IllegalStateException("rootDir must be specified in configuration");
    }
    this.rootDir = new File(rootDirString);
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId)
  {
    File indexFile = new File(rootDir, nodeId.toString() + INDEX_SUFFIX);

    Map<String, Map<String, Integer>> forwardIndex;
    try
    {
      forwardIndex = OBJECT_MAPPER.readValue(new FileInputStream(indexFile), TYPE_REFERENCE);
    }
    catch (Exception e)
    {
      throw new IllegalStateException(e);
    }

    File bufferFile = new File(rootDir, nodeId.toString() + BUFFER_SUFFIX);
    return new StarTreeRecordStoreFixedCircularBufferImpl(bufferFile, dimensionNames, metricNames, forwardIndex);
  }
}
