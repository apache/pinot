package com.linkedin.thirdeye.bootstrap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryCircularBufferHdfsImpl implements StarTreeRecordStoreFactory
{
  public static final String BUFFER_SUFFIX = ".buf";
  public static final String INDEX_SUFFIX = ".idx";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference TYPE_REFERENCE = new TypeReference<Map<String, Map<String, Integer>>>(){};

  private List<String> dimensionNames;
  private List<String> metricNames;
  private Properties config;

  private Path rootDir;
  private int numTimeBuckets;

  @Override
  public void init(List<String> dimensionNames, List<String> metricNames, Properties config)
  {
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.config = config;

    String rootDirString = config.getProperty("rootDir");
    if (rootDirString == null)
    {
      throw new IllegalStateException("rootDir must be specified in configuration");
    }
    this.rootDir = new Path(rootDirString);

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
  public Properties getConfig()
  {
    return config;
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId)
  {
    FileSystem fileSystem = null;
    try
    {
      Configuration conf = new Configuration(); // TODO: What needs to be in here

      fileSystem = FileSystem.get(conf);

      Path indexPath = new Path(rootDir, nodeId.toString() + INDEX_SUFFIX);

      Map<String, Map<String, Integer>> forwardIndex
              = OBJECT_MAPPER.readValue(fileSystem.open(indexPath), TYPE_REFERENCE);

      Path bufferPath = new Path(rootDir, nodeId.toString() + BUFFER_SUFFIX);

      return new StarTreeRecordStoreCircularBufferHdfsImpl(
              nodeId, bufferPath, conf, dimensionNames, metricNames, forwardIndex, numTimeBuckets);
    }
    catch (Exception e)
    {
      throw new IllegalStateException(e);
    }
    finally
    {
      if (fileSystem != null)
      {
        try
        {
          fileSystem.close();
        }
        catch (Exception e)
        {
          throw new IllegalStateException(e);
        }
      }
    }
  }
}
