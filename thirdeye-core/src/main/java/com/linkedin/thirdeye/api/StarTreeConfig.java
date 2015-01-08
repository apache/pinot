package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryCircularBufferImpl;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public final class StarTreeConfig
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  private static final String DEFAULT_RECORD_STORE_FACTORY_CLASS = StarTreeRecordStoreFactoryCircularBufferImpl.class.getCanonicalName();

  private String collection;
  private String recordStoreFactoryClass = DEFAULT_RECORD_STORE_FACTORY_CLASS;
  private Properties recordStoreFactoryConfig;
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;

  private TimeSpec time = new TimeSpec();
  private RollupSpec rollup = new RollupSpec();
  private SplitSpec split = new SplitSpec();

  public StarTreeConfig() {}

  private StarTreeConfig(String collection,
                         String recordStoreFactoryClass,
                         Properties recordStoreFactoryConfig,
                         List<String> dimensionNames,
                         List<String> metricNames,
                         List<String> metricTypes, 
                         TimeSpec time,
                         RollupSpec rollup,
                         SplitSpec split)
  {
    this.collection = collection;
    this.recordStoreFactoryClass = recordStoreFactoryClass;
    this.recordStoreFactoryConfig = recordStoreFactoryConfig;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.time = time;
    this.rollup = rollup;
    this.split = split;
  }

  public String getCollection()
  {
    return collection;
  }

  public String getRecordStoreFactoryClass()
  {
    return recordStoreFactoryClass;
  }

  public Properties getRecordStoreFactoryConfig()
  {
    return recordStoreFactoryConfig;
  }

  public List<String> getDimensionNames()
  {
    return dimensionNames;
  }

  public List<String> getMetricNames()
  {
    return metricNames;
  }
  
  public List<String> getMetricTypes()
  {
    return metricTypes;
  }

  public TimeSpec getTime()
  {
    return time;
  }

  public RollupSpec getRollup()
  {
    return rollup;
  }

  public SplitSpec getSplit()
  {
    return split;
  }

  public String encode() throws IOException
  {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public static class Builder
  {
    private String collection;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<String> metricTypes;
    private String recordStoreFactoryClass = DEFAULT_RECORD_STORE_FACTORY_CLASS;
    private Properties recordStoreFactoryConfig;
    private TimeSpec time = new TimeSpec();
    private RollupSpec rollup = new RollupSpec();
    private SplitSpec split = new SplitSpec();

    public String getCollection()
    {
      return collection;
    }

    public Builder setCollection(String collection)
    {
      this.collection = collection;
      return this;
    }

    public List<String> getDimensionNames()
    {
      return dimensionNames;
    }

    public Builder setDimensionNames(List<String> dimensionNames)
    {
      this.dimensionNames = dimensionNames;
      return this;
    }

    public List<String> getMetricNames()
    {
      return metricNames;
    }
    
    public Builder setMetricNames(List<String> metricNames)
    {
      this.metricNames = metricNames;
      return this;
    }
    
    public List<String> getMetricTypes()
    {
      return metricTypes;
    }
    
    public Builder setMetricTypes(List<String> metricTypes)
    {
      this.metricTypes = metricTypes;
      return this;
    }
   
    public String getRecordStoreFactoryClass()
    {
      return recordStoreFactoryClass;
    }

    public Builder setRecordStoreFactoryClass(String recordStoreFactoryClass)
    {
      this.recordStoreFactoryClass = recordStoreFactoryClass;
      return this;
    }

    public Properties getRecordStoreFactoryConfig()
    {
      return recordStoreFactoryConfig;
    }

    public Builder setRecordStoreFactoryConfig(Properties recordStoreFactoryConfig)
    {
      this.recordStoreFactoryConfig = recordStoreFactoryConfig;
      return this;
    }

    public TimeSpec getTime()
    {
      return time;
    }

    public Builder setTime(TimeSpec time)
    {
      this.time = time;
      return this;
    }

    public RollupSpec getRollup()
    {
      return rollup;
    }

    public Builder setRollup(RollupSpec rollup)
    {
      this.rollup = rollup;
      return this;
    }

    public SplitSpec getSplit()
    {
      return split;
    }

    public Builder setSplit(SplitSpec split)
    {
      this.split = split;
      return this;
    }

    public StarTreeConfig build() throws Exception
    {
      if (collection == null)
      {
        throw new IllegalArgumentException("Must provide collection");
      }

      if (metricNames == null || metricNames.isEmpty())
      {
        throw new IllegalArgumentException("Must provide metric names");
      }

      if (dimensionNames == null || dimensionNames.isEmpty())
      {
        throw new IllegalArgumentException("Must provide dimension names");
      }

      if (metricTypes == null || metricTypes.isEmpty())
      {
        throw new IllegalArgumentException("Must provide metric types");
      }

      return new StarTreeConfig(collection,
                                recordStoreFactoryClass,
                                recordStoreFactoryConfig,
                                dimensionNames,
                                metricNames,
                                metricTypes,
                                time,
                                rollup,
                                split);
    }
  }

  public static StarTreeConfig decode(InputStream inputStream) throws IOException
  {
    return OBJECT_MAPPER.readValue(inputStream, StarTreeConfig.class);
  }
}
