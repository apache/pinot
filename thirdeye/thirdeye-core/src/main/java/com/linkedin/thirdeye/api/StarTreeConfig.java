package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.impl.storage.StarTreeRecordStoreFactoryFixedImpl;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public final class StarTreeConfig
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  private static final String DEFAULT_RECORD_STORE_FACTORY_CLASS = StarTreeRecordStoreFactoryFixedImpl.class.getCanonicalName();

  private String collection;
  private String recordStoreFactoryClass = DEFAULT_RECORD_STORE_FACTORY_CLASS;
  private Properties recordStoreFactoryConfig;
  private List<DimensionSpec> dimensions;
  private List<MetricSpec> metrics;
  private TimeSpec time = new TimeSpec();
  private RollupSpec rollup = new RollupSpec();
  private SplitSpec split = new SplitSpec();
  private boolean fixed = true;

  // Anomaly detection
  private String anomalyDetectionFunctionClass;
  private Properties anomalyDetectionFunctionConfig;
  private String anomalyHandlerClass;
  private Properties anomalyHandlerConfig;
  private String anomalyDetectionMode;

  public StarTreeConfig() {}

  private StarTreeConfig(String collection,
                         String recordStoreFactoryClass,
                         Properties recordStoreFactoryConfig,
                         String anomalyDetectionFunctionClass,
                         Properties anomalyDetectionFunctionConfig,
                         String anomalyHandlerClass,
                         Properties anomalyHandlerConfig,
                         String anomalyDetectionMode,
                         List<DimensionSpec> dimensions,
                         List<MetricSpec> metrics,
                         TimeSpec time,
                         RollupSpec rollup,
                         SplitSpec split,
                         boolean fixed)
  {
    this.collection = collection;
    this.recordStoreFactoryClass = recordStoreFactoryClass;
    this.recordStoreFactoryConfig = recordStoreFactoryConfig;
    this.anomalyDetectionFunctionClass = anomalyDetectionFunctionClass;
    this.anomalyDetectionFunctionConfig = anomalyDetectionFunctionConfig;
    this.anomalyHandlerClass = anomalyHandlerClass;
    this.anomalyHandlerConfig = anomalyHandlerConfig;
    this.anomalyDetectionMode = anomalyDetectionMode;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.time = time;
    this.rollup = rollup;
    this.split = split;
    this.fixed = fixed;
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

  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  public String getAnomalyDetectionFunctionClass()
  {
    return anomalyDetectionFunctionClass;
  }

  public Properties getAnomalyDetectionFunctionConfig()
  {
    return anomalyDetectionFunctionConfig;
  }

  public String getAnomalyHandlerClass()
  {
    return anomalyHandlerClass;
  }

  public Properties getAnomalyHandlerConfig()
  {
    return anomalyHandlerConfig;
  }

  public String getAnomalyDetectionMode()
  {
    return anomalyDetectionMode;
  }

  public List<MetricSpec> getMetrics()
  {
    return metrics;
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

  public boolean isFixed()
  {
    return fixed;
  }

  public String encode() throws IOException
  {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public static class Builder
  {
    private String collection;
    private List<DimensionSpec> dimensions;
    private List<MetricSpec> metrics;
    private String recordStoreFactoryClass = DEFAULT_RECORD_STORE_FACTORY_CLASS;
    private Properties recordStoreFactoryConfig;
    private String anomalyDetectionFunctionClass;
    private Properties anomalyDetectionFunctionConfig;
    private String anomalyHandlerClass;
    private Properties anomalyHandlerConfig;
    private String anomalyDetectionMode;
    private TimeSpec time = new TimeSpec();
    private RollupSpec rollup = new RollupSpec();
    private SplitSpec split = new SplitSpec();
    private boolean fixed = true;

    public String getCollection()
    {
      return collection;
    }

    public Builder setCollection(String collection)
    {
      this.collection = collection;
      return this;
    }

    public List<DimensionSpec> getDimensions()
    {
      return dimensions;
    }

    public Builder setDimensions(List<DimensionSpec> dimensions)
    {
      this.dimensions = dimensions;
      return this;
    }

    public List<MetricSpec> getMetrics()
    {
      return metrics;
    }

    public Builder setMetrics(List<MetricSpec> metrics)
    {
      this.metrics = metrics;
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

    public String getAnomalyDetectionFunctionClass()
    {
      return anomalyDetectionFunctionClass;
    }

    public Builder setAnomalyDetectionFunctionClass(String anomalyDetectionFunctionClass)
    {
      this.anomalyDetectionFunctionClass = anomalyDetectionFunctionClass;
      return this;
    }

    public Properties getAnomalyDetectionFunctionConfig()
    {
      return anomalyDetectionFunctionConfig;
    }

    public Builder setAnomalyDetectionFunctionConfig(Properties anomalyDetectionFunctionConfig)
    {
      this.anomalyDetectionFunctionConfig = anomalyDetectionFunctionConfig;
      return this;
    }

    public String getAnomalyHandlerClass()
    {
      return anomalyHandlerClass;
    }

    public Builder setAnomalyHandlerClass(String anomalyHandlerClass)
    {
      this.anomalyHandlerClass = anomalyHandlerClass;
      return this;
    }

    public Properties getAnomalyHandlerConfig()
    {
      return anomalyHandlerConfig;
    }

    public Builder setAnomalyHandlerConfig(Properties anomalyHandlerConfig)
    {
      this.anomalyHandlerConfig = anomalyHandlerConfig;
      return this;
    }

    public String getAnomalyDetectionMode()
    {
      return anomalyDetectionMode;
    }

    public Builder setAnomalyDetectionMode(String anomalyDetectionMode)
    {
      this.anomalyDetectionMode = anomalyDetectionMode;
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

    public boolean isFixed()
    {
      return fixed;
    }

    public Builder setFixed(boolean fixed)
    {
      this.fixed = fixed;
      return this;
    }

    public StarTreeConfig build() throws Exception
    {
      if (collection == null)
      {
        throw new IllegalArgumentException("Must provide collection");
      }

      if (dimensions == null || dimensions.isEmpty())
      {
        throw new IllegalArgumentException("Must provide dimension names");
      }

      if (metrics == null || metrics.isEmpty())
      {
        throw new IllegalArgumentException("Must provide metric specs");
      }

      return new StarTreeConfig(collection,
                                recordStoreFactoryClass,
                                recordStoreFactoryConfig,
                                anomalyDetectionFunctionClass,
                                anomalyDetectionFunctionConfig,
                                anomalyHandlerClass,
                                anomalyHandlerConfig,
                                anomalyDetectionMode,
                                dimensions,
                                metrics,
                                time,
                                rollup,
                                split,
                                fixed);
    }
  }

  public static StarTreeConfig decode(InputStream inputStream) throws IOException
  {
    return OBJECT_MAPPER.readValue(inputStream, StarTreeConfig.class);
  }
}
