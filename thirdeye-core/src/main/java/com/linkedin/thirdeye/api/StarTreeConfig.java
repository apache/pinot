package com.linkedin.thirdeye.api;

import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryByteBufferImpl;

import java.util.List;
import java.util.Properties;

public final class StarTreeConfig
{
  private final StarTreeRecordStoreFactory recordStoreFactory;
  private final StarTreeRecordThresholdFunction thresholdFunction;
  private final int maxRecordStoreEntries;
  private final List<String> dimensionNames;
  private final List<String> metricNames;

  private StarTreeConfig(StarTreeRecordStoreFactory recordStoreFactory,
                         StarTreeRecordThresholdFunction thresholdFunction,
                         int maxRecordStoreEntries,
                         List<String> dimensionNames,
                         List<String> metricNames)
  {
    this.recordStoreFactory = recordStoreFactory;
    this.thresholdFunction = thresholdFunction;
    this.maxRecordStoreEntries = maxRecordStoreEntries;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
  }

  public StarTreeRecordStoreFactory getRecordStoreFactory()
  {
    return recordStoreFactory;
  }

  public StarTreeRecordThresholdFunction getThresholdFunction()
  {
    return thresholdFunction;
  }

  public int getMaxRecordStoreEntries()
  {
    return maxRecordStoreEntries;
  }

  public List<String> getDimensionNames()
  {
    return dimensionNames;
  }

  public List<String> getMetricNames()
  {
    return metricNames;
  }

  public static class Builder
  {
    private int maxRecordStoreEntries = 1000;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private String thresholdFunctionClass;
    private Properties thresholdFunctionConfig;
    private String recordStoreFactoryClass = StarTreeRecordStoreFactoryByteBufferImpl.class.getCanonicalName();
    private Properties recordStoreFactoryConfig;

    public int getMaxRecordStoreEntries()
    {
      return maxRecordStoreEntries;
    }

    public Builder setMaxRecordStoreEntries(int maxRecordStoreEntries)
    {
      this.maxRecordStoreEntries = maxRecordStoreEntries;
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

    public String getThresholdFunctionClass()
    {
      return thresholdFunctionClass;
    }

    public void setThresholdFunctionClass(String thresholdFunctionClass)
    {
      this.thresholdFunctionClass = thresholdFunctionClass;
    }

    public Properties getThresholdFunctionConfig()
    {
      return thresholdFunctionConfig;
    }

    public void setThresholdFunctionConfig(Properties thresholdFunctionConfig)
    {
      this.thresholdFunctionConfig = thresholdFunctionConfig;
    }

    public String getRecordStoreFactoryClass()
    {
      return recordStoreFactoryClass;
    }

    public void setRecordStoreFactoryClass(String recordStoreFactoryClass)
    {
      this.recordStoreFactoryClass = recordStoreFactoryClass;
    }

    public Properties getRecordStoreFactoryConfig()
    {
      return recordStoreFactoryConfig;
    }

    public void setRecordStoreFactoryConfig(Properties recordStoreFactoryConfig)
    {
      this.recordStoreFactoryConfig = recordStoreFactoryConfig;
    }

    public StarTreeConfig build() throws Exception
    {
      if (metricNames == null || metricNames.isEmpty())
      {
        throw new IllegalArgumentException("Must provide metric names");
      }

      if (dimensionNames == null || dimensionNames.isEmpty())
      {
        throw new IllegalArgumentException("Must provide dimension names");
      }

      StarTreeRecordThresholdFunction tF = null;
      if (thresholdFunctionClass != null)
      {
        tF = (StarTreeRecordThresholdFunction) Class.forName(thresholdFunctionClass).newInstance();
        tF.init(thresholdFunctionConfig);
      }

      StarTreeRecordStoreFactory rF = (StarTreeRecordStoreFactory) Class.forName(recordStoreFactoryClass).newInstance();
      rF.init(dimensionNames, metricNames, recordStoreFactoryConfig);

      return new StarTreeConfig(rF, tF, maxRecordStoreEntries, dimensionNames, metricNames);
    }
  }
}
