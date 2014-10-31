package com.linkedin.thirdeye.api;

import com.linkedin.thirdeye.impl.StarTreeRecordImpl;

import java.util.List;

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
    private StarTreeRecordStoreFactory recordStoreFactory;
    private StarTreeRecordThresholdFunction thresholdFunction;
    private int maxRecordStoreEntries = 1000;
    private List<String> dimensionNames;
    private List<String> metricNames;

    public StarTreeRecordStoreFactory getRecordStoreFactory()
    {
      return recordStoreFactory;
    }

    public Builder setRecordStoreFactory(StarTreeRecordStoreFactory recordStoreFactory)
    {
      this.recordStoreFactory = recordStoreFactory;
      return this;
    }

    public StarTreeRecordThresholdFunction getThresholdFunction()
    {
      return thresholdFunction;
    }

    public Builder setThresholdFunction(StarTreeRecordThresholdFunction thresholdFunction)
    {
      this.thresholdFunction = thresholdFunction;
      return this;
    }

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

    public StarTreeConfig build()
    {
      if (metricNames == null || metricNames.isEmpty())
      {
        throw new IllegalArgumentException("Must provide metric names");
      }
      if (dimensionNames == null || dimensionNames.isEmpty())
      {
        throw new IllegalArgumentException("Must provide dimension names");
      }

      return new StarTreeConfig(recordStoreFactory,
                                thresholdFunction,
                                maxRecordStoreEntries,
                                dimensionNames,
                                metricNames);
    }
  }
}
