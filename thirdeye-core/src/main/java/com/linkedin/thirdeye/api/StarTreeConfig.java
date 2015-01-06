package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryLogBufferImpl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class StarTreeConfig
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String collection;
  private final StarTreeRecordStoreFactory recordStoreFactory;
  private final int maxRecordStoreEntries;
  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final List<String> metricTypes;
  
  private final List<String> splitOrder;
  private final String timeColumnName;
  private final int timeBucketSize;
  private final TimeUnit timeBucketSizeUnit;

  private StarTreeConfig(String collection,
                         StarTreeRecordStoreFactory recordStoreFactory,
                         int maxRecordStoreEntries,
                         List<String> dimensionNames,
                         List<String> metricNames,
                         List<String> metricTypes, 
                         List<String> splitOrder,
                         String timeColumnName,
                         int timeBucketSize,
                         TimeUnit timeBucketSizeUnit)
  {
    this.collection = collection;
    this.recordStoreFactory = recordStoreFactory;
    this.maxRecordStoreEntries = maxRecordStoreEntries;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.splitOrder = splitOrder;
    this.timeColumnName = timeColumnName;
    this.timeBucketSize = timeBucketSize;
    this.timeBucketSizeUnit = timeBucketSizeUnit;
  }

  public String getCollection()
  {
    return collection;
  }

  public StarTreeRecordStoreFactory getRecordStoreFactory()
  {
    return recordStoreFactory;
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
  
  public List<String> getMetricTypes()
  {
    return metricTypes;
  }
  public List<String> getSplitOrder()
  {
    return splitOrder;
  }

  public String getTimeColumnName()
  {
    return timeColumnName;
  }

  public int getTimeBucketSize()
  {
    return timeBucketSize;
  }

  public TimeUnit getTimeBucketSizeUnit()
  {
    return timeBucketSizeUnit;
  }

  public String toJson() throws IOException
  {
    Map<String, Object> json = new HashMap<String, Object>();
    json.put("collection", collection);

    if (recordStoreFactory != null)
    {
      json.put("recordStoreFactoryClass", recordStoreFactory.getClass().getCanonicalName());
      json.put("recordStoreFactoryConfig", recordStoreFactory.getConfig());
    }

    if (splitOrder != null)
    {
      json.put("splitOrder", splitOrder);
    }

    json.put("dimensionNames", dimensionNames);
    json.put("metricNames", metricNames);
    json.put("metricTypes", metricTypes);
    json.put("timeColumnName", timeColumnName);
    json.put("maxRecordStoreEntries", maxRecordStoreEntries);

    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(json);
  }

  public static class Builder
  {
    private int maxRecordStoreEntries = 50000;
    private String collection;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<String> metricTypes;
    private List<String> splitOrder;
    private String timeColumnName;
    private TimeUnit timeBucketSizeUnit = TimeUnit.HOURS;
    private int timeBucketSize = 1;
    private String recordStoreFactoryClass = StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName();
    private Properties recordStoreFactoryConfig;

    public String getCollection()
    {
      return collection;
    }

    public Builder setCollection(String collection)
    {
      this.collection = collection;
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
    
    public List<String> getMetricTypes()
    {
      return metricTypes;
    }
    
    public Builder setMetricTypes(List<String> metricTypes)
    {
      this.metricTypes = metricTypes;
      return this;
    }
   
    public List<String> getSplitOrder()
    {
      return splitOrder;
    }

    public Builder setSplitOrder(List<String> splitOrder)
    {
      this.splitOrder = splitOrder;
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

    public String getTimeColumnName()
    {
      return timeColumnName;
    }

    public Builder setTimeColumnName(String timeColumnName)
    {
      this.timeColumnName = timeColumnName;
      return this;
    }

    public TimeUnit getTimeBucketSizeUnit()
    {
      return timeBucketSizeUnit;
    }

    public Builder setTimeBucketSizeUnit(TimeUnit timeBucketSizeUnit)
    {
      this.timeBucketSizeUnit = timeBucketSizeUnit;
      return this;
    }

    public int getTimeBucketSize()
    {
      return timeBucketSize;
    }

    public Builder setTimeBucketSize(int timeBucketSize)
    {
      this.timeBucketSize = timeBucketSize;
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

      StarTreeRecordStoreFactory rF = (StarTreeRecordStoreFactory) Class.forName(recordStoreFactoryClass).newInstance();
      rF.init(dimensionNames, metricNames, metricTypes, recordStoreFactoryConfig);

      return new StarTreeConfig(collection,
                                rF,
                                maxRecordStoreEntries,
                                dimensionNames,
                                metricNames,
                                metricTypes,
                                splitOrder,
                                timeColumnName,
                                timeBucketSize,
                                timeBucketSizeUnit);
    }
  }

  public static StarTreeConfig fromJson(JsonNode jsonNode) throws Exception
  {
    return fromJson(jsonNode, null);
  }

  public static StarTreeConfig fromJson(JsonNode jsonNode, File rootDir) throws Exception
  {
    // Get collection
    String collection = jsonNode.get("collection").asText();

    // Get dimension names
    List<String> dimensionNames = new ArrayList<String>();
    for (JsonNode dimensionName : jsonNode.get("dimensionNames"))
    {
      dimensionNames.add(dimensionName.asText());
    }

    // Get metric names
    List<String> metricNames = new ArrayList<String>();
    for (JsonNode metricName : jsonNode.get("metricNames"))
    {
      metricNames.add(metricName.asText());
    }
    List<String> metricTypes = new ArrayList<String>();
    for (JsonNode metricType : jsonNode.get("metricTypes"))
    {
      metricTypes.add(metricType.asText());
    }
    // Get time column name
    String timeColumnName = jsonNode.get("timeColumnName").asText();

    // Build jsonNode
    StarTreeConfig.Builder starTreeConfig = new StarTreeConfig.Builder();
    starTreeConfig.setCollection(collection)
                  .setDimensionNames(dimensionNames)
                  .setMetricNames(metricNames)
                  .setMetricTypes(metricTypes)
                  .setTimeColumnName(timeColumnName);

    // Aggregation info
    if (jsonNode.has("timeBucketSize"))
    {
      starTreeConfig.setTimeBucketSize(jsonNode.get("timeBucketSize").asInt());
    }
    if (jsonNode.has("timeBucketSizeUnit"))
    {
      starTreeConfig.setTimeBucketSizeUnit(TimeUnit.valueOf(jsonNode.get("timeBucketSizeUnit").asText().toUpperCase()));
    }

    // Record store
    if (jsonNode.has("recordStoreFactoryClass"))
    {
      starTreeConfig.setRecordStoreFactoryClass(jsonNode.get("recordStoreFactoryClass").asText());
    }

    // Record store config
    Properties recordStoreConfig = new Properties();
    if (jsonNode.has("recordStoreFactoryConfig"))
    {
      Iterator<Map.Entry<String, JsonNode>> itr = jsonNode.get("recordStoreFactoryConfig").fields();
      while (itr.hasNext())
      {
        Map.Entry<String, JsonNode> next = itr.next();
        recordStoreConfig.put(next.getKey(), next.getValue().asText());
      }
      if (rootDir != null)
      {
        recordStoreConfig.put("rootDir", new File(new File(rootDir, collection), StarTreeConstants.DATA_DIR_NAME).getAbsolutePath());
      }
    }
    starTreeConfig.setRecordStoreFactoryConfig(recordStoreConfig);

    // Record store entries
    if (jsonNode.has("maxRecordStoreEntries"))
    {
      starTreeConfig.setMaxRecordStoreEntries(jsonNode.get("maxRecordStoreEntries").asInt());
    }

    // Split order
    if (jsonNode.has("splitOrder"))
    {
      List<String> splitOrder = new ArrayList<String>();
      for (JsonNode dimensionName : jsonNode.get("splitOrder"))
      {
        splitOrder.add(dimensionName.asText());
      }
      starTreeConfig.setSplitOrder(splitOrder);
    }

    return starTreeConfig.build();
  }
}
