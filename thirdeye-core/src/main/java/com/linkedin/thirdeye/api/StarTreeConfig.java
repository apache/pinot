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

public final class StarTreeConfig
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String collection;
  private final StarTreeRecordStoreFactory recordStoreFactory;
  private final StarTreeRecordThresholdFunction thresholdFunction;
  private final int maxRecordStoreEntries;
  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final String timeColumnName;

  private StarTreeConfig(String collection,
                         StarTreeRecordStoreFactory recordStoreFactory,
                         StarTreeRecordThresholdFunction thresholdFunction,
                         int maxRecordStoreEntries,
                         List<String> dimensionNames,
                         List<String> metricNames,
                         String timeColumnName)
  {
    this.collection = collection;
    this.recordStoreFactory = recordStoreFactory;
    this.thresholdFunction = thresholdFunction;
    this.maxRecordStoreEntries = maxRecordStoreEntries;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.timeColumnName = timeColumnName;
  }

  public String getCollection()
  {
    return collection;
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

  public String getTimeColumnName()
  {
    return timeColumnName;
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

    if (thresholdFunction != null)
    {
      json.put("thresholdFunctionClass", thresholdFunction.getClass().getCanonicalName());
      json.put("thresholdFunctionConfig", thresholdFunction.getConfig());
    }

    json.put("dimensionNames", dimensionNames);
    json.put("metricNames", metricNames);
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
    private String timeColumnName;
    private String thresholdFunctionClass;
    private Properties thresholdFunctionConfig;
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

    public String getThresholdFunctionClass()
    {
      return thresholdFunctionClass;
    }

    public Builder setThresholdFunctionClass(String thresholdFunctionClass)
    {
      this.thresholdFunctionClass = thresholdFunctionClass;
      return this;
    }

    public Properties getThresholdFunctionConfig()
    {
      return thresholdFunctionConfig;
    }

    public Builder setThresholdFunctionConfig(Properties thresholdFunctionConfig)
    {
      this.thresholdFunctionConfig = thresholdFunctionConfig;
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

      StarTreeRecordThresholdFunction tF = null;
      if (thresholdFunctionClass != null)
      {
        tF = (StarTreeRecordThresholdFunction) Class.forName(thresholdFunctionClass).newInstance();
        tF.init(thresholdFunctionConfig);
      }

      StarTreeRecordStoreFactory rF = (StarTreeRecordStoreFactory) Class.forName(recordStoreFactoryClass).newInstance();
      rF.init(dimensionNames, metricNames, recordStoreFactoryConfig);

      return new StarTreeConfig(collection, rF, tF, maxRecordStoreEntries, dimensionNames, metricNames, timeColumnName);
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

    // Get time column name
    String timeColumnName = jsonNode.get("timeColumnName").asText();

    // Build jsonNode
    StarTreeConfig.Builder starTreeConfig = new StarTreeConfig.Builder();
    starTreeConfig.setCollection(collection)
                  .setDimensionNames(dimensionNames)
                  .setMetricNames(metricNames)
                  .setTimeColumnName(timeColumnName);

    // Threshold function
    if (jsonNode.has("thresholdFunctionClass"))
    {
      starTreeConfig.setThresholdFunctionClass(jsonNode.get("thresholdFunctionClass").asText());
    }

    // Threshold function config
    Properties thresholdFunctionConfig = new Properties();
    if (jsonNode.has("thresholdFunctionConfig"))
    {
      Iterator<Map.Entry<String, JsonNode>> itr = jsonNode.get("thresholdFunctionConfig").fields();
      while (itr.hasNext())
      {
        Map.Entry<String, JsonNode> next = itr.next();
        thresholdFunctionConfig.put(next.getKey(), next.getValue().asText());
      }
      starTreeConfig.setThresholdFunctionConfig(thresholdFunctionConfig);
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

    return starTreeConfig.build();
  }
}
