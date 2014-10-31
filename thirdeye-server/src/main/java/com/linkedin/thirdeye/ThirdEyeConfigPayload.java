package com.linkedin.thirdeye;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;
import java.util.Map;

public class ThirdEyeConfigPayload
{
  @NotEmpty
  private String collection;

  @NotEmpty
  private List<String> dimensionNames;

  @NotEmpty
  private List<String> metricNames;

  @NotEmpty
  private Integer maxRecordStoreEntries = 10000;

  private String thresholdFunctionClass;

  private Map<String, String> thresholdFunctionConfig;

  @NotEmpty
  private String rootUri;

  @JsonProperty
  public String getCollection()
  {
    return collection;
  }

  public void setCollection(String collection)
  {
    this.collection = collection;
  }

  @JsonProperty
  public List<String> getDimensionNames()
  {
    return dimensionNames;
  }

  public void setDimensionNames(List<String> dimensionNames)
  {
    this.dimensionNames = dimensionNames;
  }

  @JsonProperty
  public List<String> getMetricNames()
  {
    return metricNames;
  }

  public void setMetricNames(List<String> metricNames)
  {
    this.metricNames = metricNames;
  }

  @JsonProperty
  public Integer getMaxRecordStoreEntries()
  {
    return maxRecordStoreEntries;
  }

  public void setMaxRecordStoreEntries(Integer maxRecordStoreEntries)
  {
    this.maxRecordStoreEntries = maxRecordStoreEntries;
  }

  @JsonProperty
  public String getThresholdFunctionClass()
  {
    return thresholdFunctionClass;
  }

  public void setThresholdFunctionClass(String thresholdFunctionClass)
  {
    this.thresholdFunctionClass = thresholdFunctionClass;
  }

  @JsonProperty
  public Map<String, String> getThresholdFunctionConfig()
  {
    return thresholdFunctionConfig;
  }

  public void setThresholdFunctionConfig(Map<String, String> thresholdFunctionConfig)
  {
    this.thresholdFunctionConfig = thresholdFunctionConfig;
  }

  @JsonProperty
  public String getRootUri()
  {
    return rootUri;
  }

  public void setRootUri(String rootUri)
  {
    this.rootUri = rootUri;
  }
}
