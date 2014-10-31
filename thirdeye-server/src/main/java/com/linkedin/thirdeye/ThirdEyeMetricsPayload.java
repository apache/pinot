package com.linkedin.thirdeye;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Map;

public class ThirdEyeMetricsPayload
{
  @NotEmpty
  private String collection;

  @NotEmpty
  private Map<String, String> dimensionValues;

  @NotEmpty
  private Map<String, Long> metricValues;

  @NotEmpty
  private Long time;

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
  public Map<String, String> getDimensionValues()
  {
    return dimensionValues;
  }

  public void setDimensionValues(Map<String, String> dimensionValues)
  {
    this.dimensionValues = dimensionValues;
  }

  @JsonProperty
  public Map<String, Long> getMetricValues()
  {
    return metricValues;
  }

  public void setMetricValues(Map<String, Long> metricValues)
  {
    this.metricValues = metricValues;
  }

  @JsonProperty
  public Long getTime()
  {
    return time;
  }

  public void setTime(Long time)
  {
    this.time = time;
  }
}
