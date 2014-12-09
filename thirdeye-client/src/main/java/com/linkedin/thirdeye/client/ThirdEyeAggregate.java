package com.linkedin.thirdeye.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Map;

public class ThirdEyeAggregate
{
  private Map<String, String> dimensionValues;
  private Map<String, Integer> metricValues;

  @JsonProperty
  public Map<String, String> getDimensionValues()
  {
    return dimensionValues;
  }

  @JsonProperty
  public void setDimensionValues(Map<String, String> dimensionValues)
  {
    this.dimensionValues = dimensionValues;
  }

  @JsonProperty
  public Map<String, Integer> getMetricValues()
  {
    return metricValues;
  }

  @JsonProperty
  public void setMetricValues(Map<String, Integer> metricValues)
  {
    this.metricValues = metricValues;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this).addValue(dimensionValues).addValue(metricValues).toString();
  }
}
