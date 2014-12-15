package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Map;

public class ThirdEyeMetrics
{
  @NotEmpty
  private Map<String, String> dimensionValues;

  @NotEmpty
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
