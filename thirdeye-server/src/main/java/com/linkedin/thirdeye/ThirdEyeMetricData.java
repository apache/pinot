package com.linkedin.thirdeye;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Map;

public class ThirdEyeMetricData
{
  @NotEmpty
  private Map<String, String> dimensionValues;

  @NotEmpty
  private Map<String, Long> metricValues;

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
}
