package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public class ThirdEyeTimeSeries
{
  @NotNull
  private String metricName;

  @NotEmpty
  private Map<String, String> dimensionValues;

  @NotNull
  private List<List<Long>> timeSeries;

  @JsonProperty
  public String getMetricName()
  {
    return metricName;
  }

  @JsonProperty
  public void setMetricName(String metricName)
  {
    this.metricName = metricName;
  }

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
  public List<List<Long>> getTimeSeries()
  {
    return timeSeries;
  }

  @JsonProperty
  public void setTimeSeries(List<List<Long>> timeSeries)
  {
    this.timeSeries = timeSeries;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this).addValue(metricName).addValue(dimensionValues).addValue(timeSeries).toString();
  }
}
