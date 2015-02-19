package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.validation.constraints.NotNull;
import java.util.Map;

public class ThirdEyeTimeSeries
{
  @NotNull
  private String label;

  @NotNull
  private Map<String, String> dimensionValues;

  @NotNull
  private Number[][] data;

  @JsonProperty
  public String getLabel()
  {
    return label;
  }

  @JsonProperty
  public void setLabel(String label)
  {
    this.label = label;
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

  public Number[][] getData()
  {
    return data;
  }

  public void setData(Number[][] data)
  {
    this.data = data;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this).addValue(label).addValue(data).toString();
  }
}
