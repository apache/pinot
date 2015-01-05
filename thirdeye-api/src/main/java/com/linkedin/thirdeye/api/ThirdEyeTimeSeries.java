package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ThirdEyeTimeSeries
{
  @NotNull
  private String label;

  @NotNull
  private Map<String, String> dimensionValues;

  @NotNull
  private List<List<Number>> data = new ArrayList<List<Number>>();

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

  @JsonProperty
  public List<List<Number>> getData()
  {
    return data;
  }

  @JsonProperty
  public void setData(List<List<Number>> data)
  {
    this.data = data;
  }

  public void addRecord(Long time, Number value)
  {
    data.add(Arrays.asList(time, value));
  }

  // n.b. assumes data is sorted by time already
  public void normalize()
  {
    if (!data.isEmpty())
    {
      Double baseline = data.get(0).get(1).doubleValue();

      for (List<Number> datum : data)
      {
        Double scaledValue = datum.get(1).doubleValue() / baseline;
        datum.set(1, scaledValue);
      }
    }
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this).addValue(label).addValue(data).toString();
  }

  private static final Comparator<List<Number>> TIME_COMPARATOR = new Comparator<List<Number>>()
  {
    @Override
    public int compare(List<Number> o1, List<Number> o2)
    {
      return (int) (o1.get(0).longValue() - o2.get(0).longValue());
    }
  };
}
