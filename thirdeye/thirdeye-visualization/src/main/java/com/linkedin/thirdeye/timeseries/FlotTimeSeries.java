package com.linkedin.thirdeye.timeseries;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class FlotTimeSeries
{
  private final String metricName;
  private final String label;
  private final Map<String, String> dimensionValues;
  private final Number[][] data;
  private final Long start;
  private final Long end;

  public FlotTimeSeries(String metricName,
                        String label,
                        Map<String, String> dimensionValues,
                        Number[][] data,
                        Long start,
                        Long end)
  {
    this.metricName = metricName;
    this.label = label;
    this.dimensionValues = dimensionValues;
    this.data = data;
    this.start = start;
    this.end = end;
  }

  @JsonProperty
  public String getMetricName()
  {
    return metricName;
  }

  @JsonProperty
  public String getLabel()
  {
    return label;
  }

  @JsonProperty
  public Map<String, String> getDimensionValues()
  {
    return dimensionValues;
  }

  @JsonProperty
  public Number[][] getData()
  {
    return data;
  }

  @JsonProperty
  public Long getStart()
  {
    return start;
  }

  @JsonProperty
  public Long getEnd()
  {
    return end;
  }
}
