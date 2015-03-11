package com.linkedin.thirdeye.funnel;

import java.util.List;
import java.util.Map;

public class Funnel
{
  public enum Type
  {
    TOP,
    PREVIOUS
  }

  private final Map<String, String> dimensionValues;
  private final List<FunnelRow> rows;

  public Funnel(Map<String, String> dimensionValues, List<FunnelRow> rows)
  {
    this.dimensionValues = dimensionValues;
    this.rows = rows;
  }

  public Map<String, String> getDimensionValues()
  {
    return dimensionValues;
  }

  public List<FunnelRow> getRows()
  {
    return rows;
  }
}
