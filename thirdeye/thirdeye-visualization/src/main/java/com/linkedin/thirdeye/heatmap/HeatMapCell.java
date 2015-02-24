package com.linkedin.thirdeye.heatmap;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HeatMapCell
{
  private String dimensionValue;
  private Number current;
  private Number baseline;
  private String label;
  private double ratio;
  private double alpha;
  private RGBColor color;

  public HeatMapCell() {}

  public HeatMapCell(String dimensionValue,
                     Number current,
                     Number baseline,
                     String label,
                     double ratio,
                     double alpha,
                     RGBColor color)
  {
    this.dimensionValue = dimensionValue;
    this.current = current;
    this.baseline = baseline;
    this.label = label;
    this.ratio = ratio;
    this.alpha = alpha;
    this.color = color;
  }

  @JsonProperty
  public String getDimensionValue()
  {
    return dimensionValue;
  }

  @JsonProperty
  public Number getCurrent()
  {
    return current;
  }

  @JsonProperty
  public Number getBaseline()
  {
    return baseline;
  }

  @JsonProperty
  public String getLabel()
  {
    if (label == null)
    {
      return String.format("(baseline=%s, current=%s)", baseline, current);
    }
    return label;
  }

  @JsonProperty
  public double getRatio()
  {
    return ratio;
  }

  @JsonProperty
  public double getAlpha()
  {
    return alpha;
  }

  @JsonProperty
  public RGBColor getColor()
  {
    return color;
  }

  public void setDimensionValue(String dimensionValue)
  {
    this.dimensionValue = dimensionValue;
  }

  public void setCurrent(Number current)
  {
    this.current = current;
  }

  public void setBaseline(Number baseline)
  {
    this.baseline = baseline;
  }

  public void setRatio(double ratio)
  {
    this.ratio = ratio;
  }

  public void setAlpha(double alpha)
  {
    this.alpha = alpha;
  }

  public void setColor(RGBColor color)
  {
    this.color = color;
  }
}
