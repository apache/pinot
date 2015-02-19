package com.linkedin.thirdeye.heatmap;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RGBColor
{
  private final int red;
  private final int green;
  private final int blue;

  public RGBColor(int red, int green, int blue)
  {
    this.red = red;
    this.green = green;
    this.blue = blue;
  }

  @JsonProperty
  public int getRed()
  {
    return red;
  }

  @JsonProperty
  public int getGreen()
  {
    return green;
  }

  @JsonProperty
  public int getBlue()
  {
    return blue;
  }
}
