package com.linkedin.thirdeye.dashboard.views;

import io.dropwizard.views.View;

import java.util.List;

public class DimensionViewMultiTimeSeries extends View {
  private final List<String> dimensions;

  public DimensionViewMultiTimeSeries(List<String> dimensions) {
    super("dimension/multi-time-series.ftl");
    this.dimensions = dimensions;
  }

  public List<String> getDimensions() {
    return dimensions;
  }
}
