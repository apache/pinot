package com.linkedin.thirdeye.dashboard.views;

import java.util.Collection;
import java.util.Map;

import com.linkedin.thirdeye.dashboard.api.DimensionViewType;

import io.dropwizard.views.View;

public class DimensionView {
  private final View view;
  private final DimensionViewType type;
  private final Map<String, Collection<String>> dimensionValuesOptions;

  public DimensionView(View view, DimensionViewType type,
      Map<String, Collection<String>> dimensionValuesOptions) {
    this.view = view;
    this.type = type;
    this.dimensionValuesOptions = dimensionValuesOptions;
  }

  public View getView() {
    return view;
  }

  public DimensionViewType getType() {
    return type;
  }

  public Map<String, Collection<String>> getDimensionValuesOptions() {
    return dimensionValuesOptions;
  }
}
