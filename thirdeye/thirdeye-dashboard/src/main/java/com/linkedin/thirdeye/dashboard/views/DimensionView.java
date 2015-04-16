package com.linkedin.thirdeye.dashboard.views;

import com.linkedin.thirdeye.dashboard.api.DimensionViewType;
import io.dropwizard.views.View;

public class DimensionView {
  private final View view;
  private final DimensionViewType type;

  public DimensionView(View view, DimensionViewType type) {
    this.view = view;
    this.type = type;
  }

  public View getView() {
    return view;
  }

  public DimensionViewType getType() {
    return type;
  }
}
