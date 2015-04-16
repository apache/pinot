package com.linkedin.thirdeye.dashboard.views;

import com.linkedin.thirdeye.dashboard.api.MetricViewType;
import io.dropwizard.views.View;

public class MetricView {
  private final View view;
  private final MetricViewType type;

  public MetricView(View view, MetricViewType type) {
    this.view = view;
    this.type = type;
  }

  public View getView() {
    return view;
  }

  public MetricViewType getType() {
    return type;
  }
}
