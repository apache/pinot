package com.linkedin.thirdeye.dashboard.views;

import com.linkedin.thirdeye.dashboard.api.custom.CustomDashboardComponentSpec;
import io.dropwizard.views.View;
import org.apache.commons.math3.util.Pair;

import java.util.List;

public class CustomDashboardView extends View {
  private final String name;
  private final List<Pair<CustomDashboardComponentSpec, View>> componentViews;

  public CustomDashboardView(String name, List<Pair<CustomDashboardComponentSpec, View>> componentViews) {
    super("custom-dashboard.ftl");
    this.name = name;
    this.componentViews = componentViews;
  }

  public String getName() {
    return name;
  }

  public List<Pair<CustomDashboardComponentSpec, View>> getComponentViews() {
    return componentViews;
  }
}
