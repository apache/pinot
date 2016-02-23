package com.linkedin.thirdeye.dashboard.views;

import java.util.List;

import io.dropwizard.views.View;

public class DimensionViewFunnel extends View {

  private final List<FunnelTable> funnels;

  public DimensionViewFunnel(List<FunnelTable> funnels) {
    super("funnel-heatmap.ftl");
    this.funnels = funnels;
  }

  public List<FunnelTable> getFunnels() {
    return funnels;
  }
}
