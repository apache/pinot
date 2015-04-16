package com.linkedin.thirdeye.dashboard.views;

import io.dropwizard.views.View;

import java.util.List;

public class LandingView extends View {
  private final List<String> collections;

  public LandingView(List<String> collections) {
    super("landing.ftl");
    this.collections = collections;
  }

  public List<String> getCollections() {
    return collections;
  }
}
