package com.linkedin.thirdeye.anomaly.server.views;

import java.util.List;

import io.dropwizard.views.View;

/**
 *
 */
public class RootCollectionsView extends View {

  private final List<String> collections;

  public RootCollectionsView(List<String> collections) {
    super("root-collections-view.ftl");
    this.collections = collections;
  }

  public List<String> getCollections() {
    return collections;
  }

}
