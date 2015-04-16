package com.linkedin.thirdeye.dashboard.views;

import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import io.dropwizard.views.View;

public class DashboardStartView extends View {
  private final CollectionSchema collectionSchema;

  public DashboardStartView(CollectionSchema collectionSchema) {
    super("dashboard-start.ftl");
    this.collectionSchema = collectionSchema;
  }

  public CollectionSchema getCollectionSchema() {
    return collectionSchema;
  }
}
