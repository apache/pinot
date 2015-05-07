package com.linkedin.thirdeye.dashboard.views;

import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import io.dropwizard.views.View;
import org.joda.time.DateTime;

public class DashboardStartView extends View {
  private final CollectionSchema collectionSchema;
  private final DateTime earliestDataTime;
  private final DateTime latestDataTime;

  public DashboardStartView(CollectionSchema collectionSchema, DateTime earliestDataTime, DateTime latestDataTime) {
    super("dashboard-start.ftl");
    this.collectionSchema = collectionSchema;
    this.earliestDataTime = earliestDataTime;
    this.latestDataTime = latestDataTime;
  }

  public CollectionSchema getCollectionSchema() {
    return collectionSchema;
  }

  public DateTime getEarliestDataTime() {
    return earliestDataTime;
  }

  public DateTime getLatestDataTime() {
    return latestDataTime;
  }
}
