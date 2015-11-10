package com.linkedin.thirdeye.dashboard.views;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.dashboard.api.CollectionSchema;

import io.dropwizard.views.View;

public class DashboardStartView extends View {
  private final String collection;
  private final CollectionSchema collectionSchema;
  private final DateTime earliestDataTime;
  private final DateTime latestDataTime;
  private final List<String> customDashboardNames;
  private final List<String> funnelNames;

  public DashboardStartView(String collection, CollectionSchema collectionSchema,
      DateTime earliestDataTime, DateTime latestDataTime, List<String> customDashboardNames) {
    super("dashboard-start.ftl");
    this.collection = collection;
    this.collectionSchema = collectionSchema;
    this.earliestDataTime = earliestDataTime;
    this.latestDataTime = latestDataTime;
    this.customDashboardNames = customDashboardNames;
    this.funnelNames = new ArrayList<String>();
  }

  public DashboardStartView(String collection, CollectionSchema collectionSchema,
      DateTime earliestDataTime, DateTime latestDataTime, List<String> customDashboardNames,
      List<String> funnelNames) {
    super("dashboard-start.ftl");
    this.collection = collection;
    this.collectionSchema = collectionSchema;
    this.earliestDataTime = earliestDataTime;
    this.latestDataTime = latestDataTime;
    this.customDashboardNames = customDashboardNames;
    this.funnelNames = funnelNames;
  }

  public String getCollection() {
    return collection;
  }

  public List<String> getFunnelNames() {
    return funnelNames;
  }

  public List<String> getMetrics() {
    return collectionSchema.getMetrics();
  }

  public List<String> getMetricAliases() {
    return collectionSchema.getMetricAliases();
  }

  public DateTime getEarliestDataTime() {
    return earliestDataTime;
  }

  public DateTime getLatestDataTime() {
    return latestDataTime;
  }

  public List<String> getCustomDashboardNames() {
    return customDashboardNames;
  }
}
