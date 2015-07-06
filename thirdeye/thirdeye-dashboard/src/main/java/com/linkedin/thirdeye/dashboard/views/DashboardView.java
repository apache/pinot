package com.linkedin.thirdeye.dashboard.views;

import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import io.dropwizard.views.View;
import org.joda.time.DateTime;

import java.util.List;

public class DashboardView extends View {
  private final String collection;
  private final CollectionSchema collectionSchema;
  private final DateTime baselineTime;
  private final DateTime currentTime;
  private final MetricView metricView;
  private final DimensionView dimensionView;
  private final DateTime earliestDataTime;
  private final DateTime latestDataTime;
  private final List<String> customDashboardNames;

  public DashboardView(String collection,
                       CollectionSchema collectionSchema,
                       DateTime baselineTime,
                       DateTime currentTime,
                       MetricView metricView,
                       DimensionView dimensionView,
                       DateTime earliestDataTime,
                       DateTime latestDataTime,
                       List<String> customDashboardNames) {
    super("dashboard.ftl");
    this.collection = collection;
    this.collectionSchema = collectionSchema;
    this.baselineTime = baselineTime;
    this.currentTime = currentTime;
    this.metricView = metricView;
    this.dimensionView = dimensionView;
    this.earliestDataTime = earliestDataTime;
    this.latestDataTime = latestDataTime;
    this.customDashboardNames = customDashboardNames;
  }

  public String getCollection() {
    return collection;
  }

  public CollectionSchema getCollectionSchema() {
    return collectionSchema;
  }

  public DateTime getBaselineTime() {
    return baselineTime;
  }

  public DateTime getCurrentTime() {
    return currentTime;
  }

  public MetricView getMetricView() {
    return metricView;
  }

  public DimensionView getDimensionView() {
    return dimensionView;
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
