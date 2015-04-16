package com.linkedin.thirdeye.dashboard.views;

import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import io.dropwizard.views.View;
import org.joda.time.DateTime;

import java.util.List;

public class DashboardView extends View {
  private final CollectionSchema collectionSchema;
  private final DateTime baselineTime;
  private final DateTime currentTime;
  private final MetricView metricView;
  private final DimensionView dimensionView;

  public DashboardView(CollectionSchema collectionSchema,
                       DateTime baselineTime,
                       DateTime currentTime,
                       MetricView metricView,
                       DimensionView dimensionView) {
    super("dashboard.ftl");
    this.collectionSchema = collectionSchema;
    this.baselineTime = baselineTime;
    this.currentTime = currentTime;
    this.metricView = metricView;
    this.dimensionView = dimensionView;
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
}
