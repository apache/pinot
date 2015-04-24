package com.linkedin.thirdeye.dashboard.views;

import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.util.ViewUtils;
import io.dropwizard.views.View;

import java.util.Map;

public class MetricViewFlot extends View {
  private final CollectionSchema collectionSchema;
  private final Map<String, String> dimensionValues;

  public MetricViewFlot(CollectionSchema collectionSchema, Map<String, String> dimensionValues) {
    super("metric/funnel.ftl");
    this.collectionSchema = collectionSchema;
    this.dimensionValues = ViewUtils.fillDimensionValues(collectionSchema, dimensionValues);
  }

  public CollectionSchema getCollectionSchema() {
    return collectionSchema;
  }

  public Map<String, String> getDimensionValues() {
    return dimensionValues;
  }
}
