package com.linkedin.thirdeye.dashboard.views;

import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.util.ViewUtils;
import io.dropwizard.views.View;

import java.util.HashMap;
import java.util.Map;

public class MetricViewTimeSeries extends View {
  private final CollectionSchema collectionSchema;
  private final Map<String, String> dimensionValues;
  private final Map<String, String> dimensionAliases;

  public MetricViewTimeSeries(CollectionSchema collectionSchema, Map<String, String> dimensionValues) {
    super("metric/time-series.ftl");
    this.collectionSchema = collectionSchema;
    this.dimensionValues = ViewUtils.fillDimensionValues(collectionSchema, dimensionValues);
    this.dimensionAliases = generateDimensionAliases();
  }

  public CollectionSchema getCollectionSchema() {
    return collectionSchema;
  }

  public Map<String, String> getDimensionValues() {
    return dimensionValues;
  }

  public Map<String, String> getDimensionAliases() {
    return dimensionAliases;
  }

  private Map<String, String> generateDimensionAliases() {
    Map<String, String> aliases = new HashMap<>();
    for (int i = 0; i < collectionSchema.getDimensions().size(); i++) {
      aliases.put(collectionSchema.getDimensions().get(i), collectionSchema.getDimensionAliases().get(i));
    }
    return aliases;
  }
}
