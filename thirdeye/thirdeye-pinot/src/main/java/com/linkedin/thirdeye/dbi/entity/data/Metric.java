package com.linkedin.thirdeye.dbi.entity.data;

import com.linkedin.thirdeye.dbi.entity.base.AbstractJsonEntity;

public class Metric extends AbstractJsonEntity {
  String name;
  String collection;
  boolean derived;
  boolean dimensionAsMetric;

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public boolean isDerived() {
    return derived;
  }

  public void setDerived(boolean derived) {
    this.derived = derived;
  }

  public boolean isDimensionAsMetric() {
    return dimensionAsMetric;
  }

  public void setDimensionAsMetric(boolean dimensionAsMetric) {
    this.dimensionAsMetric = dimensionAsMetric;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
