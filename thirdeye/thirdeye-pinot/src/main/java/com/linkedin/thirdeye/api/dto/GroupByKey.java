package com.linkedin.thirdeye.api.dto;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class GroupByKey {
  Long functionId;
  String dimensions;
  String functionName;
  String collection;
  String metric;

  public Long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(long functionId) {
    this.functionId = functionId;
  }

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimension) {
    this.dimensions = dimension;
  }

  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String dataset) {
    this.collection = dataset;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dimensions", dimensions)
        .add("collection", collection).add("metric", metric).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupByKey that = (GroupByKey) o;
    return Objects.equal(functionId, that.functionId) &&
        Objects.equal(dimensions, that.dimensions) &&
        Objects.equal(functionName, that.functionName) &&
        Objects.equal(collection, that.collection) &&
        Objects.equal(metric, that.metric);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(functionId, dimensions, functionName, collection, metric);
  }
}
