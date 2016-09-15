package com.linkedin.thirdeye.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class CollectionSchema extends AbstractConfig {

  private String collection;
  private List<DimensionSpec> dimensions;
  private List<MetricSpec> metrics;

  private TimeSpec time;

  public CollectionSchema() {
  }

  public CollectionSchema(String collection, List<DimensionSpec> dimensions, TimeSpec time, List<MetricSpec> metrics) {
    this.collection = collection;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.time = time;
  }

  public String getCollection() {
    return collection;
  }

  public List<DimensionSpec> getDimensions() {
    return dimensions;
  }

  @JsonIgnore
  public List<String> getDimensionNames() {
    List<String> results = new ArrayList<>(dimensions.size());
    for (DimensionSpec dimensionSpec : dimensions) {
      results.add(dimensionSpec.getName());
    }
    return results;
  }

  public List<MetricSpec> getMetrics() {
    return metrics;
  }

  @JsonIgnore
  public List<String> getMetricNames() {
    List<String> results = new ArrayList<>(metrics.size());
    for (MetricSpec metricSpec : metrics) {
      results.add(metricSpec.getName());
    }
    return results;
  }

  public TimeSpec getTime() {
    return time;
  }

  public String encode() throws IOException {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public static class Builder {
    private String collection;
    private List<DimensionSpec> dimensions;
    private List<MetricSpec> metrics;
    private TimeSpec time = new TimeSpec();

    public String getCollection() {
      return collection;
    }

    public Builder setCollection(String collection) {
      this.collection = collection;
      return this;
    }

    public List<DimensionSpec> getDimensions() {
      return dimensions;
    }

    public Builder setDimensions(List<DimensionSpec> dimensions) {
      this.dimensions = dimensions;
      return this;
    }

    public List<MetricSpec> getMetrics() {
      return metrics;
    }

    public Builder setMetrics(List<MetricSpec> metrics) {
      this.metrics = metrics;
      return this;
    }

    public TimeSpec getTime() {
      return time;
    }

    public Builder setTime(TimeSpec time) {
      this.time = time;
      return this;
    }

    public CollectionSchema build() throws Exception {
      if (collection == null) {
        throw new IllegalArgumentException("Must provide collection");
      }

      if (dimensions == null || dimensions.isEmpty()) {
        throw new IllegalArgumentException("Must provide dimension names");
      }

      if (metrics == null || metrics.isEmpty()) {
        throw new IllegalArgumentException("Must provide metric specs");
      }

      return new CollectionSchema(collection, dimensions, time, metrics);
    }
  }

  public static CollectionSchema decode(InputStream inputStream) throws IOException {
    return OBJECT_MAPPER.readValue(inputStream, CollectionSchema.class);
  }

  @Override
  public String toJSON() throws Exception {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  @Override
  public String getConfigName() {
    return collection;
  }

}
