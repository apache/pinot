/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.thirdeye.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.SplitSpec;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.api.TopKRollupSpec;

public final class ThirdEyeConfig {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  private String collection;
  private List<DimensionSpec> dimensions;
  private List<MetricSpec> metrics;
  private TimeSpec time = new TimeSpec();
  private TopKRollupSpec topKRollup = new TopKRollupSpec();

  private SplitSpec split = new SplitSpec();

  public ThirdEyeConfig() {
  }

  public ThirdEyeConfig(String collection, List<DimensionSpec> dimensions,
      List<MetricSpec> metrics, TimeSpec time, TopKRollupSpec topKRollup, SplitSpec split) {
    this.collection = collection;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.time = time;
    this.topKRollup = topKRollup;
    this.split = split;
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

  public TopKRollupSpec getTopKRollup() {
    return topKRollup;
  }

  public SplitSpec getSplit() {
    return split;
  }

  public String encode() throws IOException {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public static class Builder {
    private String collection;
    private List<DimensionSpec> dimensions;
    private List<MetricSpec> metrics;
    private TimeSpec time = new TimeSpec();
    private TopKRollupSpec topKRollup = new TopKRollupSpec();
    private SplitSpec split = new SplitSpec();

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

    public TopKRollupSpec getTopKRollup() {
      return topKRollup;
    }

    public Builder setTopKRollup(TopKRollupSpec topKRollup) {
      this.topKRollup = topKRollup;
      return this;
    }

    public SplitSpec getSplit() {
      return split;
    }

    public Builder setSplit(SplitSpec split) {
      this.split = split;
      return this;
    }

    public ThirdEyeConfig build() throws Exception {
      if (collection == null) {
        throw new IllegalArgumentException("Must provide collection");
      }

      if (dimensions == null || dimensions.isEmpty()) {
        throw new IllegalArgumentException("Must provide dimension names");
      }

      if (metrics == null || metrics.isEmpty()) {
        throw new IllegalArgumentException("Must provide metric specs");
      }

      return new ThirdEyeConfig(collection, dimensions, metrics, time, topKRollup, split);
    }
  }

  public static ThirdEyeConfig decode(InputStream inputStream) throws IOException {
    return OBJECT_MAPPER.readValue(inputStream, ThirdEyeConfig.class);
  }

}
