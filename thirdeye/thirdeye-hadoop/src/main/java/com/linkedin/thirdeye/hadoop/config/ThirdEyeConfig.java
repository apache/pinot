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
package com.linkedin.thirdeye.hadoop.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.SplitSpec;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.api.TopKDimensionToMetricsSpec;
import com.linkedin.thirdeye.api.TopkWhitelistSpec;

public final class ThirdEyeConfig {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final String FIELD_SEPARATOR = ",";
  private static final String CONFIG_JOINER = ".";
  private static final String DEFAULT_TIME_TYPE = "HOURS";
  private static final String DEFAULT_TIME_SIZE = "1";

  private String collection;
  private List<DimensionSpec> dimensions;
  private List<MetricSpec> metrics;
  private TimeSpec time = new TimeSpec();
  private TopkWhitelistSpec topKWhitelist = new TopkWhitelistSpec();
  private SplitSpec split = new SplitSpec();

  public ThirdEyeConfig() {
  }

  public ThirdEyeConfig(String collection, List<DimensionSpec> dimensions,
      List<MetricSpec> metrics, TimeSpec time, TopkWhitelistSpec topKWhitelist, SplitSpec split) {
    this.collection = collection;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.time = time;
    this.topKWhitelist = topKWhitelist;
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

  public TopkWhitelistSpec getTopKWhitelist() {
    return topKWhitelist;
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
    private TopkWhitelistSpec topKWhitelist = new TopkWhitelistSpec();
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

    public TopkWhitelistSpec getTopKWhitelist() {
      return topKWhitelist;
    }

    public Builder setTopKWhitelist(TopkWhitelistSpec topKWhitelist) {
      this.topKWhitelist = topKWhitelist;
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

      return new ThirdEyeConfig(collection, dimensions, metrics, time, topKWhitelist, split);
    }
  }

  public static ThirdEyeConfig decode(InputStream inputStream) throws IOException {
    return OBJECT_MAPPER.readValue(inputStream, ThirdEyeConfig.class);
  }

  public static ThirdEyeConfig fromProperties(Properties props) {

    // collection
    String collection = getAndCheck(props,
        ThirdEyeConfigConstants.THIRDEYE_TABLE_NAME.toString());

    // dimensions
    String[] dimensionNames = getAndCheck(props,
        ThirdEyeConfigConstants.THIRDEYE_DIMENSION_NAMES.toString()).split(FIELD_SEPARATOR);
    List<DimensionSpec> dimensions = new ArrayList<>();
    for (String dimension : dimensionNames) {
      dimensions.add(new DimensionSpec(dimension));
    }

    // metrics
    String[] metricNames = getAndCheck(props,
        ThirdEyeConfigConstants.THIRDEYE_METRIC_NAMES.toString()).split(FIELD_SEPARATOR);
    String[] metricTypes = getAndCheck(props,
        ThirdEyeConfigConstants.THIRDEYE_METRIC_TYPES.toString()).split(FIELD_SEPARATOR);
    if (metricNames.length != metricTypes.length) {
      throw new IllegalStateException("Number of metric names provided "
          + "should be same as number of metric types");
    }
    List<MetricSpec> metrics = new ArrayList<>();
    for (int i = 0; i < metricNames.length; i++) {
      metrics.add(new MetricSpec(metricNames[i], MetricType.valueOf(metricTypes[i])));
    }

    // time
    String timeColumnName = getAndCheck(props,
        ThirdEyeConfigConstants.THIRDEYE_TIMECOLUMN_NAME.toString());
    String timeColumnType = getAndCheck(props,
        ThirdEyeConfigConstants.THIRDEYE_TIMECOLUMN_TYPE.toString(), DEFAULT_TIME_TYPE);
    String timeColumnSize = getAndCheck(props,
        ThirdEyeConfigConstants.THIRDEYE_TIMECOLUMN_SIZE.toString(), DEFAULT_TIME_SIZE);
    TimeGranularity timeGranularity = new TimeGranularity(Integer.parseInt(timeColumnSize), TimeUnit.valueOf(timeColumnType));
    TimeSpec time = new TimeSpec(timeColumnName, null, timeGranularity, null);

    // split
    String splitThreshold = getAndCheck(props,
        ThirdEyeConfigConstants.THIRDEYE_SPLIT_THRESHOLD.toString(), null);
    SplitSpec split = null;
    if (splitThreshold != null) {
      String splitOrder = getAndCheck(props,
          ThirdEyeConfigConstants.THIRDEYE_SPLIT_ORDER.toString(), null);
      List<String> splitOrderList = null;
      if (splitOrder != null) {
        splitOrderList = Arrays.asList(splitOrder.split(FIELD_SEPARATOR));
      }
      split = new SplitSpec(Integer.parseInt(splitThreshold), splitOrderList);
    }

    // topk
    TopkWhitelistSpec topKWhitelist = null;

    String thresholdMetricNames = getAndCheck(props, ThirdEyeConfigConstants.THIRDEYE_TOPK_THRESHOLD_METRIC_NAMES.toString(), null);
    String metricThresholdValues = getAndCheck(props, ThirdEyeConfigConstants.THIRDEYE_TOPK_METRIC_THRESHOLD_VALUES.toString(), null);
    Map<String, Double> threshold = null;
    if (thresholdMetricNames != null && metricThresholdValues != null) {
      String[] thresholdMetrics = thresholdMetricNames.split(FIELD_SEPARATOR);
      String[] thresholdValues = metricThresholdValues.split(FIELD_SEPARATOR);
      if (thresholdMetrics.length != thresholdValues.length) {
        throw new IllegalStateException("Number of threshold metric names should be same as threshold values");
      }
      threshold = new HashMap<>();
      for (int i = 0; i < thresholdMetrics.length; i++) {
        threshold.put(thresholdMetrics[i], Double.parseDouble(thresholdValues[i]));
      }
    }

    List<TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec = null;
    String topKDimensionNames = getAndCheck(props, ThirdEyeConfigConstants.THIRDEYE_TOPK_DIMENSION_NAMES.toString(), null);
    if (topKDimensionNames != null && topKDimensionNames.split(FIELD_SEPARATOR).length > 0) {
      topKDimensionToMetricsSpec = new ArrayList<>();
      for (String dimension : topKDimensionNames.split(FIELD_SEPARATOR)) {
        String[] topKDimensionMetrics = getAndCheck(props,
            ThirdEyeConfigConstants.THIRDEYE_TOPK_METRICS.toString() + CONFIG_JOINER + dimension)
            .split(FIELD_SEPARATOR);
        String[] topKDimensionKValues = getAndCheck(props,
            ThirdEyeConfigConstants.THIRDEYE_TOPK_KVALUES.toString() + CONFIG_JOINER + dimension)
            .split(FIELD_SEPARATOR);
        if (topKDimensionMetrics.length != topKDimensionKValues.length) {
          throw new IllegalStateException("Number of topk metric names and kvalues should be same for a dimension");
        }
        Map<String, Integer> topk = new HashMap<>();
        for (int i = 0; i < topKDimensionMetrics.length; i++) {
          topk.put(topKDimensionMetrics[i], Integer.parseInt(topKDimensionKValues[i]));
        }
        topKDimensionToMetricsSpec.add(new TopKDimensionToMetricsSpec(dimension, topk));
      }
    }

    Map<String, String> whitelist = null;
    String whitelistDimensions = getAndCheck(props, ThirdEyeConfigConstants.THIRDEYE_WHITELIST_DIMENSION_NAMES.toString(), null);
    if (whitelistDimensions != null && whitelistDimensions.split(FIELD_SEPARATOR).length > 0) {
      whitelist = new HashMap<>();
      for (String dimension : whitelistDimensions.split(FIELD_SEPARATOR)) {
        String dimensionWhitelist = getAndCheck(props,
            ThirdEyeConfigConstants.THIRDEYE_WHITELIST_DIMENSION.toString() + CONFIG_JOINER + dimension);
        whitelist.put(dimension, dimensionWhitelist);
      }
    }

    if (threshold != null || topKDimensionToMetricsSpec != null || whitelist != null) {
      topKWhitelist = new TopkWhitelistSpec();
      topKWhitelist.setThreshold(threshold);
      topKWhitelist.setTopKDimensionToMetricsSpec(topKDimensionToMetricsSpec);
      topKWhitelist.setWhitelist(whitelist);
    }

    return new ThirdEyeConfig(collection, dimensions, metrics, time, topKWhitelist, split);
  }

  private static String getAndCheck(Properties props, String propName) {
    String propValue = props.getProperty(propName);
    if (propValue == null) {
      throw new IllegalArgumentException(propName + " required property");
    }
    return propValue;
  }

  private static String getAndCheck(Properties props, String propName, String defaultValue) {
    String propValue = props.getProperty(propName, defaultValue);
    return propValue;
  }

}
