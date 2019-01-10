/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
import com.linkedin.thirdeye.hadoop.config.DimensionSpec;
import com.linkedin.thirdeye.hadoop.config.MetricSpec;
import com.linkedin.thirdeye.hadoop.config.MetricType;
import com.linkedin.thirdeye.hadoop.config.SplitSpec;
import com.linkedin.thirdeye.hadoop.config.TimeGranularity;
import com.linkedin.thirdeye.hadoop.config.TimeSpec;
import com.linkedin.thirdeye.hadoop.config.TopKDimensionToMetricsSpec;
import com.linkedin.thirdeye.hadoop.config.TopkWhitelistSpec;

/**
 * This class represents the configs required by the thirdeye-hadoop jobs
 * @param collection - name of the pinot table
 * @param dimensions - list of dimensionSpecs for dimensions
 * @param metrics - list of metricSpecs for metrics
 * @param time - time spec
 * @topKWhitelist - metric threshold, topk and whitelist spec
 * @split - split spec
 */
public final class ThirdEyeConfig {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final String FIELD_SEPARATOR = ",";
  private static final String CONFIG_JOINER = ".";
  private static final String DEFAULT_TIME_TYPE = "HOURS";
  private static final String DEFAULT_TIME_SIZE = "1";
  private static final String DEFAULT_TIME_FORMAT = TimeFormat.EPOCH.toString();

  private String collection;
  private List<DimensionSpec> dimensions;
  private List<MetricSpec> metrics;
  private TimeSpec inputTime = new TimeSpec();
  private TimeSpec time = new TimeSpec();
  private TopkWhitelistSpec topKWhitelist = new TopkWhitelistSpec();
  private SplitSpec split = new SplitSpec();

  public ThirdEyeConfig() {
  }

  public ThirdEyeConfig(String collection, List<DimensionSpec> dimensions,
      List<MetricSpec> metrics, TimeSpec inputTime, TimeSpec time, TopkWhitelistSpec topKWhitelist, SplitSpec split) {
    this.collection = collection;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.inputTime = inputTime;
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

  public TimeSpec getInputTime() {
    return inputTime;
  }

  public TimeSpec getTime() {
    return time;
  }

  public TopkWhitelistSpec getTopKWhitelist() {
    return topKWhitelist;
  }

  /**
   * Returns a set of all dimensions which have either topk or whitelist config
   * @return
   */
  @JsonIgnore
  public Set<String> getTransformDimensions() {
    Set<String> transformDimensions = new HashSet<>();

    if (topKWhitelist != null) {
      List<TopKDimensionToMetricsSpec> topk = topKWhitelist.getTopKDimensionToMetricsSpec();
      if (topk != null) {
        for (TopKDimensionToMetricsSpec spec : topk) {
          transformDimensions.add(spec.getDimensionName());
        }
      }
    }
    return transformDimensions;
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
    private TimeSpec inputTime = new TimeSpec();
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

    public TimeSpec getInputTime() {
      return inputTime;
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

      return new ThirdEyeConfig(collection, dimensions, metrics, inputTime, time, topKWhitelist, split);
    }
  }

  public static ThirdEyeConfig decode(InputStream inputStream) throws IOException {
    return OBJECT_MAPPER.readValue(inputStream, ThirdEyeConfig.class);
  }

  /**
   * Creates a ThirdEyeConfig object from the Properties object
   * @param props
   * @return
   */
  public static ThirdEyeConfig fromProperties(Properties props) {

    String collection = getCollectionFromProperties(props);
    List<DimensionSpec> dimensions = getDimensionFromProperties(props);
    List<MetricSpec> metrics = getMetricsFromProperties(props);
    TimeSpec inputTime = getInputTimeFromProperties(props);
    TimeSpec time = getTimeFromProperties(props);
    SplitSpec split = getSplitFromProperties(props);
    TopkWhitelistSpec topKWhitelist = getTopKWhitelistFromProperties(props);
    ThirdEyeConfig thirdeyeConfig = new ThirdEyeConfig(collection, dimensions, metrics, inputTime, time, topKWhitelist, split);
    return thirdeyeConfig;
  }

  private static TopkWhitelistSpec getTopKWhitelistFromProperties(Properties props) {
    TopkWhitelistSpec topKWhitelist = null;

    Map<String, Double> threshold = getThresholdFromProperties(props);
    List<TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec = getTopKDimensionToMetricsSpecFromProperties(props);
    Map<String, List<String>> whitelist = getWhitelistFromProperties(props);
    Map<String, String> nonWhitelistValue = getNonWhitelistValueFromProperties(props);

    if (threshold != null || topKDimensionToMetricsSpec != null || whitelist != null) {
      topKWhitelist = new TopkWhitelistSpec();
      topKWhitelist.setThreshold(threshold);
      topKWhitelist.setTopKDimensionToMetricsSpec(topKDimensionToMetricsSpec);
      topKWhitelist.setWhitelist(whitelist);
      topKWhitelist.setNonWhitelistValue(nonWhitelistValue);
    }
    return topKWhitelist;
  }

  /**
   * Creates a map of dimension name to the value that should be used for "others"
   * @param props
   * @return
   */
  private static Map<String, String> getNonWhitelistValueFromProperties(Properties props) {
    Map<String, String> dimensionToNonWhitelistValueMap = null;

    // create dimension to type map
    List<DimensionSpec> dimensions = getDimensionFromProperties(props);
    Map<String, DimensionType> dimensionToType = new HashMap<>();
    for (int i = 0; i < dimensions.size(); i ++) {
      DimensionSpec spec = dimensions.get(i);
      dimensionToType.put(spec.getName(), spec.getDimensionType());
    }

    // dimensions with  whitelist
    String whitelistDimensionsStr = getAndCheck(props, ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION_NAMES.toString(), null);
    List<String> whitelistDimensions = new ArrayList<>();
    if (StringUtils.isNotBlank(whitelistDimensionsStr)) {
      dimensionToNonWhitelistValueMap = new HashMap<>();
      whitelistDimensions.addAll(Lists.newArrayList(whitelistDimensionsStr.split(FIELD_SEPARATOR)));
    }

    for (String whitelistDimension : whitelistDimensions) {
      String nonWhitelistValue = getAndCheck(props,
          ThirdEyeConfigProperties.THIRDEYE_NONWHITELIST_VALUE_DIMENSION.toString() + CONFIG_JOINER + whitelistDimension, null);
      if (StringUtils.isNotBlank(nonWhitelistValue)) {
        dimensionToNonWhitelistValueMap.put(whitelistDimension, nonWhitelistValue);
      } else {
        dimensionToNonWhitelistValueMap.put(whitelistDimension, String.valueOf(dimensionToType.get(whitelistDimension).getDefaultOtherValue()));
      }
    }
    return dimensionToNonWhitelistValueMap;
  }


  private static Map<String, List<String>> getWhitelistFromProperties(Properties props) {
 // create dimension to type map
    List<DimensionSpec> dimensions = getDimensionFromProperties(props);
    Map<String, DimensionType> dimensionToType = new HashMap<>();
    Map<String, Integer> dimensionToIndex = new HashMap<>();
    for (int i = 0; i < dimensions.size(); i ++) {
      DimensionSpec spec = dimensions.get(i);
      dimensionToType.put(spec.getName(), spec.getDimensionType());
      dimensionToIndex.put(spec.getName(), i);
    }

    Map<String, List<String>> whitelist = null;
    String whitelistDimensionsStr = getAndCheck(props, ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION_NAMES.toString(), null);
    if (whitelistDimensionsStr != null && whitelistDimensionsStr.split(FIELD_SEPARATOR).length > 0) {
      whitelist = new HashMap<>();
      for (String dimension : whitelistDimensionsStr.split(FIELD_SEPARATOR)) {
        String whitelistValuesStr = getAndCheck(props,
            ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION.toString() + CONFIG_JOINER + dimension);
        String[] whitelistValues = whitelistValuesStr.split(FIELD_SEPARATOR);
        List<String> whitelistValuesList = Lists.newArrayList(whitelistValues);
        whitelist.put(dimension, whitelistValuesList);
      }
    }
    return whitelist;
  }

  private static List<TopKDimensionToMetricsSpec> getTopKDimensionToMetricsSpecFromProperties(Properties props) {
    List<TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec = null;
    String topKDimensionNames = getAndCheck(props, ThirdEyeConfigProperties.THIRDEYE_TOPK_DIMENSION_NAMES.toString(), null);
    if (StringUtils.isNotEmpty(topKDimensionNames) && topKDimensionNames.split(FIELD_SEPARATOR).length > 0) {
      topKDimensionToMetricsSpec = new ArrayList<>();
      for (String dimension : topKDimensionNames.split(FIELD_SEPARATOR)) {
        String[] topKDimensionMetrics = getAndCheck(props,
            ThirdEyeConfigProperties.THIRDEYE_TOPK_METRICS.toString() + CONFIG_JOINER + dimension)
            .split(FIELD_SEPARATOR);
        String[] topKDimensionKValues = getAndCheck(props,
            ThirdEyeConfigProperties.THIRDEYE_TOPK_KVALUES.toString() + CONFIG_JOINER + dimension)
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
    return topKDimensionToMetricsSpec;
  }

  private static Map<String, Double> getThresholdFromProperties(Properties props) {
    Map<String, Double> threshold = null;
    String thresholdMetricNames = getAndCheck(props, ThirdEyeConfigProperties.THIRDEYE_TOPK_THRESHOLD_METRIC_NAMES.toString(), null);
    String metricThresholdValues = getAndCheck(props, ThirdEyeConfigProperties.THIRDEYE_TOPK_METRIC_THRESHOLD_VALUES.toString(), null);
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
    return threshold;
  }

  private static SplitSpec getSplitFromProperties(Properties props) {
    SplitSpec split = null;
    String splitThreshold = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_SPLIT_THRESHOLD.toString(), null);
    if (splitThreshold != null) {
      String splitOrder = getAndCheck(props,
          ThirdEyeConfigProperties.THIRDEYE_SPLIT_ORDER.toString(), null);
      List<String> splitOrderList = null;
      if (splitOrder != null) {
        splitOrderList = Arrays.asList(splitOrder.split(FIELD_SEPARATOR));
      }
      split = new SplitSpec(Integer.parseInt(splitThreshold), splitOrderList);
    }
    return split;
  }

  private static TimeSpec getTimeFromProperties(Properties props) {
    String timeColumnName = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_NAME.toString());
    String timeColumnType = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_TYPE.toString(), DEFAULT_TIME_TYPE);
    String timeColumnSize = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_SIZE.toString(), DEFAULT_TIME_SIZE);
    TimeGranularity timeGranularity = new TimeGranularity(Integer.parseInt(timeColumnSize), TimeUnit.valueOf(timeColumnType));
    String timeFormat = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_FORMAT.toString(), DEFAULT_TIME_FORMAT);
    TimeSpec time = new TimeSpec(timeColumnName, timeGranularity, timeFormat);
    return time;
  }


  private static TimeSpec getInputTimeFromProperties(Properties props) {
    TimeSpec inputTime = null;
    String timeColumnName = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_NAME.toString());
    String timeColumnType = getAndCheck(props,
          ThirdEyeConfigProperties.THIRDEYE_INPUT_TIMECOLUMN_TYPE.toString(), null);
    String timeColumnSize = getAndCheck(props,
          ThirdEyeConfigProperties.THIRDEYE_INPUT_TIMECOLUMN_SIZE.toString(), null);
    String timeFormat = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_INPUT_TIMECOLUMN_FORMAT.toString(), DEFAULT_TIME_FORMAT);
    if (timeColumnType != null && timeColumnSize != null) {
      TimeGranularity timeGranularity = new TimeGranularity(Integer.parseInt(timeColumnSize), TimeUnit.valueOf(timeColumnType));
      inputTime = new TimeSpec(timeColumnName, timeGranularity, timeFormat);
    }
    return inputTime;
  }

  private static List<MetricSpec> getMetricsFromProperties(Properties props) {
    List<MetricSpec> metrics = new ArrayList<>();
    String[] metricNames = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString()).split(FIELD_SEPARATOR);
    String[] metricTypes = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString()).split(FIELD_SEPARATOR);
    if (metricNames.length != metricTypes.length) {
      throw new IllegalStateException("Number of metric names provided "
          + "should be same as number of metric types");
    }
    for (int i = 0; i < metricNames.length; i++) {
      metrics.add(new MetricSpec(metricNames[i], MetricType.valueOf(metricTypes[i])));
    }
    return metrics;
  }

  private static List<DimensionSpec> getDimensionFromProperties(Properties props) {
    List<DimensionSpec> dimensions = new ArrayList<>();
    String[] dimensionNames = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_DIMENSION_NAMES.toString()).split(FIELD_SEPARATOR);
    String[] dimensionTypes = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_DIMENSION_TYPES.toString()).split(FIELD_SEPARATOR);
    for (int i = 0; i < dimensionNames.length; i++) {
      dimensions.add(new DimensionSpec(dimensionNames[i], DimensionType.valueOf(dimensionTypes[i])));
    }
    return dimensions;
  }

  private static String getCollectionFromProperties(Properties props) {
    String collection = getAndCheck(props,
        ThirdEyeConfigProperties.THIRDEYE_TABLE_NAME.toString());
    return collection;
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
