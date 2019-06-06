/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.detection.yaml.translator;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.algorithm.DimensionWrapper;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.validators.DetectionConfigValidator;
import org.apache.pinot.thirdeye.detection.wrapper.AnomalyDetectorWrapper;
import org.apache.pinot.thirdeye.detection.wrapper.AnomalyFilterWrapper;
import org.apache.pinot.thirdeye.detection.wrapper.BaselineFillingMergeWrapper;
import org.apache.pinot.thirdeye.detection.wrapper.ChildKeepingMergeWrapper;
import org.apache.pinot.thirdeye.detection.wrapper.GrouperWrapper;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;

import static org.apache.pinot.thirdeye.detection.ConfigUtils.*;
import static org.apache.pinot.thirdeye.detection.yaml.DetectionConfigTuner.*;


/**
 * The YAML translator of composite pipeline. Convert YAML file into detection pipeline properties.
 * This pipeline supports multiple detection algorithm running in OR relationship.
 * Supports running multiple filter rule running in AND relationship.
 * Conceptually, the pipeline has the following structure. (Grouper not implemented yet).
 *
 *         +-----------+
 *         | Dimension |
 *         |Exploration|
 *         +-----+-----+
 *               |
 *         +-----v-----+
 *         |Dimension  |
 *         |Filter     |
 *       +-------------+--+
 *       |                |
 *       |                |
 * +-----v----+     +-----v----+
 * | Rule     |     | Algorithm|
 * | detection|     | Detection|
 * +-------------+  +-----+----+
 *       |                |
 * +-----v----+     +-----v----+
 * |Rule      |     |Algorithm |
 * |  Merger  |     |  Merger  |
 * +-----+----+     +-----+----+
 *       |                |
 *       v                v
 * +-----+----+     +-----+----+
 * |Rule      |     |Algorithm |
 * |Filters   |     |Filters   |
 * +-----+----+     +-----+----+
 *       +-------+--------+
 *               |
 *         +-----v-----+
 *         |   Merger  |
 *         |           |
 *         +-----+-----+
 *               |
 *               |
 *         +-----v-----+
 *         |  Grouper* |
 *         |           |
 *         +-----------+
 *
 *
 * This translator translates a yaml that describes the above detection flow
 * to the flowing wrapper structure to be execute in the detection pipeline.
 * +-----------------------------------------+
 * |  Merger                                 |
 * |  +--------------------------------------+
 * |  |   Dimension Exploration & Filter    ||
 * |  | +---------------------------------+ ||
 * |  | |        Filters                  | ||
 * |  | |  +--------Merger--------------+ | ||
 * |  | |  |  Rule detections           | | ||
 * |  | |  +----------------------------+ | ||
 * |  | +---------------------------------+ ||
 * |  | +---------------------------------+ ||
 * |  | |              Filters            | ||
 * |  | | +-----------Merger------------+ | ||
 * |  | | | +---------------------------| | ||
 * |  | | | | Algorithm detection       | | ||
 * |  | | | +---------------------------| | ||
 * |  | | +-----------------------------| | ||
 * |  | +---------------------------------+ ||
 * |  +--------------------------------------|
 * |  |--------------------------------------|
 * +-----------------------------------------+
 *
 */
public class CompositePipelineConfigTranslator extends DetectionConfigTranslator {
  private static final String PROP_DIMENSION_EXPLORATION = "dimensionExploration";

  private static final String PROP_DETECTION = "detection";
  private static final String PROP_CRON = "cron";
  private static final String PROP_FILTER = "filter";
  private static final String PROP_FILTERS = "filters";
  private static final String PROP_METRIC = "metric";
  private static final String PROP_DATASET = "dataset";
  private static final String PROP_TYPE = "type";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_PARAMS = "params";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_DIMENSION_FILTER_METRIC = "dimensionFilterMetric";
  private static final String PROP_NESTED_METRIC_URNS = "nestedMetricUrns";
  private static final String PROP_RULES = "rules";
  private static final String PROP_GROUPER = "grouper";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_BASELINE_PROVIDER = "baselineValueProvider";
  private static final String PROP_DETECTOR = "detector";
  private static final String PROP_MOVING_WINDOW_DETECTION = "isMovingWindowDetection";
  private static final String PROP_WINDOW_DELAY = "windowDelay";
  private static final String PROP_WINDOW_DELAY_UNIT = "windowDelayUnit";
  private static final String PROP_WINDOW_SIZE = "windowSize";
  private static final String PROP_WINDOW_UNIT = "windowUnit";
  private static final String PROP_FREQUENCY = "frequency";
  private static final String PROP_MERGER = "merger";
  private static final String PROP_TIMEZONE = "timezone";
  private static final String PROP_NAME = "name";
  private static final String DEFAULT_BASELINE_PROVIDER_YAML_TYPE = "RULE_BASELINE";
  private static final String PROP_BUCKET_PERIOD = "bucketPeriod";
  private static final String PROP_CACHE_PERIOD_LOOKBACK = "cachingPeriodLookback";

  private static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();
  static {
    // do not tune for alerts migrated from legacy anomaly function.
    DetectionRegistry.registerComponent("com.linkedin.thirdeye.detection.components.AdLibAlertFilter",
        "MIGRATED_ALGORITHM_FILTER");
    DetectionRegistry.registerComponent("com.linkedin.thirdeye.detection.components.AdLibAnomalyDetector",
        "MIGRATED_ALGORITHM");
  }
  private static final Set<String> MOVING_WINDOW_DETECTOR_TYPES = ImmutableSet.of("ALGORITHM", "MIGRATED_ALGORITHM");

  private final Map<String, Object> components = new HashMap<>();

  public CompositePipelineConfigTranslator(String yamlConfig, DataProvider provider) {
    this(yamlConfig, provider, new DetectionConfigValidator(provider));
  }

  public CompositePipelineConfigTranslator(String yamlConfig, DataProvider provider, DetectionConfigValidator validator) {
    super(yamlConfig, provider, validator);
  }

  @Override
  DetectionConfigDTO translateConfig() throws IllegalArgumentException {
    Map<String, Collection<String>> dimensionFiltersMap = MapUtils.getMap(yamlConfigMap, PROP_FILTERS);

    MetricConfigDTO metricConfig = this.dataProvider.fetchMetric(MapUtils.getString(yamlConfigMap, PROP_METRIC),
        MapUtils.getString(yamlConfigMap, PROP_DATASET));
    DatasetConfigDTO datasetConfig = this.dataProvider.fetchDatasets(Collections.singletonList(metricConfig.getDataset()))
        .get(metricConfig.getDataset());
    String metricUrn = MetricEntity.fromMetric(dimensionFiltersMap, metricConfig.getId()).getUrn();

    String detectionCronInYaml = MapUtils.getString(yamlConfigMap, PROP_CRON);
    String cron = (detectionCronInYaml == null) ? buildCron(datasetConfig.bucketTimeGranularity()) : detectionCronInYaml;
    Map<String, Object> mergerProperties = MapUtils.getMap(yamlConfigMap, PROP_MERGER, new HashMap<String, Object>());

    List<Map<String, Object>> ruleYamls = getList(yamlConfigMap.get(PROP_RULES));
    List<Map<String, Object>> nestedPipelines = new ArrayList<>();
    for (Map<String, Object> ruleYaml : ruleYamls) {
      List<Map<String, Object>> filterYamls = ConfigUtils.getList(ruleYaml.get(PROP_FILTER));
      List<Map<String, Object>> detectionYamls = ConfigUtils.getList(ruleYaml.get(PROP_DETECTION));
      List<Map<String, Object>> detectionProperties = buildListOfMergeWrapperProperties(detectionYamls, mergerProperties, datasetConfig.bucketTimeGranularity());
      if (filterYamls.isEmpty()) {
        nestedPipelines.addAll(detectionProperties);
      } else {
        List<Map<String, Object>> filterNestedProperties = detectionProperties;
        for (Map<String, Object> filterProperties : filterYamls) {
          filterNestedProperties = buildFilterWrapperProperties(AnomalyFilterWrapper.class.getName(), filterProperties,
              filterNestedProperties);
        }
        nestedPipelines.addAll(filterNestedProperties);
      }
    }
    Map<String, Object> dimensionWrapperProperties = buildDimensionWrapperProperties(dimensionFiltersMap, metricUrn, datasetConfig.getDataset());
    Map<String, Object> properties = buildWrapperProperties(
        ChildKeepingMergeWrapper.class.getName(),
        Collections.singletonList(buildWrapperProperties(DimensionWrapper.class.getName(), nestedPipelines, dimensionWrapperProperties)),
        mergerProperties);

    List<Map<String, Object>> grouperYamls = getList(yamlConfigMap.get(PROP_GROUPER));
    if (!grouperYamls.isEmpty()) {
      properties = buildGroupWrapperProperties(grouperYamls.get(0), properties);
    }

    return super.generateDetectionConfig(properties, this.components, cron);
  }

  private Map<String, Object> buildDimensionWrapperProperties(
      Map<String, Collection<String>> dimensionFilters, String metricUrn, String datasetName) {
    Map<String, Object> dimensionWrapperProperties = new HashMap<>();
    dimensionWrapperProperties.put(PROP_NESTED_METRIC_URNS, Collections.singletonList(metricUrn));
    if (yamlConfigMap.containsKey(PROP_DIMENSION_EXPLORATION)) {
      Map<String, Object> dimensionExploreYaml = MapUtils.getMap(this.yamlConfigMap, PROP_DIMENSION_EXPLORATION);
      dimensionWrapperProperties.putAll(dimensionExploreYaml);
      if (dimensionExploreYaml.containsKey(PROP_DIMENSION_FILTER_METRIC)){
        MetricConfigDTO dimensionExploreMetric = this.dataProvider.fetchMetric(MapUtils.getString(dimensionExploreYaml, PROP_DIMENSION_FILTER_METRIC), datasetName);
        dimensionWrapperProperties.put(PROP_METRIC_URN, MetricEntity.fromMetric(dimensionFilters, dimensionExploreMetric.getId()).getUrn());
      } else {
        dimensionWrapperProperties.put(PROP_METRIC_URN, metricUrn);
      }
    }
    return dimensionWrapperProperties;
  }

  private List<Map<String, Object>> buildListOfMergeWrapperProperties(
      List<Map<String, Object>> yamlConfigs, Map<String, Object> mergerProperties, TimeGranularity datasetTimegranularity) {
    List<Map<String, Object>> properties = new ArrayList<>();
    for (Map<String, Object> yamlConfig : yamlConfigs) {
      properties.add(buildMergeWrapperProperties(yamlConfig, mergerProperties, datasetTimegranularity));
    }
    return properties;
  }

  private Map<String, Object> buildMergeWrapperProperties(Map<String, Object> yamlConfig, Map<String, Object> mergerProperties, TimeGranularity datasetTimegranularity) {
    String detectorType = MapUtils.getString(yamlConfig, PROP_TYPE);
    String name = MapUtils.getString(yamlConfig, PROP_NAME);
    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_CLASS_NAME, AnomalyDetectorWrapper.class.getName());
    String detectorRefKey = makeComponentRefKey(detectorType, name);

    fillInDetectorWrapperProperties(nestedProperties, yamlConfig, detectorType, datasetTimegranularity);

    buildComponentSpec(yamlConfig, detectorType, detectorRefKey);

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, BaselineFillingMergeWrapper.class.getName());
    properties.put(PROP_NESTED, Collections.singletonList(nestedProperties));
    properties.put(PROP_DETECTOR, detectorRefKey);

    // fill in baseline provider properties
    if (DETECTION_REGISTRY.isBaselineProvider(detectorType)) {
      // if the detector implements the baseline provider interface, use it to generate baseline
      properties.put(PROP_BASELINE_PROVIDER, detectorRefKey);
    } else {
      String baselineProviderType = DEFAULT_BASELINE_PROVIDER_YAML_TYPE;
      String baselineProviderKey = makeComponentRefKey(baselineProviderType, name);
      buildComponentSpec(yamlConfig, baselineProviderType, baselineProviderKey);
      properties.put(PROP_BASELINE_PROVIDER, baselineProviderKey);
    }
    properties.putAll(mergerProperties);
    return properties;
  }

  private Map<String, Object> buildGroupWrapperProperties(Map<String, Object> grouperYaml, Map<String, Object> nestedProps) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, GrouperWrapper.class.getName());
    properties.put(PROP_NESTED, Collections.singletonList(nestedProps));

    String grouperType = MapUtils.getString(grouperYaml, PROP_TYPE);
    String grouperName = MapUtils.getString(grouperYaml, PROP_NAME);
    String grouperRefKey = makeComponentRefKey(grouperType, grouperName);
    properties.put(PROP_GROUPER, grouperRefKey);

    buildComponentSpec(grouperYaml, grouperType, grouperRefKey);

    return properties;
  }

  // fill in window size and unit if detector requires this
  private void fillInDetectorWrapperProperties(Map<String, Object> properties, Map<String, Object> yamlConfig, String detectorType, TimeGranularity datasetTimegranularity) {
    if (MOVING_WINDOW_DETECTOR_TYPES.contains(detectorType)) {
      properties.put(PROP_MOVING_WINDOW_DETECTION, true);
      switch (datasetTimegranularity.getUnit()) {
        case MINUTES:
          properties.put(PROP_WINDOW_SIZE, 6);
          properties.put(PROP_WINDOW_UNIT, TimeUnit.HOURS);
          properties.put(PROP_FREQUENCY, new TimeGranularity(15, TimeUnit.MINUTES));
          break;
        case HOURS:
          properties.put(PROP_WINDOW_SIZE, 24);
          properties.put(PROP_WINDOW_UNIT, TimeUnit.HOURS);
          break;
        case DAYS:
          properties.put(PROP_WINDOW_SIZE, 1);
          properties.put(PROP_WINDOW_UNIT, TimeUnit.DAYS);
          // TODO completeness checker true
          break;
        default:
          properties.put(PROP_WINDOW_SIZE, 6);
          properties.put(PROP_WINDOW_UNIT, TimeUnit.HOURS);
      }
    }
    // set default bucketPeriod
    properties.put(PROP_BUCKET_PERIOD, datasetTimegranularity.toPeriod().toString());
    // override from yaml
    if (yamlConfig.containsKey(PROP_WINDOW_SIZE)) {
      properties.put(PROP_MOVING_WINDOW_DETECTION, true);
      properties.put(PROP_WINDOW_SIZE, MapUtils.getString(yamlConfig, PROP_WINDOW_SIZE));
    }
    if (yamlConfig.containsKey(PROP_WINDOW_UNIT)) {
      properties.put(PROP_MOVING_WINDOW_DETECTION, true);
      properties.put(PROP_WINDOW_UNIT, MapUtils.getString(yamlConfig, PROP_WINDOW_UNIT));
    }
    if (yamlConfig.containsKey(PROP_WINDOW_DELAY)) {
      properties.put(PROP_WINDOW_DELAY, MapUtils.getString(yamlConfig, PROP_WINDOW_DELAY));
    }
    if (yamlConfig.containsKey(PROP_WINDOW_DELAY_UNIT)) {
      properties.put(PROP_WINDOW_DELAY_UNIT, MapUtils.getString(yamlConfig, PROP_WINDOW_DELAY_UNIT));
    }
    if (yamlConfig.containsKey(PROP_TIMEZONE)){
      properties.put(PROP_TIMEZONE, MapUtils.getString(yamlConfig, PROP_TIMEZONE));
    }
    if (yamlConfig.containsKey(PROP_BUCKET_PERIOD)){
      properties.put(PROP_BUCKET_PERIOD, MapUtils.getString(yamlConfig, PROP_BUCKET_PERIOD));
    }
    if (yamlConfig.containsKey(PROP_CACHE_PERIOD_LOOKBACK)) {
      properties.put(PROP_CACHE_PERIOD_LOOKBACK, MapUtils.getString(yamlConfig, PROP_CACHE_PERIOD_LOOKBACK));
    }
  }

  private List<Map<String, Object>> buildFilterWrapperProperties(String wrapperClassName,
      Map<String, Object> yamlConfig, List<Map<String, Object>> nestedProperties) {
    if (yamlConfig == null || yamlConfig.isEmpty()) {
      return nestedProperties;
    }
    Map<String, Object> wrapperProperties = buildWrapperProperties(wrapperClassName, nestedProperties);
    if (wrapperProperties.isEmpty()) {
      return Collections.emptyList();
    }
    String name = MapUtils.getString(yamlConfig, PROP_NAME);
    String filterType = MapUtils.getString(yamlConfig, PROP_TYPE);
    String filterRefKey = makeComponentRefKey(filterType, name);
    wrapperProperties.put(PROP_FILTER, filterRefKey);
    buildComponentSpec(yamlConfig, filterType, filterRefKey);

    return Collections.singletonList(wrapperProperties);
  }

  private Map<String, Object> buildWrapperProperties(String wrapperClassName,
      List<Map<String, Object>> nestedProperties) {
    return buildWrapperProperties(wrapperClassName, nestedProperties, Collections.emptyMap());
  }

  private Map<String, Object> buildWrapperProperties(String wrapperClassName,
      List<Map<String, Object>> nestedProperties, Map<String, Object> defaultProperties) {
    Map<String, Object> properties = new HashMap<>();
    List<Map<String, Object>> wrapperNestedProperties = new ArrayList<>();
    for (Map<String, Object> nested : nestedProperties) {
      if (nested != null && !nested.isEmpty()) {
        wrapperNestedProperties.add(nested);
      }
    }
    if (wrapperNestedProperties.isEmpty()) {
      return properties;
    }
    properties.put(PROP_CLASS_NAME, wrapperClassName);
    properties.put(PROP_NESTED, wrapperNestedProperties);
    properties.putAll(defaultProperties);
    return properties;
  }

  //  Default schedule:
  //  minute granularity: every 15 minutes, starts at 0 minute
  //  hourly: every hour, starts at 0 minute
  //  daily: every day, starts at 2 pm UTC
  //  others: every day, start at 12 am UTC
  private String buildCron(TimeGranularity timegranularity) {
    switch (timegranularity.getUnit()) {
      case MINUTES:
        return "0 0/15 * * * ? *";
      case HOURS:
        return "0 0 * * * ? *";
      case DAYS:
        return "0 0 14 * * ? *";
      default:
        return "0 0 0 * * ?";
    }
  }

  private void buildComponentSpec(Map<String, Object> yamlConfig, String type, String componentRefKey) {
    Map<String, Object> componentSpecs = new HashMap<>();

    String componentClassName = DETECTION_REGISTRY.lookup(type);
    componentSpecs.put(PROP_CLASS_NAME, componentClassName);

    Map<String, Object> params = new HashMap<>();
    if (yamlConfig.containsKey(PROP_PARAMS)){
      params = MapUtils.getMap(yamlConfig, PROP_PARAMS);
    }

    // For tunable components, the model params are computed from user supplied yaml params and previous model params.
    // We store the yaml params under a separate key, PROP_YAML_PARAMS, to distinguish from model params.
    if (!TURNOFF_TUNING_COMPONENTS.contains(type) && DETECTION_REGISTRY.isTunable(componentClassName)) {
      componentSpecs.put(PROP_YAML_PARAMS, params);
    } else {
      componentSpecs.putAll(params);
    }

    this.components.put(DetectionUtils.getComponentKey(componentRefKey), componentSpecs);
  }

  private String makeComponentRefKey(String type, String name) {
    return "$" + name + ":" + type;
  }
}
