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

package org.apache.pinot.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.algorithm.DimensionWrapper;
import org.apache.pinot.thirdeye.detection.annotation.Yaml;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.spec.AbstractSpec;
import org.apache.pinot.thirdeye.detection.spi.components.Tunable;
import org.apache.pinot.thirdeye.detection.wrapper.AnomalyDetectorWrapper;
import org.apache.pinot.thirdeye.detection.wrapper.AnomalyFilterWrapper;
import org.apache.pinot.thirdeye.detection.wrapper.BaselineFillingMergeWrapper;
import org.apache.pinot.thirdeye.detection.wrapper.ChildKeepingMergeWrapper;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import static org.apache.pinot.thirdeye.detection.ConfigUtils.*;
import static org.apache.pinot.thirdeye.detection.DetectionUtils.*;


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
@Yaml(pipelineType = "COMPOSITE")
public class CompositePipelineConfigTranslator extends YamlDetectionConfigTranslator {
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
  private static final String DEFAULT_TIMEZONE = "America/Los_Angeles";
  private static final String DEFAULT_BASELINE_PROVIDER_YAML_TYPE = "RULE_BASELINE";
  private static final String PROP_BUCKET_PERIOD = "bucketPeriod";
  private static final String PROP_MAX_DURATION = "maxDuration";
  private static final String PROP_CACHE_PERIOD_LOOKBACK = "cachingPeriodLookback";

  private static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();
  static {
    // do not tune for alerts migrated from legacy anomaly function.
    DetectionRegistry.registerComponent("com.linkedin.thirdeye.detection.components.AdLibAlertFilter",
        "MIGRATED_ALGORITHM_FILTER");
    DetectionRegistry.registerComponent("com.linkedin.thirdeye.detection.components.AdLibAnomalyDetectorAndBaselineProvider",
        "MIGRATED_ALGORITHM");
  }
  private static final Set<String> TUNING_OFF_COMPONENTS =
      ImmutableSet.of("MIGRATED_ALGORITHM_FILTER", "MIGRATED_ALGORITHM", "MIGRATED_ALGORITHM_BASELINE");
  private static final Map<String, String> DETECTOR_TO_BASELINE =
      ImmutableMap.of("ALGORITHM", "ALGORITHM_BASELINE", "MIGRATED_ALGORITHM", "MIGRATED_ALGORITHM_BASELINE",
          "HOLT_WINTERS_RULE", "HOLT_WINTERS_RULE");
  private static final Set<String> MOVING_WINDOW_DETECTOR_TYPES = ImmutableSet.of("ALGORITHM", "MIGRATED_ALGORITHM");

  private final Map<String, Object> components = new HashMap<>();
  private final MetricConfigDTO metricConfig;
  private final DatasetConfigDTO datasetConfig;
  private final String metricUrn;
  private final Map<String, Object> mergerProperties;
  // metric dimension filter maps
  private final Map<String, Collection<String>> filterMaps;
  protected final org.yaml.snakeyaml.Yaml yaml;

  public CompositePipelineConfigTranslator(Map<String, Object> yamlConfig, DataProvider provider) {
    super(yamlConfig, provider);
    this.yaml = new org.yaml.snakeyaml.Yaml();
    this.metricConfig = this.dataProvider.fetchMetric(MapUtils.getString(yamlConfig, PROP_METRIC),
        MapUtils.getString(yamlConfig, PROP_DATASET));
    Preconditions.checkNotNull(this.metricConfig, "metric not found");

    this.datasetConfig = this.dataProvider.fetchDatasets(Collections.singletonList(metricConfig.getDataset()))
        .get(metricConfig.getDataset());
    Preconditions.checkNotNull(this.datasetConfig, "dataset not found");
    this.mergerProperties = MapUtils.getMap(yamlConfig, PROP_MERGER, new HashMap());
    this.filterMaps = MapUtils.getMap(yamlConfig, PROP_FILTERS);
    this.metricUrn = buildMetricUrn(filterMaps, this.metricConfig.getId());
  }

  @Override
  YamlTranslationResult translateYaml() {
    String detectionCronInYaml = MapUtils.getString(yamlConfig, PROP_CRON);
    String cron = (detectionCronInYaml == null) ? buildCron() : detectionCronInYaml;

    List<Map<String, Object>> ruleYamls = getList(yamlConfig.get(PROP_RULES));
    List<Map<String, Object>> nestedPipelines = new ArrayList<>();
    for (Map<String, Object> ruleYaml : ruleYamls) {
      List<Map<String, Object>> filterYamls = ConfigUtils.getList(ruleYaml.get(PROP_FILTER));
      List<Map<String, Object>> detectionYamls = ConfigUtils.getList(ruleYaml.get(PROP_DETECTION));
      List<Map<String, Object>> detectionProperties = buildListOfMergeWrapperProperties(detectionYamls);
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
    Map<String, Object> dimensionWrapperProperties = buildDimensionWrapperProperties();
    Map<String, Object> properties = buildWrapperProperties(ChildKeepingMergeWrapper.class.getName(),
        Collections.singletonList(
            buildWrapperProperties(DimensionWrapper.class.getName(), nestedPipelines, dimensionWrapperProperties)), this.mergerProperties);
    return new YamlTranslationResult().withProperties(properties).withComponents(this.components).withCron(cron);
  }

  private Map<String, Object> buildDimensionWrapperProperties() {
    Map<String, Object> dimensionWrapperProperties = new HashMap<>();
    dimensionWrapperProperties.put(PROP_NESTED_METRIC_URNS, Collections.singletonList(this.metricUrn));
    if (yamlConfig.containsKey(PROP_DIMENSION_EXPLORATION)) {
      Map<String, Object> dimensionExploreYaml = MapUtils.getMap(this.yamlConfig, PROP_DIMENSION_EXPLORATION);
      dimensionWrapperProperties.putAll(dimensionExploreYaml);
      if (dimensionExploreYaml.containsKey(PROP_DIMENSION_FILTER_METRIC)){
        MetricConfigDTO dimensionExploreMetric = this.dataProvider.fetchMetric(MapUtils.getString(dimensionExploreYaml, PROP_DIMENSION_FILTER_METRIC), this.datasetConfig.getDataset());
        dimensionWrapperProperties.put(PROP_METRIC_URN, buildMetricUrn(filterMaps, dimensionExploreMetric.getId()));
      } else {
        dimensionWrapperProperties.put(PROP_METRIC_URN, this.metricUrn);
      }
    }
    return dimensionWrapperProperties;
  }

  private List<Map<String, Object>> buildListOfMergeWrapperProperties(
      List<Map<String, Object>> yamlConfigs) {
    List<Map<String, Object>> properties = new ArrayList<>();
    for (Map<String, Object> yamlConfig : yamlConfigs) {
      properties.add(buildMergeWrapperProperties(yamlConfig));
    }
    return properties;
  }

  private Map<String, Object> buildMergeWrapperProperties(Map<String, Object> yamlConfig) {
    String detectorType = MapUtils.getString(yamlConfig, PROP_TYPE);
    String name = MapUtils.getString(yamlConfig, PROP_NAME);
    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_CLASS_NAME, AnomalyDetectorWrapper.class.getName());
    String detectorKey = makeComponentKey(detectorType, name);

    fillInDetectorWrapperProperties(nestedProperties, yamlConfig, detectorType);

    buildComponentSpec(yamlConfig, detectorType, detectorKey);

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, BaselineFillingMergeWrapper.class.getName());
    properties.put(PROP_NESTED, Collections.singletonList(nestedProperties));
    properties.put(PROP_DETECTOR, detectorKey);

    // fill in baseline provider properties
    if (DETECTION_REGISTRY.isBaselineProvider(detectorType)) {
      // if the detector implements the baseline provider interface, use it to generate baseline
      properties.put(PROP_BASELINE_PROVIDER, detectorKey);
    } else {
      String baselineProviderType = DEFAULT_BASELINE_PROVIDER_YAML_TYPE;
      if (DETECTOR_TO_BASELINE.containsKey(detectorType)) {
        baselineProviderType = DETECTOR_TO_BASELINE.get(detectorType);
      }
      String baselineProviderKey = makeComponentKey(baselineProviderType, name);
      buildComponentSpec(yamlConfig, baselineProviderType, baselineProviderKey);
      properties.put(PROP_BASELINE_PROVIDER, baselineProviderKey);
    }
    properties.putAll(this.mergerProperties);
    return properties;
  }

  // fill in window size and unit if detector requires this
  private void fillInDetectorWrapperProperties(Map<String, Object> properties, Map<String, Object> yamlConfig, String detectorType) {
    if (MOVING_WINDOW_DETECTOR_TYPES.contains(detectorType)) {
      properties.put(PROP_MOVING_WINDOW_DETECTION, true);
      switch (this.datasetConfig.bucketTimeGranularity().getUnit()) {
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
    properties.put(PROP_BUCKET_PERIOD, this.datasetConfig.bucketTimeGranularity().toPeriod().toString());
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
    String filterKey = makeComponentKey(filterType, name);
    wrapperProperties.put(PROP_FILTER, filterKey);
    buildComponentSpec(yamlConfig, filterType, filterKey);

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

  private String buildCron() {
    switch (this.datasetConfig.bucketTimeGranularity().getUnit()) {
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

  private String buildMetricUrn(Map<String, Collection<String>> filterMaps, long metricId) {
    Multimap<String, String> filters = ArrayListMultimap.create();
    if (filterMaps != null) {
      for (Map.Entry<String, Collection<String>> entry : filterMaps.entrySet()) {
        filters.putAll(entry.getKey(), entry.getValue());
      }
    }

    MetricEntity me = MetricEntity.fromMetric(1.0, metricId, filters);
    return me.getUrn();
  }

  private void buildComponentSpec(Map<String, Object> yamlConfig, String type, String componentKey) {
    String componentName = DetectionUtils.getComponentName(componentKey);
    String componentClassName = DETECTION_REGISTRY.lookup(type);
    Map<String, Object> componentSpecs = new HashMap<>();
    componentSpecs.put(PROP_CLASS_NAME, componentClassName);
    Map<String, Object> params = new HashMap<>();
    if (yamlConfig.containsKey(PROP_PARAMS)){
      params = MapUtils.getMap(yamlConfig, PROP_PARAMS);
    }

    if (!TUNING_OFF_COMPONENTS.contains(type) && DETECTION_REGISTRY.isTunable(componentClassName)) {
      try {
        componentSpecs.putAll(getTunedSpecs(componentName, componentClassName, params));
      } catch (Exception e) {
        LOG.error("Tuning failed for component " + type, e);
      }
    } else {
      componentSpecs.putAll(params);
    }
    this.components.put(componentName, componentSpecs);
  }

  private Map<String, Object> getTunedSpecs(String componentName, String componentClassName, Map<String, Object> params)
      throws Exception {
    long configId = this.existingConfig == null ? 0 : this.existingConfig.getId();
    InputDataFetcher dataFetcher = new DefaultInputDataFetcher(this.dataProvider, configId);
    Tunable tunable = getTunable(componentClassName, params, dataFetcher);

    // round to daily boundary
    DateTimeZone timezone = DateTimeZone.forID(this.datasetConfig.getTimezone() == null ? DEFAULT_TIMEZONE : this.datasetConfig.getTimezone());
    DateTime start = new DateTime(this.startTime, timezone).withTimeAtStartOfDay();
    DateTime end =  new DateTime(this.endTime, timezone).withTimeAtStartOfDay();
    Interval window = new Interval(start, end);
    Map<String, Object> existingComponentSpec =
        this.existingComponentSpecs.containsKey(componentName) ? MapUtils.getMap(this.existingComponentSpecs,
            componentName) : Collections.emptyMap();

    // TODO: if dimension drill down applied, pass in the metric urn of top dimension
    return tunable.tune(existingComponentSpec, window, this.metricUrn);
  }

  private Tunable getTunable(String componentClassName, Map<String, Object> params, InputDataFetcher dataFetcher)
      throws Exception {
    String tunableClassName = DETECTION_REGISTRY.lookupTunable(componentClassName);
    Class clazz = Class.forName(tunableClassName);
    Class<AbstractSpec> specClazz = (Class<AbstractSpec>) Class.forName(getSpecClassName(clazz));
    AbstractSpec spec = AbstractSpec.fromProperties(params, specClazz);
    Tunable tunable = (Tunable) clazz.newInstance();
    tunable.init(spec, dataFetcher);
    return tunable;
  }

  private String makeComponentKey(String type, String name) {
    return "$" + name + ":" + type;
  }

  @Override
  protected void validateYAML(Map<String, Object> yamlConfig) {
    super.validateYAML(yamlConfig);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_METRIC), "Property missing " + PROP_METRIC);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_DATASET), "Property missing " + PROP_DATASET);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_RULES), "Property missing " + PROP_RULES);
    Preconditions.checkArgument(!yamlConfig.containsKey(PROP_FILTER),
        "Please double check the filter config. Adding dimensions filters should be in the yaml root level using 'filters' as the key. Anomaly filter should be added in to the indentation level of detection yaml it applies to.");
    if (existingConfig != null) {
      Map<String, Object> existingYamlConfig = (Map<String, Object>) this.yaml.load(existingConfig.getYaml());
      Preconditions.checkArgument(MapUtils.getString(yamlConfig, PROP_METRIC).equals(MapUtils.getString(existingYamlConfig, PROP_METRIC)), "metric name cannot be modified");
      Preconditions.checkArgument(MapUtils.getString(yamlConfig, PROP_DATASET).equals(MapUtils.getString(existingYamlConfig, PROP_DATASET)), "dataset name cannot be modified");
    }

    // Safety condition: Validate if maxDuration is greater than 15 minutes
    Map<String, Object> mergerProperties = MapUtils.getMap(yamlConfig, PROP_MERGER, new HashMap());
    if (mergerProperties.get(PROP_MAX_DURATION) != null) {
      Preconditions.checkArgument(MapUtils.getLong(mergerProperties, PROP_MAX_DURATION) >= datasetConfig.bucketTimeGranularity().toMillis(),
          "The maxDuration field set is not acceptable. Please check the the document  and set it correctly.");
    }

    Set<String> names = new HashSet<>();
    List<Map<String, Object>> ruleYamls = getList(yamlConfig.get(PROP_RULES));
    for (int i = 0; i < ruleYamls.size(); i++) {
      Map<String, Object> ruleYaml = ruleYamls.get(i);
      Preconditions.checkArgument(ruleYaml.containsKey(PROP_DETECTION),
          "In rule No." + (i + 1) + ", detection rule is missing. ");
      List<Map<String, Object>> detectionStageYamls = ConfigUtils.getList(ruleYaml.get(PROP_DETECTION));
      // check each detection rule
      for (Map<String, Object> detectionStageYaml : detectionStageYamls) {
        Preconditions.checkArgument(detectionStageYaml.containsKey(PROP_TYPE),
            "In rule No." + (i + 1) + ", a detection rule type is missing. ");
        String type = MapUtils.getString(detectionStageYaml, PROP_TYPE);
        String name = MapUtils.getString(detectionStageYaml, PROP_NAME);
        Preconditions.checkNotNull(name, "In rule No." + (i + 1) + ", a detection rule name for type " +  type + " is missing");
        Preconditions.checkArgument(!names.contains(name), "In rule No." + (i + 1) +
            ", found duplicate rule name, rule name must be unique." );
        Preconditions.checkArgument(!name.contains(":"), "Sorry, rule name cannot contain \':\'");
      }
      if (ruleYaml.containsKey(PROP_FILTER)) {
        List<Map<String, Object>> filterStageYamls = ConfigUtils.getList(ruleYaml.get(PROP_FILTER));
        // check each filter rule
        for (Map<String, Object> filterStageYaml : filterStageYamls) {
          Preconditions.checkArgument(filterStageYaml.containsKey(PROP_TYPE),
              "In rule No." + (i + 1) + ", a filter rule type is missing. ");
          String type = MapUtils.getString(filterStageYaml, PROP_TYPE);
          String name = MapUtils.getString(filterStageYaml, PROP_NAME);
          Preconditions.checkNotNull(name, "In rule No." + (i + 1) + ", a filter rule name for type " + type + " is missing");
          Preconditions.checkArgument(!names.contains(name), "In rule No." + (i + 1) +
              ", found duplicate rule name, rule name must be unique." );
          Preconditions.checkArgument(!name.contains(":"), "Sorry, rule name cannot contain \':\'");
        }
      }
    }
  }
}
