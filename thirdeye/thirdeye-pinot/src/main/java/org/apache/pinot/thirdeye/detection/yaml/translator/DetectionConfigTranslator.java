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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
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
import org.apache.pinot.thirdeye.detection.wrapper.EntityAnomalyMergeWrapper;
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
public class DetectionConfigTranslator extends ConfigTranslator<DetectionConfigDTO, DetectionConfigValidator> {
  public static final String PROP_SUB_ENTITY_NAME = "subEntityName";

  public static final String PROP_DIMENSION_EXPLORATION = "dimensionExploration";
  public static final String PROP_FILTERS = "filters";

  private static final String PROP_DETECTION = "detection";
  private static final String PROP_CRON = "cron";
  private static final String PROP_FILTER = "filter";
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

  private static final String PROP_DETECTION_NAME = "detectionName";
  private static final String PROP_DESC_NAME = "description";
  private static final String PROP_ACTIVE = "active";
  private static final String PROP_OWNERS = "owners";

  private static final String PROP_ALERTS = "alerts";
  private static final String COMPOSITE_ALERT = "COMPOSITE_ALERT";
  private static final String METRIC_ALERT = "METRIC_ALERT";


  private static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();
  private static final Set<String> MOVING_WINDOW_DETECTOR_TYPES = ImmutableSet.of("ALGORITHM", "MIGRATED_ALGORITHM");

  private final Map<String, Object> components = new HashMap<>();
  private DataProvider dataProvider;
  private DetectionMetricAttributeHolder metricAttributesMap;
  private Set<DatasetConfigDTO> datasetConfigs;

  public DetectionConfigTranslator(String yamlConfig, DataProvider provider) {
    this(yamlConfig, provider, new DetectionConfigValidator(provider));
  }

  public DetectionConfigTranslator(String yamlConfig, DataProvider provider, DetectionConfigValidator validator) {
    super(yamlConfig, validator);
    this.dataProvider = provider;
    this.metricAttributesMap = new DetectionMetricAttributeHolder(provider);
    this.datasetConfigs = new HashSet<>();
  }

  private Map<String, Object> translateMetricAlert(Map<String, Object> metricAlertConfigMap) {
    String subEntityName = MapUtils.getString(metricAlertConfigMap, PROP_NAME);

    DatasetConfigDTO datasetConfigDTO = metricAttributesMap.fetchDataset(metricAlertConfigMap);
    datasetConfigs.add(datasetConfigDTO);
    Map<String, Collection<String>> dimensionFiltersMap = ConfigUtils.getMap(metricAlertConfigMap.get(PROP_FILTERS));
    String metricUrn = MetricEntity.fromMetric(dimensionFiltersMap, metricAttributesMap.fetchMetric(metricAlertConfigMap).getId()).getUrn();
    Map<String, Object> mergerProperties = ConfigUtils.getMap(metricAlertConfigMap.get(PROP_MERGER));

    // Translate all the rules
    List<Map<String, Object>> ruleYamls = getList(metricAlertConfigMap.get(PROP_RULES));
    List<Map<String, Object>> nestedPipelines = new ArrayList<>();
    for (Map<String, Object> ruleYaml : ruleYamls) {
      List<Map<String, Object>> filterYamls = ConfigUtils.getList(ruleYaml.get(PROP_FILTER));
      List<Map<String, Object>> detectionYamls = ConfigUtils.getList(ruleYaml.get(PROP_DETECTION));
      List<Map<String, Object>> detectionProperties = buildListOfMergeWrapperProperties(
          subEntityName, metricUrn, detectionYamls, mergerProperties,
          datasetConfigDTO.bucketTimeGranularity());
      if (filterYamls.isEmpty()) {
        nestedPipelines.addAll(detectionProperties);
      } else {
        List<Map<String, Object>> filterNestedProperties = detectionProperties;
        for (Map<String, Object> filterProperties : filterYamls) {
          filterNestedProperties = buildFilterWrapperProperties(metricUrn, AnomalyFilterWrapper.class.getName(), filterProperties,
              filterNestedProperties);
        }
        nestedPipelines.addAll(filterNestedProperties);
      }
    }

    // Wrap with dimension exploration properties
    Map<String, Object> dimensionWrapperProperties = buildDimensionWrapperProperties(
        metricAlertConfigMap, dimensionFiltersMap, metricUrn, datasetConfigDTO.getDataset());
    Map<String, Object> properties = buildWrapperProperties(
        ChildKeepingMergeWrapper.class.getName(),
        Collections.singletonList(buildWrapperProperties(DimensionWrapper.class.getName(), nestedPipelines, dimensionWrapperProperties)),
        mergerProperties);

    // Wrap with metric level grouper, restricting to only 1 grouper
    List<Map<String, Object>> grouperYamls = getList(metricAlertConfigMap.get(PROP_GROUPER));
    if (!grouperYamls.isEmpty()) {
      properties = buildWrapperProperties(
          EntityAnomalyMergeWrapper.class.getName(),
          Collections.singletonList(buildGroupWrapperProperties(subEntityName, metricUrn, grouperYamls.get(0), Collections.singletonList(properties))),
          mergerProperties);

      properties = buildWrapperProperties(
          ChildKeepingMergeWrapper.class.getName(),
          Collections.singletonList(properties),
          mergerProperties);
    }

    return properties;
  }

  private Map<String, Object> translateCompositeAlert(Map<String, Object> compositeAlertConfigMap) {
    Map<String, Object> properties;
    String subEntityName = MapUtils.getString(compositeAlertConfigMap, PROP_NAME);

    // Recursively translate all the sub-alerts
    List<Map<String, Object>> subDetectionYamls = ConfigUtils.getList(compositeAlertConfigMap.get(PROP_ALERTS));
    List<Map<String, Object>> nestedPropertiesList = new ArrayList<>();
    for (Map<String, Object> subDetectionYaml : subDetectionYamls) {
      Map<String, Object> subProps;
      if (subDetectionYaml.containsKey(PROP_TYPE) && subDetectionYaml.get(PROP_TYPE).equals(COMPOSITE_ALERT)) {
        subProps = translateCompositeAlert(subDetectionYaml);
      } else {
        subProps = translateMetricAlert(subDetectionYaml);
      }

      nestedPropertiesList.add(subProps);
    }

    // Wrap the entity level grouper, only 1 grouper is supported now
    List<Map<String, Object>> grouperProps = ConfigUtils.getList(compositeAlertConfigMap.get(PROP_GROUPER));
    Map<String, Object> mergerProperties = ConfigUtils.getMap(compositeAlertConfigMap.get(PROP_MERGER));
    if (!grouperProps.isEmpty()) {
      properties = buildWrapperProperties(
          EntityAnomalyMergeWrapper.class.getName(),
          Collections.singletonList(buildGroupWrapperProperties(subEntityName, grouperProps.get(0), nestedPropertiesList)),
          mergerProperties);
      nestedPropertiesList = Collections.singletonList(properties);;
    }

    properties = buildWrapperProperties(
        ChildKeepingMergeWrapper.class.getName(),
        nestedPropertiesList,
        mergerProperties);

    return properties;
  }

  @Override
  DetectionConfigDTO translateConfig(Map<String, Object> yamlConfigMap) throws IllegalArgumentException {
    // Hack to support 'detectionName' attribute at root level and 'name' attribute elsewhere
    // We consistently use 'name' as a convention to define the sub-alerts. However, at the root
    // level, as a convention, we will use 'detectionName' which defines the name of the complete alert.
    String alertName = MapUtils.getString(yamlConfigMap, PROP_DETECTION_NAME);
    yamlConfigMap.put(PROP_NAME, alertName);

    // By default if 'type' is not specified, we assume it as a METRIC_ALERT
    if (!yamlConfigMap.containsKey(PROP_TYPE)) {
      yamlConfigMap.put(PROP_TYPE, METRIC_ALERT);
    }

    // Translate config depending on the type (METRIC_ALERT OR COMPOSITE_ALERT)
    Map<String, Object> properties;
    String cron;
    if (yamlConfigMap.get(PROP_TYPE).equals(COMPOSITE_ALERT)) {
      properties = translateCompositeAlert(yamlConfigMap);

      // TODO: discuss strategy for default cron
      Preconditions.checkArgument(yamlConfigMap.containsKey(PROP_CRON), "Missing property (" + PROP_CRON + ") in alert");
      cron = MapUtils.getString(yamlConfigMap, PROP_CRON);
    } else {
      // The legacy type 'COMPOSITE' will be treated as a metric alert along with the new convention METRIC_ALERT.
      // This is applicable only at the root level to maintain backward compatibility.
      properties = translateMetricAlert(yamlConfigMap);
      cron = metricAttributesMap.fetchCron(yamlConfigMap);
    }

    return generateDetectionConfig(yamlConfigMap, properties, this.components, cron);
  }

  private Map<String, Object> buildDimensionWrapperProperties(Map<String, Object> yamlConfigMap,
      Map<String, Collection<String>> dimensionFilters, String metricUrn, String datasetName) {
    Map<String, Object> dimensionWrapperProperties = new HashMap<>();
    dimensionWrapperProperties.put(PROP_NESTED_METRIC_URNS, Collections.singletonList(metricUrn));
    if (yamlConfigMap.containsKey(PROP_DIMENSION_EXPLORATION)) {
      Map<String, Object> dimensionExploreYaml = ConfigUtils.getMap(yamlConfigMap.get(PROP_DIMENSION_EXPLORATION));
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

  private List<Map<String, Object>> buildListOfMergeWrapperProperties(String subEntityName, String metricUrn,
      List<Map<String, Object>> yamlConfigs, Map<String, Object> mergerProperties, TimeGranularity datasetTimegranularity) {
    List<Map<String, Object>> properties = new ArrayList<>();
    for (Map<String, Object> yamlConfig : yamlConfigs) {
      properties.add(buildMergeWrapperProperties(subEntityName, metricUrn, yamlConfig, mergerProperties, datasetTimegranularity));
    }
    return properties;
  }

  private Map<String, Object> buildMergeWrapperProperties(String subEntityName, String metricUrn, Map<String, Object> yamlConfig,
      Map<String, Object> mergerProperties, TimeGranularity datasetTimegranularity) {
    String detectorType = MapUtils.getString(yamlConfig, PROP_TYPE);
    String name = MapUtils.getString(yamlConfig, PROP_NAME);
    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_CLASS_NAME, AnomalyDetectorWrapper.class.getName());
    nestedProperties.put(PROP_SUB_ENTITY_NAME, subEntityName);
    String detectorRefKey = makeComponentRefKey(detectorType, name);

    fillInDetectorWrapperProperties(nestedProperties, yamlConfig, detectorType, datasetTimegranularity);

    buildComponentSpec(metricUrn, yamlConfig, detectorType, detectorRefKey);

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
      buildComponentSpec(metricUrn, yamlConfig, baselineProviderType, baselineProviderKey);
      properties.put(PROP_BASELINE_PROVIDER, baselineProviderKey);
    }
    properties.putAll(mergerProperties);
    return properties;
  }

  private Map<String, Object> buildGroupWrapperProperties(String entityName, Map<String, Object> grouperYaml, List<Map<String, Object>> nestedProps) {
    return buildGroupWrapperProperties(entityName, null, grouperYaml, nestedProps);
  }

  private Map<String, Object> buildGroupWrapperProperties(String entityName, String metricUrn,
      Map<String, Object> grouperYaml, List<Map<String, Object>> nestedProps) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, GrouperWrapper.class.getName());
    properties.put(PROP_NESTED, nestedProps);
    properties.put(PROP_SUB_ENTITY_NAME, entityName);

    String grouperType = MapUtils.getString(grouperYaml, PROP_TYPE);
    String grouperName = MapUtils.getString(grouperYaml, PROP_NAME);
    String grouperRefKey = makeComponentRefKey(grouperType, grouperName);
    properties.put(PROP_GROUPER, grouperRefKey);

    buildComponentSpec(metricUrn, grouperYaml, grouperType, grouperRefKey);

    return properties;
  }

  // fill in window size and unit if detector requires this
  private void fillInDetectorWrapperProperties(Map<String, Object> properties, Map<String, Object> yamlConfig, String detectorType, TimeGranularity datasetTimegranularity) {
    // set default bucketPeriod
    properties.put(PROP_BUCKET_PERIOD, datasetTimegranularity.toPeriod().toString());

    // override bucketPeriod now since it is needed by detection window
    if (yamlConfig.containsKey(PROP_BUCKET_PERIOD)){
      properties.put(PROP_BUCKET_PERIOD, MapUtils.getString(yamlConfig, PROP_BUCKET_PERIOD));
    }

    // set default detection window
    setDefaultDetectionWindow(properties, detectorType);

    // override other properties from yaml
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
    if (yamlConfig.containsKey(PROP_CACHE_PERIOD_LOOKBACK)) {
      properties.put(PROP_CACHE_PERIOD_LOOKBACK, MapUtils.getString(yamlConfig, PROP_CACHE_PERIOD_LOOKBACK));
    }
  }

  // Set the default detection window if it is not specified.
  // Here instead of using data granularity we use the detection period to set the default window size.
  private void setDefaultDetectionWindow(Map<String, Object> properties, String detectorType) {
    if (MOVING_WINDOW_DETECTOR_TYPES.contains(detectorType)) {
      properties.put(PROP_MOVING_WINDOW_DETECTION, true);
      org.joda.time.Period detectionPeriod =
          org.joda.time.Period.parse(MapUtils.getString(properties, PROP_BUCKET_PERIOD));
      int days = detectionPeriod.toStandardDays().getDays();
      int hours = detectionPeriod.toStandardHours().getHours();
      int minutes = detectionPeriod.toStandardMinutes().getMinutes();
      if (days >= 1) {
        properties.put(PROP_WINDOW_SIZE, 1);
        properties.put(PROP_WINDOW_UNIT, TimeUnit.DAYS);
      } else if (hours >= 1) {
        properties.put(PROP_WINDOW_SIZE, 24);
        properties.put(PROP_WINDOW_UNIT, TimeUnit.HOURS);
      } else if (minutes >= 1) {
        properties.put(PROP_WINDOW_SIZE, 6);
        properties.put(PROP_WINDOW_UNIT, TimeUnit.HOURS);
        properties.put(PROP_FREQUENCY, new TimeGranularity(15, TimeUnit.MINUTES));
      } else {
        properties.put(PROP_WINDOW_SIZE, 6);
        properties.put(PROP_WINDOW_UNIT, TimeUnit.HOURS);
      }
    }
  }

  private List<Map<String, Object>> buildFilterWrapperProperties(String metricUrn, String wrapperClassName,
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
    buildComponentSpec(metricUrn, yamlConfig, filterType, filterRefKey);

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

  private void buildComponentSpec(String metricUrn, Map<String, Object> yamlConfig, String type, String componentRefKey) {
    Map<String, Object> componentSpecs = new HashMap<>();

    String componentClassName = DETECTION_REGISTRY.lookup(type);
    componentSpecs.put(PROP_CLASS_NAME, componentClassName);
    componentSpecs.put(PROP_METRIC_URN, metricUrn);

    Map<String, Object> params = ConfigUtils.getMap(yamlConfig.get(PROP_PARAMS));

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

  /**
   * Fill in common fields of detection config. Properties of the pipeline is filled by the subclass.
   */
  private DetectionConfigDTO generateDetectionConfig(Map<String, Object> yamlConfigMap, Map<String, Object> properties,
      Map<String, Object> components, String cron) {
    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName(MapUtils.getString(yamlConfigMap, PROP_DETECTION_NAME));
    config.setDescription(MapUtils.getString(yamlConfigMap, PROP_DESC_NAME));
    config.setLastTimestamp(System.currentTimeMillis());
    config.setOwners(filterOwners(ConfigUtils.getList(yamlConfigMap.get(PROP_OWNERS))));

    config.setProperties(properties);
    config.setComponentSpecs(components);
    config.setCron(cron);
    config.setActive(MapUtils.getBooleanValue(yamlConfigMap, PROP_ACTIVE, true));
    config.setYaml(yamlConfig);

    //TODO: data-availability trigger is only enabled for detections running on PINOT only
    if (MapUtils.getString(yamlConfigMap, PROP_CRON) == null
        && datasetConfigs.stream().allMatch(c -> c.getDataSource().equals(PinotThirdEyeDataSource.DATA_SOURCE_NAME))) {
      config.setDataAvailabilitySchedule(true);
    }
    return config;
  }
}
