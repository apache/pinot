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

package org.apache.pinot.thirdeye.detection.yaml.translator.builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.wrapper.ChildKeepingMergeWrapper;
import org.apache.pinot.thirdeye.detection.wrapper.EntityAnomalyMergeWrapper;
import org.apache.pinot.thirdeye.detection.wrapper.GrouperWrapper;
import org.apache.pinot.thirdeye.detection.yaml.translator.DetectionMetricAttributeHolder;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;

import static org.apache.pinot.thirdeye.detection.yaml.DetectionConfigTuner.*;


/**
 * This is the root of the detection config builder. Other translators extend from this class.
 */
public abstract class DetectionConfigPropertiesBuilder {

  public static final String PROP_SUB_ENTITY_NAME = "subEntityName";
  public static final String PROP_DIMENSION_EXPLORATION = "dimensionExploration";
  public static final String PROP_FILTERS = "filters";

  static final String PROP_DETECTION = "detection";
  static final String PROP_FILTER = "filter";
  static final String PROP_QUALITY = "quality";
  static final String PROP_TYPE = "type";
  static final String PROP_CLASS_NAME = "className";
  static final String PROP_PARAMS = "params";
  static final String PROP_METRIC_URN = "metricUrn";
  static final String PROP_DIMENSION_FILTER_METRIC = "dimensionFilterMetric";
  static final String PROP_NESTED_METRIC_URNS = "nestedMetricUrns";
  static final String PROP_RULES = "rules";
  static final String PROP_GROUPER = "grouper";
  static final String PROP_LABELER = "labeler";
  static final String PROP_NESTED = "nested";
  static final String PROP_BASELINE_PROVIDER = "baselineValueProvider";
  static final String PROP_DETECTOR = "detector";
  static final String PROP_MOVING_WINDOW_DETECTION = "isMovingWindowDetection";
  static final String PROP_WINDOW_DELAY = "windowDelay";
  static final String PROP_WINDOW_DELAY_UNIT = "windowDelayUnit";
  static final String PROP_WINDOW_SIZE = "windowSize";
  static final String PROP_WINDOW_UNIT = "windowUnit";
  static final String PROP_FREQUENCY = "frequency";
  static final String PROP_MERGER = "merger";
  static final String PROP_TIMEZONE = "timezone";
  static final String PROP_NAME = "name";

  static final String DEFAULT_BASELINE_PROVIDER_YAML_TYPE = "RULE_BASELINE";
  static final String PROP_BUCKET_PERIOD = "bucketPeriod";
  static final String PROP_CACHE_PERIOD_LOOKBACK = "cachingPeriodLookback";

  static final String PROP_ALERTS = "alerts";
  static final String COMPOSITE_ALERT = "COMPOSITE_ALERT";

  final DetectionMetricAttributeHolder metricAttributesMap;
  final DataProvider dataProvider;
  static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();

  DetectionConfigPropertiesBuilder(DetectionMetricAttributeHolder metricAttributesMap, DataProvider dataProvider) {
    this.metricAttributesMap = metricAttributesMap;
    this.dataProvider = dataProvider;
  }

  public abstract Map<String, Object> buildMetricAlertProperties(Map<String, Object> yamlConfigMap);

  public abstract Map<String, Object> buildCompositeAlertProperties(Map<String, Object> yamlConfigMap);

  Map<String, Object> buildDimensionWrapperProperties(Map<String, Object> yamlConfigMap,
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

  Map<String, Object> buildGroupWrapperProperties(String entityName, Map<String, Object> grouperYaml, List<Map<String, Object>> nestedProps) {
    return buildGroupWrapperProperties(entityName, null, grouperYaml, nestedProps);
  }

  Map<String, Object> buildGroupWrapperProperties(String entityName, String metricUrn,
      Map<String, Object> grouperYaml, List<Map<String, Object>> nestedProps) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, GrouperWrapper.class.getName());
    properties.put(PROP_NESTED, nestedProps);
    properties.put(PROP_SUB_ENTITY_NAME, entityName);

    String grouperRefKey = makeComponentRefKey(
        MapUtils.getString(grouperYaml, PROP_TYPE), MapUtils.getString(grouperYaml, PROP_NAME));
    properties.put(PROP_GROUPER, grouperRefKey);

    buildComponentSpec(metricUrn, grouperYaml, grouperRefKey);

    return properties;
  }

  List<Map<String, Object>> buildFilterWrapperProperties(String metricUrn, String wrapperClassName,
      Map<String, Object> yamlConfig, List<Map<String, Object>> nestedProperties) {
    if (yamlConfig == null || yamlConfig.isEmpty()) {
      return nestedProperties;
    }
    Map<String, Object> wrapperProperties = buildWrapperProperties(wrapperClassName, nestedProperties);
    if (wrapperProperties.isEmpty()) {
      return Collections.emptyList();
    }
    String filterRefKey = makeComponentRefKey(
        MapUtils.getString(yamlConfig, PROP_TYPE), MapUtils.getString(yamlConfig, PROP_NAME));
    wrapperProperties.put(PROP_FILTER, filterRefKey);
    buildComponentSpec(metricUrn, yamlConfig, filterRefKey);

    return Collections.singletonList(wrapperProperties);
  }

  Map<String, Object> buildLabelerWrapperProperties(String metricUrn, String wrapperClassName,
      Map<String, Object> yamlConfig, List<Map<String, Object>> nestedProperties) {
    Map<String, Object> wrapperProperties = buildWrapperProperties(wrapperClassName, nestedProperties);
    String labelerRefKey = makeComponentRefKey(
        MapUtils.getString(yamlConfig, PROP_TYPE), MapUtils.getString(yamlConfig, PROP_NAME));
    wrapperProperties.put(PROP_LABELER, labelerRefKey);
    buildComponentSpec(metricUrn, yamlConfig, labelerRefKey);
    return wrapperProperties;
  }


  void buildComponentSpec(String metricUrn, Map<String, Object> yamlConfig, String componentRefKey) {
    Map<String, Object> componentSpecs = new HashMap<>();
    String componentKey = DetectionUtils.getComponentKey(componentRefKey);
    String componentClassName = DETECTION_REGISTRY.lookup(DetectionUtils.getComponentType(componentKey));
    componentSpecs.put(PROP_CLASS_NAME, componentClassName);
    if (metricUrn != null) {
      componentSpecs.put(PROP_METRIC_URN, metricUrn);
    }

    Map<String, Object> params = ConfigUtils.getMap(yamlConfig.get(PROP_PARAMS));

    // For tunable components, the model params are computed from user supplied yaml params and previous model params.
    // We store the yaml params under a separate key, PROP_YAML_PARAMS, to distinguish from model params.
    if (DETECTION_REGISTRY.isTunable(componentClassName)) {
      componentSpecs.put(PROP_YAML_PARAMS, params);
    } else {
      componentSpecs.putAll(params);
    }

    metricAttributesMap.addComponent(componentKey, componentSpecs);
  }

  Map<String, Object> compositePropertyBuilderHelper(List<Map<String, Object>> nestedPropertiesList,
      Map<String, Object> compositeAlertConfigMap) {
    Map<String, Object> properties;
    String subEntityName = MapUtils.getString(compositeAlertConfigMap, PROP_NAME);

    // Wrap the entity level grouper, only 1 grouper is supported now
    List<Map<String, Object>> grouperProps = ConfigUtils.getList(compositeAlertConfigMap.get(PROP_GROUPER));
    Map<String, Object> mergerProperties = ConfigUtils.getMap(compositeAlertConfigMap.get(PROP_MERGER));
    if (!grouperProps.isEmpty()) {
      properties = buildWrapperProperties(
          EntityAnomalyMergeWrapper.class.getName(),
          Collections.singletonList(buildGroupWrapperProperties(subEntityName, grouperProps.get(0), nestedPropertiesList)),
          mergerProperties);
      nestedPropertiesList = Collections.singletonList(properties);
    }

    return buildWrapperProperties(
        ChildKeepingMergeWrapper.class.getName(),
        nestedPropertiesList,
        mergerProperties);
  }

  Map<String, Object> buildWrapperProperties(String wrapperClassName,
      List<Map<String, Object>> nestedProperties) {
    return buildWrapperProperties(wrapperClassName, nestedProperties, Collections.emptyMap());
  }

  Map<String, Object> buildWrapperProperties(String wrapperClassName,
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

  static String makeComponentRefKey(String type, String name) {
    return "$" + name + ":" + type;
  }
}
