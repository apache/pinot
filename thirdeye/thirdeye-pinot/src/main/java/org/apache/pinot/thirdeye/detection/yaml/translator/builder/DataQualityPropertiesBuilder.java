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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.dataquality.wrapper.DataSlaWrapper;
import org.apache.pinot.thirdeye.detection.wrapper.DataQualityMergeWrapper;
import org.apache.pinot.thirdeye.detection.yaml.translator.DetectionMetricAttributeHolder;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;

import static org.apache.pinot.thirdeye.detection.ConfigUtils.*;


/**
 * This class is responsible for translating the data quality properties
 */
public class DataQualityPropertiesBuilder extends DetectionConfigPropertiesBuilder {

  static final String PROP_QUALITY_CHECK = "qualityCheck";

  public DataQualityPropertiesBuilder(DetectionMetricAttributeHolder metricAttributesMap, DataProvider provider) {
    super(metricAttributesMap, provider);
  }

  /**
   * Constructs the data quality properties mapping by translating the quality yaml
   *
   * @param metricAlertConfigMap holds the parsed yaml for a single metric alert
   */
  @Override
  public Map<String, Object> buildMetricAlertProperties(Map<String, Object> metricAlertConfigMap) {
    Map<String, Object> properties = new HashMap<>();
    MetricConfigDTO metricConfigDTO = metricAttributesMap.fetchMetric(metricAlertConfigMap);

    String subEntityName = MapUtils.getString(metricAlertConfigMap, PROP_NAME);
    Map<String, Object> mergerProperties = ConfigUtils.getMap(metricAlertConfigMap.get(PROP_MERGER));

    Map<String, Collection<String>> dimensionFiltersMap = ConfigUtils.getMap(metricAlertConfigMap.get(PROP_FILTERS));
    String metricUrn = MetricEntity.fromMetric(dimensionFiltersMap, metricConfigDTO.getId()).getUrn();

    // Translate all the rules
    List<Map<String, Object>> ruleYamls = getList(metricAlertConfigMap.get(PROP_RULES));
    List<Map<String, Object>> nestedPipelines = new ArrayList<>();
    for (Map<String, Object> ruleYaml : ruleYamls) {
      List<Map<String, Object>> qualityYamls = ConfigUtils.getList(ruleYaml.get(PROP_QUALITY));
      if (qualityYamls.isEmpty()) {
        continue;
      }
      List<Map<String, Object>> qualityProperties = buildListOfDataQualityProperties(
          subEntityName, metricUrn, qualityYamls, mergerProperties);
      nestedPipelines.addAll(qualityProperties);
    }
    if (nestedPipelines.isEmpty()) {
      // No data quality rules
      return properties;
    }

    properties.putAll(buildWrapperProperties(DataQualityMergeWrapper.class.getName(), nestedPipelines, mergerProperties));
    return properties;
  }

  @Override
  public Map<String, Object> buildCompositeAlertProperties(Map<String, Object> compositeAlertConfigMap) {
    Map<String, Object> properties = new HashMap<>();

    // Recursively translate all the sub-alerts
    List<Map<String, Object>> subDetectionYamls = ConfigUtils.getList(compositeAlertConfigMap.get(PROP_ALERTS));
    List<Map<String, Object>> nestedPropertiesList = new ArrayList<>();
    for (Map<String, Object> subDetectionYaml : subDetectionYamls) {
      Map<String, Object> subProps;
      if (subDetectionYaml.containsKey(PROP_TYPE) && subDetectionYaml.get(PROP_TYPE).equals(COMPOSITE_ALERT)) {
        subProps = buildCompositeAlertProperties(subDetectionYaml);
      } else {
        subProps = buildMetricAlertProperties(subDetectionYaml);
      }

      nestedPropertiesList.add(subProps);
    }
    if (nestedPropertiesList.isEmpty()) {
      return properties;
    }

    properties.putAll(compositePropertyBuilderHelper(nestedPropertiesList, compositeAlertConfigMap));
    return properties;
  }

  private List<Map<String, Object>> buildListOfDataQualityProperties(String subEntityName, String metricUrn,
      List<Map<String, Object>> yamlConfigs, Map<String, Object> mergerProperties) {
    List<Map<String, Object>> properties = new ArrayList<>();
    for (Map<String, Object> yamlConfig : yamlConfigs) {
      properties.add(buildDataQualityWrapperProperties(subEntityName, metricUrn, yamlConfig, mergerProperties));
    }
    return properties;
  }

  private Map<String, Object> buildDataQualityWrapperProperties(String subEntityName, String metricUrn,
      Map<String, Object> yamlConfig, Map<String, Object> mergerProperties) {
    String qualityType = MapUtils.getString(yamlConfig, PROP_TYPE);
    String name = MapUtils.getString(yamlConfig, PROP_NAME);

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, DataSlaWrapper.class.getName());
    properties.put(PROP_SUB_ENTITY_NAME, subEntityName);
    properties.put(PROP_METRIC_URN, metricUrn);

    String qualityRefKey = makeComponentRefKey(qualityType, name);
    properties.put(PROP_QUALITY_CHECK, qualityRefKey);

    buildComponentSpec(metricUrn, yamlConfig, qualityRefKey);

    properties.putAll(mergerProperties);
    return properties;
  }
}
