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

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;


/**
 * The translator converts the alert yaml config into a detection alert config
 */
public class YamlDetectionAlertConfigTranslator {
  public static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  public static final String PROP_RECIPIENTS = "recipients";

  public static final String PROP_SUBS_GROUP_NAME = "subscriptionGroupName";
  public static final String PROP_CRON = "cron";
  public static final String PROP_ACTIVE = "active";
  public static final String PROP_APPLICATION = "application";
  public static final String PROP_FROM = "fromAddress";
  public static final String PROP_EMAIL_SUBJECT_TYPE = "emailSubjectStyle";
  public static final String PROP_ALERT_SCHEMES = "alertSchemes";
  public static final String PROP_DETECTION_NAMES = "subscribedDetections";
  public static final String PROP_TYPE = "type";
  public static final String PROP_CLASS_NAME = "className";

  static final String PROP_PARAM = "params";
  static final String PROP_ALERT_SUPPRESSORS = "alertSuppressors";
  static final String PROP_REFERENCE_LINKS = "referenceLinks";
  static final String PROP_TIME_WINDOWS = "timeWindows";
  static final String CRON_SCHEDULE_DEFAULT = "0 0/5 * * * ? *"; // Every 5 min

  private static final String PROP_ONLY_FETCH_LEGACY_ANOMALIES = "onlyFetchLegacyAnomalies";
  private static final String PROP_DIMENSION = "dimension";
  private static final String PROP_DIMENSION_RECIPIENTS = "dimensionRecipients";

  private static final DetectionAlertRegistry DETECTION_ALERT_REGISTRY = DetectionAlertRegistry.getInstance();
  private static final Set<String> PROPERTY_KEYS = new HashSet<>(
      Arrays.asList(PROP_RECIPIENTS, PROP_DIMENSION, PROP_DIMENSION_RECIPIENTS));
  private final DetectionConfigManager detectionConfigDAO;

  public YamlDetectionAlertConfigTranslator(DetectionConfigManager detectionConfigDAO) {
    this.detectionConfigDAO = detectionConfigDAO;
  }

  private Map<String, Object> buildAlerterProperties(Map<String, Object> alertYamlConfigs, Collection<Long> detectionConfigIds) {
    Map<String, Object> properties = buildAlerterProperties(alertYamlConfigs);
    properties.put(PROP_DETECTION_CONFIG_IDS, detectionConfigIds);
    return properties;
  }

  private Map<String, Object> buildAlerterProperties(Map<String, Object> alertYamlConfigs) {
    Map<String, Object> properties = new HashMap<>();
    for (Map.Entry<String, Object> entry : alertYamlConfigs.entrySet()) {
      if (entry.getKey().equals(PROP_TYPE)) {
        properties.put(PROP_CLASS_NAME, DETECTION_ALERT_REGISTRY.lookupAlertFilters(MapUtils.getString(alertYamlConfigs, PROP_TYPE)));
      } else {
        if (PROPERTY_KEYS.contains(entry.getKey())) {
          properties.put(entry.getKey(), entry.getValue());
        }
      }
    }

    return properties;
  }

  @SuppressWarnings("unchecked")
  private Map<String,Map<String,Object>> buildAlertSuppressors(Map<String,Object> yamlAlertConfig) {
    List<Map<String, Object>> alertSuppressors = ConfigUtils.getList(yamlAlertConfig.get(PROP_ALERT_SUPPRESSORS));
    Map<String, Map<String, Object>> alertSuppressorsHolder = new HashMap<>();
    Map<String, Object> alertSuppressorsParsed = new HashMap<>();
    if (!alertSuppressors.isEmpty()) {
      for (Map<String, Object> alertSuppressor : alertSuppressors) {
        Map<String, Object> alertSuppressorsTimeWindow = new HashMap<>();
        if (alertSuppressor.get(PROP_TYPE) != null) {
          alertSuppressorsTimeWindow.put(PROP_CLASS_NAME,
              DETECTION_ALERT_REGISTRY.lookupAlertSuppressors(alertSuppressor.get(PROP_TYPE).toString()));
        }

        if (alertSuppressor.get(PROP_PARAM) != null) {
          for (Map.Entry<String, Object> params : ((Map<String, Object>) alertSuppressor.get(PROP_PARAM)).entrySet()) {
            alertSuppressorsParsed.put(params.getKey(), params.getValue());
          }
        }

        String suppressorType =
            CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, alertSuppressor.get(PROP_TYPE).toString());
        alertSuppressorsTimeWindow.put(PROP_TIME_WINDOWS, new ArrayList<>(Arrays.asList(alertSuppressorsParsed)));
        alertSuppressorsHolder.put(suppressorType + "Suppressor", alertSuppressorsTimeWindow);
      }
    }

    return alertSuppressorsHolder;
  }

  @SuppressWarnings("unchecked")
  private Map<String,Map<String,Object>>  buildAlertSchemes(Map<String,Object> yamlAlertConfig) {
    List<Map<String, Object>> alertSchemes = ConfigUtils.getList(yamlAlertConfig.get(PROP_ALERT_SCHEMES));
    Map<String, Map<String, Object>> alertSchemesHolder = new HashMap<>();
    Map<String, Object> alertSchemesParsed = new HashMap<>();
    if (!alertSchemes.isEmpty()) {
      for (Map<String, Object> alertScheme : alertSchemes) {
        if (alertScheme.get(PROP_TYPE) != null) {
          alertSchemesParsed.put(PROP_CLASS_NAME,
              DETECTION_ALERT_REGISTRY.lookupAlertSchemes(alertScheme.get(PROP_TYPE).toString()));
        }

        if (alertScheme.get(PROP_PARAM) != null) {
          for (Map.Entry<String, Object> params : ((Map<String, Object>) alertScheme.get(PROP_PARAM)).entrySet()) {
            alertSchemesParsed.put(params.getKey(), params.getValue());
          }
        }

        alertSchemesHolder.put(alertScheme.get(PROP_TYPE).toString().toLowerCase() + "Scheme", alertSchemesParsed);
      }
    }

    return alertSchemesHolder;
  }

  /**
   * Generates the {@link DetectionAlertConfigDTO} from the YAML Alert Map
   */
  @SuppressWarnings("unchecked")
  public DetectionAlertConfigDTO translate(Map<String,Object> yamlAlertConfig) {
    DetectionAlertConfigDTO alertConfigDTO = new DetectionAlertConfigDTO();

    alertConfigDTO.setName(MapUtils.getString(yamlAlertConfig, PROP_SUBS_GROUP_NAME));
    alertConfigDTO.setApplication(MapUtils.getString(yamlAlertConfig, PROP_APPLICATION));
    alertConfigDTO.setFrom(MapUtils.getString(yamlAlertConfig, PROP_FROM));

    alertConfigDTO.setCronExpression(MapUtils.getString(yamlAlertConfig, PROP_CRON, CRON_SCHEDULE_DEFAULT));
    alertConfigDTO.setActive(MapUtils.getBooleanValue(yamlAlertConfig, PROP_ACTIVE, true));

    // TODO: Remove all references to onlyFetchLegacyAnomalies after migration
    alertConfigDTO.setOnlyFetchLegacyAnomalies(MapUtils.getBooleanValue(yamlAlertConfig, PROP_ONLY_FETCH_LEGACY_ANOMALIES, false));

    alertConfigDTO.setSubjectType((AlertConfigBean.SubjectType) MapUtils.getObject(yamlAlertConfig, PROP_EMAIL_SUBJECT_TYPE, AlertConfigBean.SubjectType.METRICS));

    Map<String, String> refLinks = MapUtils.getMap(yamlAlertConfig, PROP_REFERENCE_LINKS);
    if (refLinks == null) {
      refLinks = new HashMap<>();
      yamlAlertConfig.put(PROP_REFERENCE_LINKS, refLinks);
    }
    refLinks.put("ThirdEye User Guide", "https://go/thirdeyeuserguide");
    refLinks.put("Add Reference Links", "https://go/thirdeyealertreflink");
    alertConfigDTO.setReferenceLinks(MapUtils.getMap(yamlAlertConfig, PROP_REFERENCE_LINKS));

    alertConfigDTO.setAlertSchemes(buildAlertSchemes(yamlAlertConfig));
    alertConfigDTO.setAlertSuppressors(buildAlertSuppressors(yamlAlertConfig));
    alertConfigDTO.setHighWaterMark(0L);

    // NOTE: The below fields will/should be hidden from the YAML/UI. They will only be updated by the backend pipeline.
    List<Long> detectionConfigIds = new ArrayList<>();
    List<String> detectionNames = ConfigUtils.getList(yamlAlertConfig.get(PROP_DETECTION_NAMES));

    try {
      detectionConfigIds.addAll(detectionNames.stream().map(detectionName ->  this.detectionConfigDAO.findByPredicate(
          Predicate.EQ("name", detectionName)).get(0).getId()).collect(Collectors.toList()));
    } catch (Exception e){
      throw new IllegalArgumentException("cannot find detection pipeline, please check the subscribed detections.");
    }

    alertConfigDTO.setProperties(buildAlerterProperties(yamlAlertConfig, detectionConfigIds));
    Map<Long, Long> vectorClocks = new HashMap<>();
    long currentTimestamp = System.currentTimeMillis();
    for (long detectionConfigId : detectionConfigIds) {
      vectorClocks.put(detectionConfigId, currentTimestamp);
    }
    alertConfigDTO.setVectorClocks(vectorClocks);

    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    options.setPrettyFlow(true);
    alertConfigDTO.setYaml(new Yaml(options).dump(yamlAlertConfig));

    return alertConfigDTO;
  }
}
