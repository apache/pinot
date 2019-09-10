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

package org.apache.pinot.thirdeye.detection.validators;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.quartz.CronExpression;


public class DetectionConfigValidator implements ConfigValidator<DetectionConfigDTO> {

  private final DetectionPipelineLoader loader;
  private final DataProvider provider;

  private static final String PROP_DETECTION = "detection";
  private static final String PROP_FILTER = "filter";
  private static final String PROP_METRIC = "metric";
  private static final String PROP_GROUPER = "grouper";
  private static final String PROP_DATASET = "dataset";
  private static final String PROP_TYPE = "type";
  private static final String PROP_RULES = "rules";
  private static final String PROP_ALERTS = "alerts";
  private static final String PROP_MERGER = "merger";
  private static final String PROP_NAME = "name";
  private static final String PROP_DETECTION_NAME = "detectionName";
  private static final String PROP_MAX_DURATION = "maxDuration";
  private static final String PROP_CLASS_NAME = "className";

  private static final String LEGACY_METRIC_ALERT = "COMPOSITE";
  private static final String METRIC_ALERT = "METRIC_ALERT";
  private static final String COMPOSITE_ALERT = "COMPOSITE_ALERT";
  private static final Set<String> SUPPORTED_ALERT_TYPES = new HashSet<>(
      Arrays.asList(LEGACY_METRIC_ALERT, METRIC_ALERT, COMPOSITE_ALERT));

  public DetectionConfigValidator(DataProvider provider) {
    this.provider = provider;
    this.loader = new DetectionPipelineLoader();
  }

  /**
   * Validate the pipeline by loading and initializing components
   */
  private void semanticValidation(DetectionConfigDTO detectionConfig) {
    // backup and swap out id
    Long id = detectionConfig.getId();
    detectionConfig.setId(-1L);

    // try to load the detection pipeline and init all the components
    this.loader.from(provider, detectionConfig, 0, 0);

    // set id back
    detectionConfig.setId(id);
  }

  /**
   * Perform validation on the detection config like verifying if all the required fields are set
   */
  @Override
  public void validateConfig(DetectionConfigDTO detectionConfig) throws IllegalArgumentException {
    Preconditions.checkNotNull(detectionConfig);

    // Cron Validator
    Preconditions.checkArgument(CronExpression.isValidExpression(detectionConfig.getCron()),
        "The detection cron specified is incorrect. Please verify your cron expression using online cron"
            + " makers.");

    // Empty detection properties
    Preconditions.checkArgument((detectionConfig.getProperties() != null
            && detectionConfig.getProperties().get(PROP_CLASS_NAME) != null
            && StringUtils.isNotEmpty((detectionConfig.getProperties().get(PROP_CLASS_NAME).toString()))),
        "No detection properties found");

    semanticValidation(detectionConfig);
  }

  /**
   * Validates the detection or filter rule accordingly based on {@param ruleType}
   */
  private void validateRule(String alertName, Map<String, Object> ruleYaml, int ruleIndex, String ruleType, Set<String> ruleNamesTaken) {
    Preconditions.checkArgument(ruleYaml.containsKey(PROP_TYPE),
        "Missing property " + ruleType + " (" + ruleType + ") for sub-alert " + alertName + " rule no. " + ruleIndex);

    Preconditions.checkArgument(ruleYaml.containsKey(PROP_NAME),
        "Missing property " + ruleType + " (" + PROP_NAME + ") for sub-alert " + alertName + " rule no. " + ruleIndex);
    String name = MapUtils.getString(ruleYaml, PROP_NAME);

    Preconditions.checkArgument(!ruleNamesTaken.contains(name),
        "Duplicate rule name (" + name + ") found for sub-alert " + alertName + " rule no. " + ruleIndex + ". Names have to be unique within a config.");

    Preconditions.checkArgument(!name.contains(":"),
        "Illegal character (:) found in " + ruleType + " (" + PROP_NAME + ") for sub-alert " + alertName + " rule no. " + ruleIndex);
  }

  private void validateBasicAttributes(Map<String, Object> detectionYaml, String parentAlertName) {
    Preconditions.checkArgument(detectionYaml.containsKey(PROP_NAME),
        "Missing property ( " + PROP_NAME + " ) in one of the sub-alerts under " + parentAlertName);
    String alertName = MapUtils.getString(detectionYaml, PROP_NAME);

    Preconditions.checkArgument(detectionYaml.containsKey(PROP_TYPE),
        "Missing property ( " + PROP_TYPE + " ) in sub-alert " + alertName);
    String alertType = MapUtils.getString(detectionYaml, PROP_TYPE);

    Preconditions.checkArgument(SUPPORTED_ALERT_TYPES.contains(alertType),
        "Unsupported type (" + alertType + ") in sub-alert " + alertName);
  }

  private void validateMetricAlertConfig(Map<String, Object> detectionYaml, String parentAlertName)
      throws IllegalArgumentException {
    validateBasicAttributes(detectionYaml, parentAlertName);
    String alertName = MapUtils.getString(detectionYaml, PROP_NAME);

    // Validate all compulsory fields
    String metric = MapUtils.getString(detectionYaml, PROP_METRIC);
    Preconditions.checkArgument(StringUtils.isNotEmpty(metric),
        "Missing property (" + PROP_METRIC + ") in sub-alert " + alertName);
    String dataset = MapUtils.getString(detectionYaml, PROP_DATASET);
    Preconditions.checkArgument(StringUtils.isNotEmpty(dataset),
        "Missing property (" + PROP_DATASET + ") in sub-alert " + alertName);
    Preconditions.checkArgument(detectionYaml.containsKey(PROP_RULES),
        "Missing property (" + PROP_RULES + ") in sub-alert " + alertName);

    // Validate fields which shouldn't be defined at this level
    Preconditions.checkArgument(!detectionYaml.containsKey(PROP_FILTER),
        "For sub-alert " + alertName + ", please double check the filter config. Adding dimensions filters"
            + " should be in the yaml root level using 'filters' as the key. Anomaly filter should be added in to the"
            + " indentation level of detection yaml it applies to.");

    // Check if the metric defined in the config exists
    MetricConfigDTO metricConfig = provider.fetchMetric(metric, dataset);
    Preconditions.checkArgument(metricConfig != null,
        "Metric doesn't exist in our records. Metric " + metric + " Dataset " + dataset + " in sub-alert " + alertName);

    // Check if the dataset defined in the config exists
    DatasetConfigDTO datasetConfig = provider
        .fetchDatasets(Collections.singletonList(metricConfig.getDataset())).get(metricConfig.getDataset());
    Preconditions.checkArgument(datasetConfig != null,
        "Dataset doesn't exist in our records. Dataset " + dataset + " in sub-alert " + alertName);

    // We support only one grouper per metric
    Preconditions.checkArgument(ConfigUtils.getList(detectionYaml.get(PROP_GROUPER)).size() <= 1,
        "Multiple groupers detected for metric in sub-alert " + alertName);

    // Validate all the rules
    Set<String> names = new HashSet<>();
    List<Map<String, Object>> ruleYamls = ConfigUtils.getList(detectionYaml.get(PROP_RULES));
    for (int ruleIndex = 1; ruleIndex <= ruleYamls.size(); ruleIndex++) {
      Map<String, Object> ruleYaml = ruleYamls.get(ruleIndex - 1);

      // Validate detection rules
      Preconditions.checkArgument(ruleYaml.containsKey(PROP_DETECTION),
          "Detection rule missing for sub-alert " + alertName + " rule no. " + ruleIndex);
      List<Map<String, Object>> detectionRuleYamls = ConfigUtils.getList(ruleYaml.get(PROP_DETECTION));
      for (Map<String, Object> detectionRuleYaml : detectionRuleYamls) {
        validateRule(alertName, detectionRuleYaml, ruleIndex, "detection", names);
        names.add(MapUtils.getString(detectionRuleYaml, PROP_NAME));
      }

      // Validate filter rules
      if (ruleYaml.containsKey(PROP_FILTER)) {
        List<Map<String, Object>> filterRuleYamls = ConfigUtils.getList(ruleYaml.get(PROP_FILTER));
        for (Map<String, Object> filterRuleYaml : filterRuleYamls) {
          validateRule(alertName, filterRuleYaml, ruleIndex, "filter", names);
          names.add(MapUtils.getString(filterRuleYaml, PROP_NAME));
        }
      }
    }

    // Safety condition: Validate if maxDuration is greater than 15 minutes
    Map<String, Object> mergerProperties = ConfigUtils.getMap(detectionYaml.get(PROP_MERGER));
    if (mergerProperties.get(PROP_MAX_DURATION) != null) {
      Preconditions.checkArgument(
          MapUtils.getLong(mergerProperties, PROP_MAX_DURATION) >= datasetConfig.bucketTimeGranularity().toMillis(),
          "The maxDuration field set is not acceptable. Please check the the document and set it correctly.");
    }
  }

  private void validateCompositeAlertConfig(Map<String, Object> detectionYaml, String parentAlertName) throws IllegalArgumentException {
    validateBasicAttributes(detectionYaml, parentAlertName);
    String alertName = MapUtils.getString(detectionYaml, PROP_NAME);

    // Validate all compulsory fields
    Preconditions.checkArgument(detectionYaml.containsKey(PROP_ALERTS),
        "Missing property (" + PROP_ALERTS + ") in alert/sub-alert " + alertName);

    // validate all the sub-alerts depending on the type (METRIC_ALERT OR COMPOSITE_ALERT)
    List<Map<String, Object>> subDetectionYamls = ConfigUtils.getList(detectionYaml.get(PROP_ALERTS));
    for (Map<String, Object> subDetectionYaml : subDetectionYamls) {
      if (subDetectionYaml.containsKey(PROP_TYPE) && subDetectionYaml.get(PROP_TYPE).equals(COMPOSITE_ALERT)) {
        validateCompositeAlertConfig(subDetectionYaml, alertName);
      } else {
        validateMetricAlertConfig(subDetectionYaml, alertName);
      }
    }

    // TODO: Add more validations
    // Validate Alert Condition
    // Validate Merger
  }

  /**
   * Validate the the detection yaml configuration.
   *
   * @param detectionYaml the detection yaml configuration to be validated
   */
  @Override
  public void validateYaml(Map<String, Object> detectionYaml) throws IllegalArgumentException {
    // Validate detectionName
    Preconditions.checkArgument(detectionYaml.containsKey(PROP_DETECTION_NAME), "Property missing: " + PROP_DETECTION_NAME);
    String alertName = MapUtils.getString(detectionYaml, PROP_DETECTION_NAME);

    // Hack to support 'detectionName' attribute at root level and 'name' attribute elsewhere
    // We consistently use 'name' as a convention to define the sub-alerts. However, at the root
    // level, as a convention, we will use 'detectionName' which defines the name of the complete alert.
    detectionYaml.put(PROP_NAME, alertName);

    // By default if 'type' is not specified, we assume it as a METRIC_ALERT
    if (!detectionYaml.containsKey(PROP_TYPE)) {
      detectionYaml.put(PROP_TYPE, METRIC_ALERT);
    }

    // Validate config depending on the type (METRIC_ALERT OR COMPOSITE_ALERT)
    if (detectionYaml.get(PROP_TYPE).equals(COMPOSITE_ALERT)) {
      validateCompositeAlertConfig(detectionYaml, alertName);
    } else {
      // The legacy type 'COMPOSITE' will be treated as a metric alert along with the new convention METRIC_ALERT.
      // This is applicable only at the root level to maintain backward compatibility.
      validateMetricAlertConfig(detectionYaml, alertName);
    }
  }

  /**
   * Perform validation on the updated detection config. Check for fields which shouldn't be
   * updated by the user.
   */
  @Override
  public void validateUpdatedConfig(DetectionConfigDTO updatedConfig, DetectionConfigDTO oldConfig)
      throws IllegalArgumentException {
    validateConfig(updatedConfig);
    Preconditions.checkNotNull(oldConfig);

    Preconditions.checkArgument(updatedConfig.getId().equals(oldConfig.getId()));
    Preconditions.checkArgument(updatedConfig.getLastTimestamp() == oldConfig.getLastTimestamp());
  }
}