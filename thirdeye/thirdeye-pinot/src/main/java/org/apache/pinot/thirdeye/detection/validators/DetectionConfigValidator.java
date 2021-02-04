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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import static org.apache.pinot.thirdeye.detection.ConfigUtils.*;
import static org.apache.pinot.thirdeye.detection.yaml.translator.SubscriptionConfigTranslator.*;


/**
 * Application specific constraints and validations on detection config are defined here
 */
public class DetectionConfigValidator extends ThirdEyeUserConfigValidator<DetectionConfigDTO> {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionConfigValidator.class);

  private final DetectionPipelineLoader loader;
  private final DataProvider provider;

  private static final String PROP_DETECTION = "detection";
  private static final String PROP_FILTER = "filter";
  public static final String PROP_METRIC = "metric";
  public static final String PROP_DATASET = "dataset";
  public static final String PROP_RULES = "rules";
  public static final String PROP_ALERTS = "alerts";
  public static final String PROP_TYPE = "type";
  private static final String PROP_GROUPER = "grouper";
  private static final String PROP_NAME = "name";
  private static final String PROP_DETECTION_NAME = "detectionName";

  private static final String METRIC_ALERT = "METRIC_ALERT";
  private static final String COMPOSITE_ALERT = "COMPOSITE_ALERT";
  private static final String DETECTION_CONFIG_SCHEMA_PATH =
      "/validators/detection/detection-config-schema.json";

  public DetectionConfigValidator(DataProvider provider) {
    super(DETECTION_CONFIG_SCHEMA_PATH);

    this.provider = provider;
    this.loader = new DetectionPipelineLoader();
  }

  /**
   * Validate the pipeline by loading and initializing components
   */
  @Override
  public void semanticValidation(DetectionConfigDTO detectionConfig) throws ConfigValidationException {
    // backup and swap out id
    Long id = detectionConfig.getId();
    detectionConfig.setId(-1L);

    try {
      // try to load the detection pipeline and init all the components
      this.loader.from(provider, detectionConfig, 0, 0);
    } catch (Exception e) {
      throw new RuntimeException("Semantic validation error in " + detectionConfig.getId(), e);
    }

    // set id back
    detectionConfig.setId(id);
  }

  private void validateMetricAlertConfig(Map<String, Object> detectionYaml, Set<String> ruleNames) throws ConfigValidationException {
    String alertName = MapUtils.getString(detectionYaml, PROP_NAME);
    String metric = MapUtils.getString(detectionYaml, PROP_METRIC);
    String dataset = MapUtils.getString(detectionYaml, PROP_DATASET);

    // Check if the metric/dataset defined in the config exists
    DatasetConfigDTO datasetConfig;
    try {
      datasetConfig = fetchDatasetConfigDTO(this.provider, dataset);
    } catch (Exception e) {
      throw new ConfigValidationException(e.getMessage());
    }
    MetricConfigDTO metricConfig = provider.fetchMetric(metric, datasetConfig.getDataset());
    ConfigValidationUtils.checkArgument(metricConfig != null,
        String.format("metric/dataset doesn't exist in our records. metric %s in dataset %s in alert %s",
            metric, dataset, alertName));

    // We support only one grouper per metric
    ConfigValidationUtils.checkArgument(ConfigUtils.getList(detectionYaml.get(PROP_GROUPER)).size() <= 1,
        "Multiple groupers detected for metric in sub-alert " + alertName);

    // Validate all the rules
    List<Map<String, Object>> ruleYamls = ConfigUtils.getList(detectionYaml.get(PROP_RULES));
    for (int ruleIndex = 1; ruleIndex <= ruleYamls.size(); ruleIndex++) {
      Map<String, Object> ruleYaml = ruleYamls.get(ruleIndex - 1);

      // Validate detection rules
      validateDuplicateRuleNames(alertName, ruleIndex, ConfigUtils.getList(ruleYaml.get(PROP_DETECTION)), ruleNames);

      // Validate filter rules
      if (ruleYaml.containsKey(PROP_FILTER)) {
        validateDuplicateRuleNames(alertName, ruleIndex, ConfigUtils.getList(ruleYaml.get(PROP_FILTER)), ruleNames);
      }
    }
  }

  /**
   * Validates to ensure the rule/filter name is unique within the alert
   */
  private void validateDuplicateRuleNames(String alertName, int ruleIndex, List<Map<String, Object>> ruleYamls,
      Set<String> ruleNames) throws ConfigValidationException {
    for (Map<String, Object> ruleYaml : ruleYamls) {
      String name = MapUtils.getString(ruleYaml, PROP_NAME);
      ConfigValidationUtils.checkArgument(!ruleNames.contains(name),
          "Duplicate rule name (" + name + ") found for sub-alert " + alertName + " rule no. " + ruleIndex
              + ". Names have to be unique within a config.");
      ruleNames.add(MapUtils.getString(ruleYaml, PROP_NAME));
    }
  }

  private void validateCompositeAlertConfig(Map<String, Object> detectionYaml, Set<String> ruleNames)
      throws ConfigValidationException {
    // validate all the sub-alerts depending on the type (METRIC_ALERT OR COMPOSITE_ALERT)
    List<Map<String, Object>> subDetectionYamls = ConfigUtils.getList(detectionYaml.get(PROP_ALERTS));
    for (Map<String, Object> subDetectionYaml : subDetectionYamls) {
      if (subDetectionYaml.containsKey(PROP_TYPE) && subDetectionYaml.get(PROP_TYPE).equals(COMPOSITE_ALERT)) {
        validateCompositeAlertConfig(subDetectionYaml, ruleNames);
      } else {
        validateMetricAlertConfig(subDetectionYaml, ruleNames);
      }
    }
  }

  /**
   * Perform validations on the raw user specified detection yaml configuration
   *
   * @param config detection yaml configuration to be validated
   */
  @Override
  public void staticValidation(String config) throws ConfigValidationException {
    Map<String, Object> detectionConfigMap = ConfigUtils.getMap(new Yaml().load(config));
    if (detectionConfigMap.containsKey(PROP_DISABLE_VALD) && MapUtils.getBoolean(detectionConfigMap, PROP_DISABLE_VALD)) {
      LOG.info("Validation disabled for detection config " + config);
      return;
    }

    super.schemaValidation(config);

    // Hack to support 'detectionName' attribute at root level and 'name' attribute elsewhere
    // We consistently use 'name' as a convention to define the sub-alerts. However, at the root
    // level, as a convention, we will use 'detectionName' which defines the name of the complete alert.
    String alertName = MapUtils.getString(detectionConfigMap, PROP_DETECTION_NAME);
    detectionConfigMap.put(PROP_NAME, alertName);

    // make sure the specified cron is valid
    String cron = MapUtils.getString(detectionConfigMap, PROP_CRON);
    if (cron != null) {
      ConfigValidationUtils.checkArgument(CronExpression.isValidExpression(cron), "The cron"
          + " specified in the detection config is incorrect. Please verify using an online cron maker.");
    }

    // Validate config depending on the type (METRIC_ALERT OR COMPOSITE_ALERT)
    Set<String> ruleNames = new HashSet<>();
    if (detectionConfigMap.get(PROP_TYPE) == null || detectionConfigMap.get(PROP_TYPE).equals(METRIC_ALERT)) {
      // By default, we treat every alert as a METRIC_ALERT unless explicitly specified.
      // Even the legacy type termed 'COMPOSITE' will be treated as a metric alert along
      // with the new convention METRIC_ALERT. This is applicable only at the root level
      // to maintain backward compatibility.
      validateMetricAlertConfig(detectionConfigMap, ruleNames);
    } else {
      validateCompositeAlertConfig(detectionConfigMap, ruleNames);
    }
  }
}