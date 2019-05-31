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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
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
  private static final String PROP_MERGER = "merger";
  private static final String PROP_NAME = "name";
  private static final String PROP_DETECTION_NAME = "detectionName";
  private static final String PROP_MAX_DURATION = "maxDuration";
  private static final String PROP_CLASS_NAME = "className";

  public DetectionConfigValidator(DataProvider provider) {
    this.provider = provider;
    this.loader = new DetectionPipelineLoader();
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
   * Validate the pipeline by loading and initializing components
   */
  private void semanticValidation(DetectionConfigDTO detectionConfig) {
    try {
      // backup and swap out id
      Long id = detectionConfig.getId();
      detectionConfig.setId(-1L);

      // try to load the detection pipeline and init all the components
      this.loader.from(provider, detectionConfig, 0, 0);

      // set id back
      detectionConfig.setId(id);
    } catch (Exception e){
      // exception thrown in validate pipeline via reflection
      throw new IllegalArgumentException("Semantic error: " + e.getCause().getMessage());
    }
  }

  /**
   * Validates the detection or filter rule accordingly based on {@param ruleType}
   */
  private void validateRule(Map<String, Object> ruleYaml, int ruleIndex, String ruleType, Set<String> ruleNamesTaken) {
    Preconditions.checkArgument(ruleYaml.containsKey(PROP_TYPE),
        "In rule no." + (ruleIndex) + ", " + ruleType + " rule type is missing.");
    String type = MapUtils.getString(ruleYaml, PROP_TYPE);
    String name = MapUtils.getString(ruleYaml, PROP_NAME);
    Preconditions.checkNotNull(name,
        "In rule no." + (ruleIndex) + ", " + ruleType + " rule name for type " +  type + " is missing.");
    Preconditions.checkArgument(!ruleNamesTaken.contains(name),
        "In rule No." + (ruleIndex) + ", found duplicate rule name, rule name must be unique within config." );
    Preconditions.checkArgument(!name.contains(":"), "Sorry, rule name cannot contain \':\'");
  }

  /**
   * Validate the the detection yaml configuration.
   *
   * @param detectionYaml the detection yaml configuration to be validated
   */
  @Override
  public void validateYaml(Map<String, Object> detectionYaml) {
    // Validate all compulsory fields
    Preconditions.checkArgument(detectionYaml.containsKey(PROP_DETECTION_NAME), "Property missing " + PROP_DETECTION_NAME);
    Preconditions.checkArgument(detectionYaml.containsKey(PROP_METRIC), "Property missing " + PROP_METRIC);
    Preconditions.checkArgument(detectionYaml.containsKey(PROP_DATASET), "Property missing " + PROP_DATASET);
    Preconditions.checkArgument(detectionYaml.containsKey(PROP_RULES), "Property missing " + PROP_RULES);

    // Validate fields which shouldn't be defined at root level
    Preconditions.checkArgument(!detectionYaml.containsKey(PROP_FILTER), "Please double check the filter"
        + " config. Adding dimensions filters should be in the yaml root level using 'filters' as the key. Anomaly "
        + "filter should be added in to the indentation level of detection yaml it applies to.");

    // Check if the metric defined in the config exists
    MetricConfigDTO metricConfig = provider
        .fetchMetric(MapUtils.getString(detectionYaml, PROP_METRIC), MapUtils.getString(detectionYaml, PROP_DATASET));
    Preconditions.checkNotNull(metricConfig, "Metric defined in the config cannot be found");
    DatasetConfigDTO datasetConfig = provider
        .fetchDatasets(Collections.singletonList(metricConfig.getDataset()))
        .get(metricConfig.getDataset());

    // We support only one grouper per metric
    Preconditions.checkArgument(ConfigUtils.getList(detectionYaml.get(PROP_GROUPER)).size() <= 1,
        "Multiple groupers detected for metric. We support only one grouper per metric.");

    // Validate all the rules
    Set<String> names = new HashSet<>();
    List<Map<String, Object>> ruleYamls = ConfigUtils.getList(detectionYaml.get(PROP_RULES));
    for (int i = 1; i <= ruleYamls.size(); i++) {
      Map<String, Object> ruleYaml = ruleYamls.get(i - 1);

      // Validate detection rules
      Preconditions.checkArgument(ruleYaml.containsKey(PROP_DETECTION), "In rule no." + (i) + ", detection rule is missing.");
      List<Map<String, Object>> detectionRuleYamls = ConfigUtils.getList(ruleYaml.get(PROP_DETECTION));
      for (Map<String, Object> detectionRuleYaml : detectionRuleYamls) {
        validateRule(detectionRuleYaml, i, "detection", names);
        names.add(MapUtils.getString(ruleYaml, PROP_NAME));
      }

      // Validate filter rules
      if (ruleYaml.containsKey(PROP_FILTER)) {
        List<Map<String, Object>> filterRuleYamls = ConfigUtils.getList(ruleYaml.get(PROP_FILTER));
        for (Map<String, Object> filterRuleYaml : filterRuleYamls) {
          validateRule(filterRuleYaml, i, "filter", names);
          names.add(MapUtils.getString(ruleYaml, PROP_NAME));
        }
      }
    }

    // Safety condition: Validate if maxDuration is greater than 15 minutes
    Map<String, Object> mergerProperties = MapUtils.getMap(detectionYaml, PROP_MERGER, new HashMap());
    if (mergerProperties.get(PROP_MAX_DURATION) != null) {
      Preconditions.checkArgument(MapUtils.getLong(mergerProperties, PROP_MAX_DURATION) >= datasetConfig.bucketTimeGranularity().toMillis(),
          "The maxDuration field set is not acceptable. Please check the the document and set it correctly.");
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