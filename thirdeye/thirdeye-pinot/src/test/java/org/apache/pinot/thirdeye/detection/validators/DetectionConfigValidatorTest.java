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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.MockGrouper;
import org.apache.pinot.thirdeye.detection.components.RuleBaselineProvider;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleAnomalyFilter;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleDetector;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;


public class DetectionConfigValidatorTest {

  private Yaml yaml;
  private Map<String, Object> yamlConfig1;
  private Map<String, Object> yamlConfig2;
  private DataProvider provider;
  private DAOTestBase testDAOProvider;
  private DAORegistry daoRegistry;

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    daoRegistry = DAORegistry.getInstance();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @BeforeMethod
  public void setUp() {
    MetricConfigDTO metricConfig = new MetricConfigDTO();
    metricConfig.setAlias("alias");
    metricConfig.setName("test_metric");
    metricConfig.setDataset("test_dataset");
    metricConfig.setId(1L);
    daoRegistry.getMetricConfigDAO().save(metricConfig);

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset("test_dataset");
    datasetConfigDTO.setTimeUnit(TimeUnit.DAYS);
    datasetConfigDTO.setTimeDuration(1);
    daoRegistry.getDatasetConfigDAO().save(datasetConfigDTO);

    this.yaml = new Yaml();
    DetectionRegistry.registerComponent(ThresholdRuleDetector.class.getName(), "THRESHOLD");
    DetectionRegistry.registerComponent(ThresholdRuleAnomalyFilter.class.getName(), "THRESHOLD_RULE_FILTER");
    DetectionRegistry.registerComponent(RuleBaselineProvider.class.getName(), "RULE_BASELINE");
    DetectionRegistry.registerComponent(MockGrouper.class.getName(), "MOCK_GROUPER");
    this.provider = new MockDataProvider().setMetrics(Collections.singletonList(metricConfig)).setDatasets(Collections.singletonList(datasetConfigDTO));

    this.yamlConfig1 = (Map<String, Object>) this.yaml.load(this.getClass().getResourceAsStream("entity-pipeline-config-1.yaml"));
    this.yamlConfig2 = (Map<String, Object>) this.yaml.load(this.getClass().getResourceAsStream("entity-pipeline-config-2.yaml"));
  }

  @Test
  public void testMetricAlertValidation() {
    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  // detectionName is a compulsory field at root level
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNoDetectionNameValidation() {
    this.yamlConfig1.remove("detectionName");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  // If type is not specified at root level, then assume it to be METRIC_ALERT and proceed
  @Test
  public void testNoTypeValidation() {
    this.yamlConfig1.remove("type");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  // Validates if the type field is acceptable
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidTypeValidation() {
    this.yamlConfig1.put("type", "INVALID_TYPE");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNoMetricValidation() {
    this.yamlConfig1.remove("metric");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  // Every METRIC_ALERT must have rules
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNoRulesValidation() {
    this.yamlConfig1.remove("rules");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNonExistingMetricValidation() {
    this.yamlConfig1.put("metric", "metric_doesnt_exist");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  // Every rule must have at least 1 detection
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNoDetectionUnderRuleValidation() {
    ((Map<String, Object>) ConfigUtils.getList(this.yamlConfig1.get("rules")).get(0)).remove("detection");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNoDetectionTypeUnderRuleValidation() {
    ((Map<String, Object>) ConfigUtils.getList(((Map<String, Object>) ConfigUtils.getList(this.yamlConfig1.get("rules")).get(0))
        .get("detection")).get(0)).remove("type");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNoDetectionNameUnderRuleValidation() {
    ((Map<String, Object>) ConfigUtils.getList(((Map<String, Object>) ConfigUtils.getList(this.yamlConfig1.get("rules")).get(0))
        .get("detection")).get(0)).remove("name");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  // Validate if detection/filter names are unique
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDuplicateDetectionNameUnderRuleValidation() {
    // Update rule 1 detection name to that of rule 2 detection name
    ((Map<String, Object>) ConfigUtils.getList(
        ((Map<String, Object>) ConfigUtils.getList(this.yamlConfig1.get("rules")).get(0)).get("detection")
    ).get(0)).put("name", "maxThreshold_2");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig1);
  }

  // Entity Example 2 Validations

  @Test
  public void testEntityAlertValidation() {
    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig2);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNoAlertsValidation() {
    this.yamlConfig2.remove("alerts");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig2);
  }

  // type is a compulsory field at sub-alert levels
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNoTypeUnderAlertsValidation() {
    ((Map<String, Object>) ConfigUtils.getList(this.yamlConfig2.get("alerts")).get(0)).remove("type");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.validateYaml(this.yamlConfig2);
  }
}
