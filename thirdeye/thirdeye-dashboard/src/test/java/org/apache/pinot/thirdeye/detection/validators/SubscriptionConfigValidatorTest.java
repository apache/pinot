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

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.swagger.util.Json;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;


public class SubscriptionConfigValidatorTest {

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

    this.yamlConfig1 = ConfigUtils.getMap(this.yaml.load(this.getClass().getResourceAsStream("entity-pipeline-config-1.yaml")));
    this.yamlConfig2 = ConfigUtils.getMap(this.yaml.load(this.getClass().getResourceAsStream("entity-pipeline-config-2.yaml")));
  }

  @Test
  public void testMetricAlertValidation() throws Exception {
    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.staticValidation(this.yaml.dump(yamlConfig1));
  }

  // detectionName is a compulsory field at root level
  @Test(expectedExceptions = ConfigValidationException.class)
  public void testNoDetectionNameValidation() throws Exception {
    this.yamlConfig1.remove("detectionName");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.staticValidation(this.yaml.dump(this.yamlConfig1));
  }

  // If type is not specified at root level, then assume it to be METRIC_ALERT and proceed
  @Test
  public void testNoTypeValidation() throws Exception {
    this.yamlConfig1.remove("type");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.staticValidation(this.yaml.dump(this.yamlConfig1));
  }

  // Validates if the type field is acceptable
  @Test(expectedExceptions = ConfigValidationException.class)
  public void testInvalidTypeValidation() throws Exception {
    this.yamlConfig1.put("type", "INVALID_TYPE");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.staticValidation(this.yaml.dump(this.yamlConfig1));
  }

  @Test(expectedExceptions = ConfigValidationException.class)
  public void testNonExistingMetricValidation() throws Exception {
    this.yamlConfig1.put("metric", "metric_doesnt_exist");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.staticValidation(this.yaml.dump(this.yamlConfig1));
  }

  // Every rule must have at least 1 detection
  @Test(expectedExceptions = ConfigValidationException.class)
  public void testNoDetectionUnderRuleValidation() throws Exception {
    ((Map<String, Object>) ConfigUtils.getList(this.yamlConfig1.get("rules")).get(0)).remove("detection");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.staticValidation(this.yaml.dump(this.yamlConfig1));
  }

  @Test(expectedExceptions = ConfigValidationException.class)
  public void testNoDetectionTypeUnderRuleValidation() throws Exception {
    ((Map<String, Object>) ConfigUtils.getList(((Map<String, Object>) ConfigUtils.getList(this.yamlConfig1.get("rules")).get(0))
        .get("detection")).get(0)).remove("type");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.staticValidation(this.yaml.dump(this.yamlConfig1));
  }

  @Test(expectedExceptions = ConfigValidationException.class)
  public void testNoDetectionNameUnderRuleValidation() throws Exception {
    ((Map<String, Object>) ConfigUtils.getList(((Map<String, Object>) ConfigUtils.getList(this.yamlConfig1.get("rules")).get(0))
        .get("detection")).get(0)).remove("name");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.staticValidation(this.yaml.dump(this.yamlConfig1));
  }

  // Validate if detection/filter names are unique
  @Test(expectedExceptions = ConfigValidationException.class)
  public void testDuplicateDetectionNameUnderRuleValidation() throws Exception {
    // Update rule 1 detection name to that of rule 2 detection name
    ((Map<String, Object>) ConfigUtils.getList(
        ((Map<String, Object>) ConfigUtils.getList(this.yamlConfig1.get("rules")).get(0)).get("detection")
    ).get(0)).put("name", "maxThreshold_2");

    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.staticValidation(this.yaml.dump(this.yamlConfig1));
  }

  // Entity Example 2 Validations

  @Test
  public void testEntityAlertValidation() throws Exception {
    DetectionConfigValidator detectionConfigValidator = new DetectionConfigValidator(provider);
    detectionConfigValidator.staticValidation(this.yaml.dump(this.yamlConfig2));
  }

  @Test
  public void testGoodDetectionSchemaValidation() throws IOException, ProcessingException {
    final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
    JsonNode node = JsonLoader.fromResource("/validators/detection/detection-config-schema.json");
    final JsonSchema schema = factory.getJsonSchema(node);

    Yaml yaml = new Yaml();
    String yamlConfig = IOUtils.toString(this.getClass().getResourceAsStream("detection-config-good-1.yaml"), "UTF-8");
    Map<String, Object> configObject = ConfigUtils.getMap(yaml.load(yamlConfig));
    JsonNode config = Json.mapper().convertValue(configObject, JsonNode.class);
    ProcessingReport report = schema.validate(config);
    Assert.assertTrue(report.isSuccess());

    yamlConfig = IOUtils.toString(this.getClass().getResourceAsStream("composite-detection-config-good-1.yaml"), "UTF-8");
    configObject = ConfigUtils.getMap(yaml.load(yamlConfig));
    config = Json.mapper().convertValue(configObject, JsonNode.class);
    report = schema.validate(config);
    Assert.assertTrue(report.isSuccess());
  }

  @Test
  public void testBadMetricDetectionSchemaValidation() throws IOException, ProcessingException {
    final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
    JsonNode node = JsonLoader.fromResource("/validators/detection/detection-config-schema.json");
    final JsonSchema schema = factory.getJsonSchema(node);

    Yaml yaml = new Yaml();
    String yamlConfig = IOUtils.toString(this.getClass().getResourceAsStream("detection-config-bad-1.yaml"), "UTF-8");
    Map<String, Object> configObject = ConfigUtils.getMap(yaml.load(yamlConfig));
    JsonNode config = Json.mapper().convertValue(configObject, JsonNode.class);

    // detectionName is a required field
    ProcessingReport report = schema.validate(config);
    Assert.assertEquals(ConfigValidationUtils.getValidationMessage(report), "Validation errors: \n"
        + "- object has missing required properties ([\"detectionName\"])");
    configObject.put("detectionName", "test");  // Fix missing detectionName error

    // minContribution should be an integer
    config = Json.mapper().convertValue(configObject, JsonNode.class);
    report = schema.validate(config);
    Assert.assertEquals(ConfigValidationUtils.getValidationMessage(report), "Validation errors: \n"
        + "- instance type (string) does not match any allowed primitive type (allowed: [\"integer\",\"number\"])");
    Map<String, Object> dimExplr = ConfigUtils.getMap(configObject.get("dimensionExploration"));
    dimExplr.put("minContribution", 10);
    configObject.put("dimensionExploration", dimExplr);  // Fix minContribution error

    // unregistered fields are not supported
    configObject.put("randomField", "value_doesnt_matter");
    config = Json.mapper().convertValue(configObject, JsonNode.class);
    report = schema.validate(config);
    Assert.assertEquals(ConfigValidationUtils.getValidationMessage(report), "Validation errors: \n"
        + "- object instance has properties which are not allowed by the schema: [\"randomField\"]");
    configObject.remove("randomField"); // remove the unregistered field

    // detection incorrectly indented at the root level
    configObject.put("detection", "value_doesnt_matter");
    config = Json.mapper().convertValue(configObject, JsonNode.class);
    report = schema.validate(config);
    Assert.assertEquals(ConfigValidationUtils.getValidationMessage(report), "Validation errors: \n"
        + "- object instance has properties which are not allowed by the schema: [\"detection\"]");
    configObject.remove("detection");

    // maxDuration of anomaly merging cannot be smaller than 15 mins (900000 millisec)
    config = Json.mapper().convertValue(configObject, JsonNode.class);
    report = schema.validate(config);
    Assert.assertEquals(ConfigValidationUtils.getValidationMessage(report), "Validation errors: \n"
        + "- numeric instance is lower than the required minimum (minimum: 900000, found: 1000)");
  }

  @Test
  public void testBadEntityDetectionSchemaValidation() throws IOException, ProcessingException {
    final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
    JsonNode node = JsonLoader.fromResource("/validators/detection/detection-config-schema.json");
    final JsonSchema schema = factory.getJsonSchema(node);

    Yaml yaml = new Yaml();
    String yamlConfig = IOUtils.toString(this.getClass().getResourceAsStream("composite-detection-config-bad-1.yaml"), "UTF-8");
    Map<String, Object> configObject = ConfigUtils.getMap(yaml.load(yamlConfig));
    JsonNode config = Json.mapper().convertValue(configObject, JsonNode.class);

    ProcessingReport report = schema.validate(config);
    Assert.assertEquals(ConfigValidationUtils.getValidationMessage(report), "Validation errors: \n"
        + "- object instance has properties which are not allowed by the schema: [\"metricTypo\"]");
  }
}
