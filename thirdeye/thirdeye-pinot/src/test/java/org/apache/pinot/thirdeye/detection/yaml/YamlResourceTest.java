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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.dashboard.DetectionPreviewConfiguration;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleDetector;
import org.apache.pinot.thirdeye.detection.validators.ConfigValidationException;
import org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class YamlResourceTest {
  private DAOTestBase testDAOProvider;
  private YamlResource yamlResource;
  private DAORegistry daoRegistry;
  private ThirdEyePrincipal user;
  private static long alertId1;
  private static long alertId2;

  private static final String MISSING_FIELD_PREFIX_TEMPLATE = "Validation errors: \n"
      + "- object has missing required properties ([%s])";

  @BeforeMethod
  public void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    this.user = new ThirdEyePrincipal("test", "test");
    this.yamlResource = new YamlResource(null, new DetectionPreviewConfiguration());
    this.daoRegistry = DAORegistry.getInstance();
    DetectionConfigManager detectionDAO = this.daoRegistry.getDetectionConfigManager();
    DetectionConfigDTO config1 = new DetectionConfigDTO();
    config1.setName("test_detection_1");
    alertId1 = detectionDAO.save(config1);
    DetectionConfigDTO config2 = new DetectionConfigDTO();
    config2.setName("test_detection_2");
    alertId2 = detectionDAO.save(config2);

    ApplicationDTO app = new ApplicationDTO();
    app.setApplication("test_application");
    app.setRecipients("test");
    this.daoRegistry.getApplicationDAO().save(app);

    DetectionRegistry.getInstance().registerYamlConvertor(DetectionConfigTranslator.class.getName(), "COMPOSITE");
    DetectionRegistry.registerComponent(ThresholdRuleDetector.class.getName(), "THRESHOLD");
    DetectionAlertRegistry.getInstance().registerAlertScheme("EMAIL", "EmailClass");
    DetectionAlertRegistry.getInstance().registerAlertScheme("IRIS", "IrisClass");
    DetectionAlertRegistry.getInstance().registerAlertSuppressor("TIME_WINDOW", "TimeWindowClass");
    DetectionAlertRegistry.getInstance().registerAlertFilter("DIMENSIONAL_ALERTER_PIPELINE", "DimClass");
    DetectionAlertRegistry.getInstance().registerAlertFilter("DEFAULT_ALERTER_PIPELINE", "DefClass");
  }

  @AfterMethod(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testValidateCreateAlertYaml() throws IOException {
    // No validation error should be thrown for valid yaml payload
    String detectionPayload = IOUtils.toString(this.getClass().getResourceAsStream("detection/detection-config-2.yaml"));
    String subscriptionPayload = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-5.yaml"));
    Map<String, String> config = new HashMap<>();
    config.put("detection", detectionPayload);
    config.put("subscription", subscriptionPayload);
    try {
      this.yamlResource.validateCreateAlertYaml(config);
    } catch (Exception e) {
      Assert.fail("No exception should be thrown for valid payload");
      return;
    }

    // Throw error if the subscription group is not subscribed to the detection
    detectionPayload = IOUtils.toString(this.getClass().getResourceAsStream("detection/detection-config-2.yaml"));
    subscriptionPayload = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-2.yaml"));
    config.put("detection", detectionPayload);
    config.put("subscription", subscriptionPayload);
    try {
      this.yamlResource.validateCreateAlertYaml(config);
    } catch (ConfigValidationException e) {
      Assert.assertEquals(e.getMessage(), "You have not subscribed to the alert. Please configure the"
          + " detectionName under the subscribedDetections field in your subscription group.");
      return;
    }
    Assert.fail("Since the subscription group has not subscribed to the alert, an error should have been thrown.");
  }

  @Test
  public void testCreateOrUpdateDetectionConfig() throws IOException {
    String blankYaml = "";
    try {
      this.yamlResource.createOrUpdateDetectionConfig(user, blankYaml);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "detectionName cannot be left empty");
    }

    MetricConfigDTO metricConfig = new MetricConfigDTO();
    metricConfig.setAlias("test_alias");
    metricConfig.setName("test_metric");
    metricConfig.setDataset("test_dataset");
    daoRegistry.getMetricConfigDAO().save(metricConfig);

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset("test_dataset");
    datasetConfigDTO.setTimeUnit(TimeUnit.DAYS);
    datasetConfigDTO.setTimeDuration(1);
    datasetConfigDTO.setDataSource(PinotThirdEyeDataSource.class.getSimpleName());
    daoRegistry.getDatasetConfigDAO().save(datasetConfigDTO);

    // Create a new detection
    String validYaml = IOUtils.toString(this.getClass().getResourceAsStream("detection/detection-config-1.yaml"));
    try {
      long id = this.yamlResource.createOrUpdateDetectionConfig(user, validYaml);
      DetectionConfigDTO detection = daoRegistry.getDetectionConfigManager().findById(id);
      Assert.assertNotNull(detection);
      Assert.assertEquals(detection.getName(), "test_detection_1");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown for valid yaml. Message: " + e + " Cause: " + e.getCause(), e);
    }

    // Update above created detection
    String updatedYaml = IOUtils.toString(this.getClass().getResourceAsStream("detection/detection-config-2.yaml"));
    try {
      long id = this.yamlResource.createOrUpdateDetectionConfig(user, updatedYaml);
      DetectionConfigDTO detection = daoRegistry.getDetectionConfigManager().findById(id);
      Assert.assertNotNull(detection);
      Assert.assertEquals(detection.getName(), "test_detection_2");
      Assert.assertEquals(detection.getDescription(), "My modified pipeline");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown if detection already exists. Message: " + e + " Cause: " + e.getCause());
    }
  }

  @Test
  public void testCreateOrUpdateSubscriptionConfig() throws IOException {
    // Create a new subscription
    String validYaml = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-2.yaml"));
    try {
      long id = this.yamlResource.createOrUpdateSubscriptionConfig(user, validYaml);
      DetectionAlertConfigDTO subscription = daoRegistry.getDetectionAlertConfigManager().findById(id);
      Assert.assertNotNull(subscription);
      Assert.assertEquals(subscription.getName(), "Subscription Group Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown for valid yaml. Message: " + e + " Cause: " + e.getCause(), e);
    }

    // Update above created subscription based on subscriptionGroupName
    String updatedYaml = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-4.yaml"));
    try {
      long id = this.yamlResource.createOrUpdateSubscriptionConfig(user, updatedYaml);
      DetectionAlertConfigDTO subscription = daoRegistry.getDetectionAlertConfigManager().findById(id);
      Assert.assertNotNull(subscription);
      Assert.assertEquals(subscription.getName(), "Subscription Group Name");
      Assert.assertEquals(subscription.getApplication(), "test_application");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown if subscription already exists. Message: " + e + " Cause: " + e.getCause());
    }
  }

  @Test
  public void testCreateSubscriptionConfig() throws IOException {
    String blankYaml = "";
    try {
      this.yamlResource.createSubscriptionConfig(blankYaml);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (ConfigValidationException e) {
      Assert.assertEquals(e.getMessage(), String.format(MISSING_FIELD_PREFIX_TEMPLATE,
          "\"alertSchemes\",\"application\",\"subscribedDetections\",\"subscriptionGroupName\""));
    }

    String inValidYaml = "application:test:application";
    try {
      this.yamlResource.createSubscriptionConfig(inValidYaml);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Could not parse as map: application:test:application");
    }

    // Error should be thrown if subscriptionGroupName is not defined
    String noSubscriptGroupYaml = "application: test_application\nsubscribedDetections:\n- test_detection_1\n";
    try {
      this.yamlResource.createSubscriptionConfig(noSubscriptGroupYaml);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), String.format(MISSING_FIELD_PREFIX_TEMPLATE,
          "\"alertSchemes\",\"subscriptionGroupName\""));
    }

    String appFieldMissingYaml = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-1.yaml"));
    try {
      this.yamlResource.createSubscriptionConfig(appFieldMissingYaml);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), String.format(MISSING_FIELD_PREFIX_TEMPLATE,
          "\"application\""));
    }

    String appMissingYaml = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-6.yaml"));
    try {
      this.yamlResource.createSubscriptionConfig(appMissingYaml);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Application name doesn't exist in our registry."
          + " Please use an existing application name or reach out to the ThirdEye team to setup a new one.");
    }

    DetectionAlertConfigDTO oldAlertDTO = new DetectionAlertConfigDTO();
    oldAlertDTO.setName("test_group");
    daoRegistry.getDetectionAlertConfigManager().save(oldAlertDTO);

    ApplicationDTO request = new ApplicationDTO();
    request.setApplication("test_application");
    request.setRecipients("abc@abc.in");
    daoRegistry.getApplicationDAO().save(request);

    String groupExists = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-3.yaml"));
    try {
      this.yamlResource.createSubscriptionConfig(groupExists);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "subscriptionGroupName is already taken. Please use a different one.");
    }

    String validYaml = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-4.yaml"));
    try {
      long id = this.yamlResource.createSubscriptionConfig(validYaml);
      DetectionConfigDTO detection = daoRegistry.getDetectionConfigManager().findById(id);
      Assert.assertNotNull(detection);
      Assert.assertEquals(detection.getName(), "Subscription Group Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown for valid yaml. Message = " + e);
    }
  }

  @Test
  public void testUpdateSubscriptionConfig() throws IOException {
    ApplicationDTO request = new ApplicationDTO();
    request.setApplication("test_application");
    request.setRecipients("abc@abc.in");
    daoRegistry.getApplicationDAO().save(request);

    String validYaml = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-4.yaml"));
    long oldId = -1;
    try {
      oldId = this.yamlResource.createSubscriptionConfig(validYaml);
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown for valid yaml. Message = " + e);
    }

    DetectionAlertConfigDTO alertDTO;

    try {
      this.yamlResource.updateSubscriptionGroup(user, -1, "");
      Assert.fail("Exception not thrown when the subscription group doesn't exist");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Cannot find subscription group -1");
    }

    String blankYaml = "";
    try {
      this.yamlResource.updateSubscriptionGroup(user, oldId, blankYaml);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), String.format(MISSING_FIELD_PREFIX_TEMPLATE,
          "\"alertSchemes\",\"application\",\"subscribedDetections\",\"subscriptionGroupName\""));
    }

    String inValidYaml = "application:test:application";
    try {
      this.yamlResource.updateSubscriptionGroup(user, oldId, inValidYaml);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Could not parse as map: application:test:application");
    }

    // Error should be thrown if no subscriptionGroupName is specified
    String noSubscriptGroupYaml = "application: test_app\nsubscribedDetections:\n- test_detection_1";
    try {
      this.yamlResource.updateSubscriptionGroup(user, oldId, noSubscriptGroupYaml);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), String.format(MISSING_FIELD_PREFIX_TEMPLATE,
          "\"alertSchemes\",\"subscriptionGroupName\""));
    }

    String appFieldMissingYaml = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-1.yaml"));
    try {
      this.yamlResource.updateSubscriptionGroup(user, oldId, appFieldMissingYaml);
      Assert.fail("Exception not thrown on empty yaml");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), String.format(MISSING_FIELD_PREFIX_TEMPLATE,
          "\"application\""));
    }

    String validYaml2 = IOUtils.toString(this.getClass().getResourceAsStream("subscription/subscription-config-5.yaml"));
    try {
      this.yamlResource.updateSubscriptionGroup(user, oldId, validYaml2);
      alertDTO = daoRegistry.getDetectionAlertConfigManager().findById(oldId);
      Assert.assertNotNull(alertDTO);
      Assert.assertEquals(alertDTO.getName(), "Subscription Group Name");
      Assert.assertEquals(alertDTO.getApplication(), "test_application");
      Assert.assertNotNull(alertDTO.getAlertSchemes().get("emailScheme"));
      Assert.assertEquals(ConfigUtils.getMap(alertDTO.getAlertSchemes().get("emailScheme")).get("template"), "ENTITY_GROUPBY_REPORT");
      Assert.assertEquals(ConfigUtils.getMap(alertDTO.getAlertSchemes().get("emailScheme")).get("subject"), "METRICS");

      // Verify if the vector clock is updated with the updated detection
      Assert.assertEquals(alertDTO.getVectorClocks().keySet().size(), 1);
      Assert.assertEquals(alertDTO.getVectorClocks().keySet().toArray()[0], alertId2);
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown for valid yaml. Message = " + e);
    }
  }
}

