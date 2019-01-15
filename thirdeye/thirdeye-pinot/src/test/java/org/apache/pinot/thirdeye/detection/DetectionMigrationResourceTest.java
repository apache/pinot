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

package org.apache.pinot.thirdeye.detection;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.Response;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.PercentageChangeRuleDetector;
import org.apache.pinot.thirdeye.detection.components.RuleBaselineProvider;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleAnomalyFilter;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleDetector;
import org.apache.pinot.thirdeye.detection.yaml.CompositePipelineConfigTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.DetectionMigrationResource.*;
import static org.apache.pinot.thirdeye.detection.yaml.YamlDetectionAlertConfigTranslator.*;


public class DetectionMigrationResourceTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(DetectionMigrationResourceTest.class);

  private DAOTestBase testDAOProvider;
  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private AlertConfigManager alertConfigDAO;
  private ApplicationManager applicationDAO;
  private DetectionConfigManager detectionConfigDAO;
  private DetectionAlertConfigManager detectionAlertConfigDAO;

  private DetectionMigrationResource migrationResource;

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeMethod(alwaysRun = true)
  public void setup() {
    this.testDAOProvider = DAOTestBase.getInstance();
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.anomalyFunctionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();
    this.alertConfigDAO = DAORegistry.getInstance().getAlertConfigDAO();
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.detectionAlertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.applicationDAO = DAORegistry.getInstance().getApplicationDAO();

    migrationResource = new DetectionMigrationResource(
        anomalyFunctionDAO, alertConfigDAO, detectionConfigDAO, detectionAlertConfigDAO, datasetDAO, anomalyDAO);

    DetectionRegistry.registerYamlConvertor(CompositePipelineConfigTranslator.class.getName(), "COMPOSITE");
    DetectionRegistry.registerComponent(PercentageChangeRuleDetector.class.getName(), "PERCENTAGE_RULE");
    DetectionRegistry.registerComponent(RuleBaselineProvider.class.getName(), "RULE_BASELINE");
    DetectionRegistry.registerComponent(ThresholdRuleDetector.class.getName(), "THRESHOLD");
    DetectionRegistry.registerComponent(ThresholdRuleAnomalyFilter.class.getName(), "THRESHOLD_RULE_FILTER");

    DetectionAlertRegistry.getInstance().registerAlertScheme("EMAIL", "EmailClass");
    DetectionAlertRegistry.getInstance().registerAlertFilter("DEFAULT_ALERTER_PIPELINE", "RECIPIENTClass");

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setName("test_metric");
    metricConfigDTO.setDataset("test_collection");
    metricConfigDTO.setActive(true);
    metricConfigDTO.setAlias("test_collection::test_metric");
    metricDAO.save(metricConfigDTO);

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset("test_collection");
    datasetConfigDTO.setNonAdditiveBucketSize(1);
    datasetConfigDTO.setNonAdditiveBucketUnit(TimeUnit.DAYS);
    datasetDAO.save(datasetConfigDTO);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanup() {
    this.testDAOProvider.cleanup();
  }

  @Test
  public void testTranslateAlertToYaml() throws Exception {
    AnomalyFunctionDTO anomalyFunction1 = new AnomalyFunctionDTO();
    anomalyFunction1.setFunctionName("function1");
    long id1 = anomalyFunctionDAO.save(anomalyFunction1);

    AnomalyFunctionDTO anomalyFunction2 = new AnomalyFunctionDTO();
    anomalyFunction2.setFunctionName("function2");
    long id2 = anomalyFunctionDAO.save(anomalyFunction2);

    AlertConfigDTO alertConfigDTO = new AlertConfigDTO();
    alertConfigDTO.setActive(true);
    alertConfigDTO.setName("test_notification");
    alertConfigDTO.setApplication("test_application");
    alertConfigDTO.setSubjectType(AlertConfigBean.SubjectType.ALERT);
    alertConfigDTO.setCronExpression("0 0 14 * * ? *");
    alertConfigDTO.setFromAddress("test@thirdeye.com");
    alertConfigDTO.setReceiverAddresses(new DetectionAlertFilterRecipients(
        Collections.singleton("to@test"),
        Collections.singleton("cc@test"),
        Collections.singleton("bcc@test")));

    AlertConfigBean.EmailConfig emailConfig = new AlertConfigBean.EmailConfig();
    emailConfig.setFunctionIds(Arrays.asList(id1, id2));
    emailConfig.setAnomalyWatermark(9999);
    alertConfigDTO.setEmailConfig(emailConfig);

    Map<String, Object> yamlMap = migrationResource.translateAlertToYaml(alertConfigDTO);

    Assert.assertTrue(MapUtils.getBoolean(yamlMap, "active"));
    Assert.assertEquals(yamlMap.get(PROP_SUBS_GROUP_NAME), "test_notification");
    Assert.assertEquals(yamlMap.get(PROP_CRON), "0 0 14 * * ? *");
    Assert.assertEquals(yamlMap.get(PROP_APPLICATION), "test_application");
    Assert.assertEquals(yamlMap.get(PROP_EMAIL_SUBJECT_TYPE), AlertConfigBean.SubjectType.ALERT);
    Assert.assertEquals(yamlMap.get(PROP_FROM), "test@thirdeye.com");
    Assert.assertEquals(yamlMap.get(PROP_TYPE), "DEFAULT_ALERTER_PIPELINE");
    Assert.assertEquals(ConfigUtils.getList(yamlMap.get(PROP_DETECTION_NAMES)).size(), 2);
    Assert.assertNotNull(yamlMap.get(PROP_ALERT_SCHEMES));
    Assert.assertEquals(ConfigUtils.getList(yamlMap.get(PROP_ALERT_SCHEMES)).size(), 1);
    Assert.assertEquals(ConfigUtils.getMap(ConfigUtils.getList(yamlMap.get(PROP_ALERT_SCHEMES)).get(0)).get(PROP_TYPE), "EMAIL");
    Assert.assertNotNull(yamlMap.get(PROP_RECIPIENTS));
    Assert.assertEquals((ConfigUtils.getMap(yamlMap.get(PROP_RECIPIENTS)).get("to")), Collections.singleton("to@test"));
    Assert.assertEquals((ConfigUtils.getMap(yamlMap.get(PROP_RECIPIENTS)).get("cc")), Collections.singleton("cc@test"));
    Assert.assertEquals((ConfigUtils.getMap(yamlMap.get(PROP_RECIPIENTS)).get("bcc")), Collections.singleton("bcc@test"));
  }

  @Test
  public void testMigrateWoWAnomalyFunction() throws Exception {
    AnomalyFunctionDTO actual = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("legacy-anomaly-function-1.json"), AnomalyFunctionDTO.class);
    long oldID = anomalyFunctionDAO.save(actual);

    MergedAnomalyResultDTO mergedAnomalyResultDTO = new MergedAnomalyResultDTO();
    mergedAnomalyResultDTO.setFunction(actual);
    anomalyDAO.save(mergedAnomalyResultDTO);

    Response responseId = migrationResource.migrateAnomalyFunction(oldID);
    long newID = (long) responseId.getEntity();

    AnomalyFunctionDTO legacyAnomalyFunction = anomalyFunctionDAO.findById(oldID);
    DetectionConfigDTO migratedAnomalyFunction = detectionConfigDAO.findById(newID);

    // Verify if the migration status is updated correctly and the old detection is disabled.
    Assert.assertEquals(legacyAnomalyFunction.getFunctionName(), "test_function_thirdeye_migrated_" + newID);
    Assert.assertFalse(legacyAnomalyFunction.getIsActive());

    // Verify if the anomaly is pointing to the migrated anomaly function
    List<MergedAnomalyResultDTO> mergedAnomalyResultDTOList = anomalyDAO.findAll();
    Assert.assertEquals(mergedAnomalyResultDTOList.get(0).getDetectionConfigId().longValue(), newID);

    // Assert the migrated object
    DetectionConfigDTO expected = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("migrated-detection-config-1.json"), DetectionConfigDTO.class);
    expected.setId(migratedAnomalyFunction.getId());
    expected.setVersion(migratedAnomalyFunction.getVersion());
    expected.setCreatedBy(migratedAnomalyFunction.getCreatedBy());
    expected.setUpdatedBy(migratedAnomalyFunction.getUpdatedBy());
    expected.setLastTimestamp(migratedAnomalyFunction.getLastTimestamp());

    Assert.assertEquals(migratedAnomalyFunction, expected);
  }

  @Test
  public void testMinMaxAnomalyFunction() throws Exception {
    AnomalyFunctionDTO actual = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("legacy-anomaly-function-2.json"), AnomalyFunctionDTO.class);
    long oldID = anomalyFunctionDAO.save(actual);

    MergedAnomalyResultDTO mergedAnomalyResultDTO = new MergedAnomalyResultDTO();
    mergedAnomalyResultDTO.setFunction(actual);
    anomalyDAO.save(mergedAnomalyResultDTO);

    Response responseId = migrationResource.migrateAnomalyFunction(oldID);
    long newID = (long) responseId.getEntity();

    AnomalyFunctionDTO legacyAnomalyFunction = anomalyFunctionDAO.findById(oldID);
    DetectionConfigDTO migratedAnomalyFunction = detectionConfigDAO.findById(newID);

    // Verify if the migration status is updated correctly and the old detection is disabled.
    Assert.assertEquals(legacyAnomalyFunction.getFunctionName(), "test_function_thirdeye_migrated_" + newID);
    Assert.assertFalse(legacyAnomalyFunction.getIsActive());

    // Verify if the anomaly is pointing to the migrated anomaly function
    List<MergedAnomalyResultDTO> mergedAnomalyResultDTOList = anomalyDAO.findAll();
    Assert.assertEquals(mergedAnomalyResultDTOList.get(0).getDetectionConfigId().longValue(), newID);

    // Assert the migrated object
    DetectionConfigDTO expected = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("migrated-detection-config-2.json"), DetectionConfigDTO.class);
    expected.setId(migratedAnomalyFunction.getId());
    expected.setVersion(migratedAnomalyFunction.getVersion());
    expected.setCreatedBy(migratedAnomalyFunction.getCreatedBy());
    expected.setUpdatedBy(migratedAnomalyFunction.getUpdatedBy());
    expected.setLastTimestamp(migratedAnomalyFunction.getLastTimestamp());
    Assert.assertEquals(migratedAnomalyFunction, expected);
  }

  @Test
  public void testMigrateApplication() throws Exception {
    AnomalyFunctionDTO actual = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("legacy-anomaly-function-2.json"), AnomalyFunctionDTO.class);
    long id1 = anomalyFunctionDAO.save(actual);

    AlertConfigDTO alertConfigDTO = new AlertConfigDTO();
    alertConfigDTO.setActive(true);
    alertConfigDTO.setName("test_notification");
    alertConfigDTO.setApplication("test_application");
    alertConfigDTO.setSubjectType(AlertConfigBean.SubjectType.ALERT);
    alertConfigDTO.setCronExpression("0 0 14 * * ? *");
    alertConfigDTO.setFromAddress("test@thirdeye.com");
    alertConfigDTO.setReceiverAddresses(new DetectionAlertFilterRecipients(
        Collections.singleton("to@test"),
        Collections.singleton("cc@test"),
        Collections.singleton("bcc@test")));
    AlertConfigBean.EmailConfig emailConfig = new AlertConfigBean.EmailConfig();
    emailConfig.setFunctionIds(Arrays.asList(id1));
    emailConfig.setAnomalyWatermark(9999);
    alertConfigDTO.setEmailConfig(emailConfig);
    alertConfigDAO.save(alertConfigDTO);

    AlertConfigDTO alertConfigDTOMigrated = new AlertConfigDTO();
    alertConfigDTOMigrated.setActive(false);
    alertConfigDTOMigrated.setName("test_notification" + MIGRATED_TAG);
    alertConfigDTOMigrated.setApplication("test_application");
    alertConfigDTOMigrated.setSubjectType(AlertConfigBean.SubjectType.ALERT);
    alertConfigDTOMigrated.setCronExpression("0 0 14 * * ? *");
    alertConfigDTOMigrated.setFromAddress("test@thirdeye.com");
    alertConfigDTOMigrated.setReceiverAddresses(new DetectionAlertFilterRecipients(
        Collections.singleton("to@test"),
        Collections.singleton("cc@test"),
        Collections.singleton("bcc@test")));
    alertConfigDTO.setEmailConfig(emailConfig);
    alertConfigDAO.save(alertConfigDTOMigrated);

    ApplicationDTO applicationDTO = new ApplicationDTO();
    applicationDTO.setApplication("test_application");
    applicationDTO.setRecipients("test@thirdeye.com");
    applicationDAO.save(applicationDTO);

    Response response = migrationResource.migrateApplication("test_application");

    Assert.assertEquals(response.getEntity(), "Application test_application has been successfully migrated");
  }
}
