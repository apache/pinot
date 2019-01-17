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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.yaml.YamlDetectionAlertConfigTranslator.*;


public class DetectionMigrationResourceTest {

  private DAOTestBase testDAOProvider;
  private DatasetConfigManager datasetDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private AlertConfigManager alertConfigDAO;
  private DetectionConfigManager detectionConfigDAO;
  private DetectionAlertConfigManager detectionAlertConfigDAO;

  private DetectionMigrationResource migrationResource;

  @BeforeMethod
  public void beforeMethod() {
    this.testDAOProvider = DAOTestBase.getInstance();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.anomalyFunctionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();
    this.alertConfigDAO = DAORegistry.getInstance().getAlertConfigDAO();
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.detectionAlertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();

    migrationResource = new DetectionMigrationResource(anomalyFunctionDAO, detectionConfigDAO, datasetDAO);
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    this.testDAOProvider.cleanup();
  }

  @Test
  public void testTranslateAlertToYaml() throws Exception {
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
    emailConfig.setFunctionIds(Arrays.asList(1l, 2l, 3l, 4l));
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
    Assert.assertEquals(yamlMap.get(PROP_DETECTION_CONFIG_IDS), Arrays.asList(1l, 2l, 3l, 4l));
    Assert.assertNotNull(yamlMap.get(PROP_ALERT_SCHEMES));
    Assert.assertEquals(((Map<String, Object>) yamlMap.get(PROP_ALERT_SCHEMES)).get(PROP_TYPE), "EMAIL");
    Assert.assertNotNull(yamlMap.get(PROP_RECIPIENTS));
    Assert.assertEquals(((Map<String, Object>) yamlMap.get(PROP_RECIPIENTS)).get("to"), Collections.singleton("to@test"));
    Assert.assertEquals(((Map<String, Object>) yamlMap.get(PROP_RECIPIENTS)).get("cc"), Collections.singleton("cc@test"));
    Assert.assertEquals(((Map<String, Object>) yamlMap.get(PROP_RECIPIENTS)).get("bcc"), Collections.singleton("bcc@test"));
  }
}