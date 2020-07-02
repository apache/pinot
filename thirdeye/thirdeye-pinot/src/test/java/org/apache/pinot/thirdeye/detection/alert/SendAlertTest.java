/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.detection.alert;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SendAlertTest {
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String FROM_ADDRESS_VALUE = "test3@test.test";
  private static final String ALERT_NAME_VALUE = "alert_name";
  private static final String DASHBOARD_HOST_VALUE = "dashboard";
  private static final String COLLECTION_VALUE = "test_dataset";
  private static final String DETECTION_NAME_VALUE = "test detection";
  private static final String METRIC_VALUE = "test_metric";

  private DAOTestBase testDAOProvider;
  private DetectionAlertTaskRunner taskRunner;
  private DetectionAlertConfigManager alertConfigDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private DetectionConfigManager detectionDAO;
  private MetricConfigManager metricDAO;
  private DatasetConfigManager dataSetDAO;
  private DetectionAlertConfigDTO alertConfigDTO;
  private Long alertConfigId;
  private Long detectionConfigId;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    this.testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.alertConfigDAO = daoRegistry.getDetectionAlertConfigManager();
    this.anomalyDAO = daoRegistry.getMergedAnomalyResultDAO();
    this.detectionDAO = daoRegistry.getDetectionConfigManager();
    this.metricDAO = daoRegistry.getMetricConfigDAO();
    this.dataSetDAO = daoRegistry.getDatasetConfigDAO();

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setName(METRIC_VALUE);
    metricConfigDTO.setDataset(COLLECTION_VALUE);
    metricConfigDTO.setAlias(COLLECTION_VALUE + ":" + METRIC_VALUE);
    this.metricDAO.save(metricConfigDTO);

    DetectionConfigDTO detectionConfig = new DetectionConfigDTO();
    detectionConfig.setName(DETECTION_NAME_VALUE);
    detectionConfig.setActive(true);
    this.detectionConfigId = this.detectionDAO.save(detectionConfig);

    this.alertConfigDTO = new DetectionAlertConfigDTO();
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, "org.apache.pinot.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    properties.put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(this.detectionConfigId));

    Map<String, Object> emailScheme = new HashMap<>();
    emailScheme.put("className", "org.apache.pinot.thirdeye.detection.alert.scheme.RandomAlerter");
    this.alertConfigDTO.setAlertSchemes(Collections.singletonMap("emailScheme", emailScheme));
    this.alertConfigDTO.setProperties(properties);
    this.alertConfigDTO.setFrom(FROM_ADDRESS_VALUE);
    this.alertConfigDTO.setName(ALERT_NAME_VALUE);
    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(this.detectionConfigId, 0l);
    this.alertConfigDTO.setVectorClocks(vectorClocks);
    this.alertConfigId = this.alertConfigDAO.save(this.alertConfigDTO);

    MergedAnomalyResultDTO anomalyResultDTO = new MergedAnomalyResultDTO();
    anomalyResultDTO.setStartTime(1000L);
    anomalyResultDTO.setEndTime(2000L);
    anomalyResultDTO.setDetectionConfigId(this.detectionConfigId);
    anomalyResultDTO.setCollection(COLLECTION_VALUE);
    anomalyResultDTO.setMetric(METRIC_VALUE);
    this.anomalyDAO.save(anomalyResultDTO);

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset(COLLECTION_VALUE);
    this.dataSetDAO.save(datasetConfigDTO);

    this.taskRunner = new DetectionAlertTaskRunner();
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testSendAlert() throws Exception {
    DetectionAlertTaskInfo alertTaskInfo = new DetectionAlertTaskInfo();
    alertTaskInfo.setDetectionAlertConfigId(alertConfigId);

    Map<String, Object> smtpProperties = new HashMap<>();
    smtpProperties.put("smtpHost", "test");
    smtpProperties.put("smtpPort", 25);
    Map<String, Map<String, Object>> alerterProps = new HashMap<>();
    alerterProps.put("smtpConfiguration", smtpProperties);

    ThirdEyeAnomalyConfiguration thirdEyeConfig = new ThirdEyeAnomalyConfiguration();
    thirdEyeConfig.setDashboardHost(DASHBOARD_HOST_VALUE);
    thirdEyeConfig.setAlerterConfiguration(alerterProps);

    TaskContext taskContext = new TaskContext();
    taskContext.setThirdEyeAnomalyConfiguration(thirdEyeConfig);

    taskRunner.execute(alertTaskInfo, taskContext);

    DetectionAlertConfigDTO alert = alertConfigDAO.findById(this.alertConfigId);
    Assert.assertTrue((long) alert.getVectorClocks().get(this.detectionConfigId) > 0);
  }
}