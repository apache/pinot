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

package org.apache.pinot.thirdeye.api.detection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.api.user.dashboard.UserDashboardResource;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.datalayer.bao.*;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.PercentageChangeRuleAnomalyFilter;
import org.apache.pinot.thirdeye.detection.components.PercentageChangeRuleDetector;
import org.apache.pinot.thirdeye.detection.dataquality.components.DataSlaQualityChecker;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.IOException;

public class AnomalyDetectionResourceTest {
  private static final String DEFAULT_DETECTION_NAME = "online_detection";
  private static final String DEFAULT_DATASET_NAME = "online_dataset";
  private static final String DEFAULT_METRIC_NAME = "online_metric";

  private DAOTestBase testDAOProvider;
  private AnomalyDetectionResource anomalyDetectionResource;
  private DAORegistry daoRegistry;
  private DetectionConfigManager detectionDAO;
  private DatasetConfigManager datasetDAO;
  private MetricConfigManager metricDAO;
  private TaskManager taskDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private ThirdEyePrincipal user;
  private String suffix;
  private ObjectMapper objectMapper;

  @BeforeMethod
  public void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    this.user = new ThirdEyePrincipal("test", "test");
    this.daoRegistry = DAORegistry.getInstance();
    this.detectionDAO = this.daoRegistry.getDetectionConfigManager();
    this.datasetDAO = this.daoRegistry.getDatasetConfigDAO();
    this.metricDAO = this.daoRegistry.getMetricConfigDAO();
    this.taskDAO = this.daoRegistry.getTaskDAO();
    anomalyDAO = this.daoRegistry.getMergedAnomalyResultDAO();
    this.suffix = "_" + this.user.getName();
    this.objectMapper = new ObjectMapper();
    UserDashboardResource userDashboardResource = new UserDashboardResource(
        this.daoRegistry.getMergedAnomalyResultDAO(), metricDAO, datasetDAO,
        detectionDAO, this.daoRegistry.getDetectionAlertConfigManager());
    this.anomalyDetectionResource = new AnomalyDetectionResource(userDashboardResource);

    DetectionRegistry
        .registerComponent(PercentageChangeRuleDetector.class.getName(), "PERCENTAGE_RULE");
    DetectionRegistry.registerComponent(PercentageChangeRuleAnomalyFilter.class.getName(),
        "PERCENTAGE_CHANGE_FILTER");
    DetectionRegistry.registerComponent(DataSlaQualityChecker.class.getName(), "DATA_SLA");
  }

  @AfterMethod(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  // TODO: will add more testings

  @Test
  public void testCleanExistingOnlineTask() {
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset(DEFAULT_DATASET_NAME + this.suffix);
    this.datasetDAO.save(datasetConfigDTO);

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setName(DEFAULT_METRIC_NAME + this.suffix);
    metricConfigDTO.setDataset(datasetConfigDTO.getDataset());
    metricConfigDTO.setAlias(ThirdEyeUtils
        .constructMetricAlias(datasetConfigDTO.getDataset(), metricConfigDTO.getName()));
    this.metricDAO.save(metricConfigDTO);

    DetectionConfigDTO detectionConfigDTO = new DetectionConfigDTO();
    detectionConfigDTO.setName(DEFAULT_DETECTION_NAME + this.suffix);

    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setJobName(TaskConstants.TaskType.DETECTION + this.suffix);
    taskDTO.setStatus(TaskConstants.TaskStatus.FAILED);
    taskDTO.setTaskType(TaskConstants.TaskType.DETECTION_ONLINE);
    this.taskDAO.save(taskDTO);

    MergedAnomalyResultDTO anomalyResultDTO = new MergedAnomalyResultDTO();
    anomalyResultDTO.setMetric(metricConfigDTO.getName());
    long anomalyId1 = anomalyDAO.save(anomalyResultDTO);

    anomalyResultDTO.setCollection(datasetConfigDTO.getName());
    anomalyResultDTO.setMetric(null);
    anomalyResultDTO.setId(null);
    long anomalyId2 = anomalyDAO.save(anomalyResultDTO);

    this.anomalyDetectionResource.cleanExistingOnlineTask(this.suffix);

    Assert.assertNull(this.datasetDAO.findById(datasetConfigDTO.getId()));
    Assert.assertNull(this.metricDAO.findById(metricConfigDTO.getId()));
    Assert.assertNull(this.taskDAO.findById(taskDTO.getId()));
    Assert.assertNull(this.anomalyDAO.findById(anomalyId1));
    Assert.assertNull(this.anomalyDAO.findById(anomalyId2));
  }

  @Test
  public void testValidateOnlineRequestPayload() throws Exception {
    String goodPayload = IOUtils.toString(this.getClass().getResourceAsStream("payload-good.json"));
    boolean goodResult =
        this.anomalyDetectionResource.validateOnlineRequestPayload(this.objectMapper.readTree(goodPayload));
    Assert.assertTrue(goodResult);

    String badPayload = IOUtils.toString(this.getClass().getResourceAsStream("payload-bad.json"));
    boolean badResult =
        this.anomalyDetectionResource.validateOnlineRequestPayload(this.objectMapper.readTree(badPayload));
    Assert.assertFalse(badResult);
  }

  @Test
  public void testGenerateDatasetConfig() throws Exception {
    String payload = IOUtils.toString(this.getClass().getResourceAsStream("payload-good.json"));

    DatasetConfigDTO datasetConfigDTO = this.anomalyDetectionResource
        .generateDatasetConfig(this.objectMapper.readTree(payload), this.suffix);

    // Do not support customized config names. Test this
    Assert.assertEquals(datasetConfigDTO.getDataset(), DEFAULT_DATASET_NAME + this.suffix);
  }

  @Test
  public void testGenerateMetricConfig() throws Exception {
    String payload = IOUtils.toString(this.getClass().getResourceAsStream("payload-good.json"));

    MetricConfigDTO metricConfigDTO = this.anomalyDetectionResource
        .generateMetricConfig(this.objectMapper.readTree(payload), this.suffix);

    // Do not support customized config names. Test this
    Assert.assertEquals(metricConfigDTO.getName(), DEFAULT_METRIC_NAME + this.suffix);
    Assert.assertNotNull(metricConfigDTO.getOnlineData());
  }

  @Test
  public void testGenerateDetectionConfig() throws IOException {
    String payload = IOUtils.toString(this.getClass().getResourceAsStream("payload-good.json"));
    JsonNode payloadNode = this.objectMapper.readTree(payload);

    MetricConfigDTO metricConfigDTO =
        this.anomalyDetectionResource.generateMetricConfig(payloadNode, this.suffix);

    DatasetConfigDTO datasetConfigDTO =
        this.anomalyDetectionResource.generateDatasetConfig(payloadNode, this.suffix);

    DetectionConfigDTO detectionConfigDTO = this.anomalyDetectionResource
        .generateDetectionConfig(payloadNode, this.suffix, datasetConfigDTO, metricConfigDTO, 0, 0);

    // Do not support customized config names. Test this
    Assert.assertEquals(detectionConfigDTO.getName(), DEFAULT_DETECTION_NAME + this.suffix);
  }

  @Test
  public void testGenerateTaskConfig() throws JsonProcessingException {
    long dummyId = 123456L;
    DetectionConfigDTO detectionConfigDTO = new DetectionConfigDTO();
    detectionConfigDTO.setId(dummyId);
    TaskDTO taskDTO = this.anomalyDetectionResource
        .generateTaskConfig(detectionConfigDTO, 0, 0, this.suffix);

    Assert.assertEquals(taskDTO.getTaskType(), TaskConstants.TaskType.DETECTION_ONLINE);
    Assert.assertEquals(taskDTO.getStatus(), TaskConstants.TaskStatus.WAITING);
  }
}
