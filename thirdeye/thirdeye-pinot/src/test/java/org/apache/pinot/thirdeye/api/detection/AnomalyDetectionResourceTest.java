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
import org.apache.pinot.thirdeye.common.metric.MetricType;
import org.apache.pinot.thirdeye.datalayer.bao.*;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.OnlineDetectionDataDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.PercentageChangeRuleAnomalyFilter;
import org.apache.pinot.thirdeye.detection.components.PercentageChangeRuleDetector;
import org.apache.pinot.thirdeye.detection.dataquality.components.DataSlaQualityChecker;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;
import java.util.Map;
import java.util.concurrent.TimeUnit;


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
  private ThirdEyePrincipal user;
  private String suffix;
  private ObjectMapper objectMapper;
  private Yaml yaml;

  @BeforeMethod
  public void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    this.user = new ThirdEyePrincipal("test", "test");
    this.daoRegistry = DAORegistry.getInstance();
    this.detectionDAO = this.daoRegistry.getDetectionConfigManager();
    this.datasetDAO = this.daoRegistry.getDatasetConfigDAO();
    this.metricDAO = this.daoRegistry.getMetricConfigDAO();
    this.objectMapper = new ObjectMapper();
    UserDashboardResource userDashboardResource = new UserDashboardResource(
        this.daoRegistry.getMergedAnomalyResultDAO(), metricDAO, datasetDAO,
        detectionDAO, this.daoRegistry.getDetectionAlertConfigManager());
    this.anomalyDetectionResource = new AnomalyDetectionResource(userDashboardResource);
    this.suffix = this.anomalyDetectionResource.generateSuffix(user);
    this.yaml = new Yaml();

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

  @Test
  public void testValidateOnlineRequestPayload() throws Exception {
    String goodPayload = IOUtils.toString(this.getClass().getResourceAsStream("payload-good.json"));
    boolean goodResult =
        this.anomalyDetectionResource.validateOnlineRequestPayload(this.objectMapper.readTree(goodPayload), false);
    Assert.assertTrue(goodResult);

    String badPayload = IOUtils.toString(this.getClass().getResourceAsStream("payload-bad.json"));
    boolean badResult =
        this.anomalyDetectionResource.validateOnlineRequestPayload(this.objectMapper.readTree(badPayload), false);
    Assert.assertFalse(badResult);
  }


  @Test
  public void testValidateOnlineDetectionData() throws Exception {
    String payload = IOUtils.toString(this.getClass().getResourceAsStream("payload-good.json"));
    JsonNode node = this.objectMapper.readTree(payload);
    boolean result1 = this.anomalyDetectionResource
        .validateOnlineDetectionData(node.get("data"), "date", "online_metric");
    Assert.assertTrue(result1);

    boolean result2 = this.anomalyDetectionResource
        .validateOnlineDetectionData(node.get("data"), "date", "metric");
    Assert.assertFalse(result2);

    boolean result3 = this.anomalyDetectionResource
        .validateOnlineDetectionData(node.get("data"), "time", "online_metric");
    Assert.assertFalse(result3);
  }

  @Test
  public void testSaveOnlineDetectionData() throws Exception {
    String suffix = this.anomalyDetectionResource.generateSuffix(this.user);

    // Test customized metric and time column names - good
    String payload = IOUtils.toString(this.getClass().getResourceAsStream("payload-good-custom.json"));
    JsonNode node = this.objectMapper.readTree(payload);
    DatasetConfigDTO datasetConfigDTO = this.anomalyDetectionResource
        .generateDatasetConfig(node, suffix);
    MetricConfigDTO metricConfigDTO = this.anomalyDetectionResource
        .generateMetricConfig(node, suffix);

    try {
      // pass
      this.anomalyDetectionResource.saveOnlineDetectionData(node, datasetConfigDTO, metricConfigDTO);
    } catch (IllegalArgumentException e) {
      Assert.fail("Unexpected exception: " + e);
    }

    suffix = this.anomalyDetectionResource.generateSuffix(this.user);
    // Test customized metric and time column names - bad
    payload = IOUtils.toString(this.getClass().getResourceAsStream("payload-bad-custom.json"));
    node = this.objectMapper.readTree(payload);
    datasetConfigDTO = this.anomalyDetectionResource
        .generateDatasetConfig(node, suffix);
    metricConfigDTO = this.anomalyDetectionResource
        .generateMetricConfig(node, suffix);
    try {
      this.anomalyDetectionResource.saveOnlineDetectionData(node, datasetConfigDTO, metricConfigDTO);
      Assert.fail("Inconsistent metric name should throw illegal arg exception");
    } catch (IllegalArgumentException e) {
      // pass
    } catch (Exception e) {
      Assert.fail("Unexpected exception: " + e);
    }
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
    JsonNode node = this.objectMapper.readTree(payload);
    MetricConfigDTO metricConfigDTO = this.anomalyDetectionResource
        .generateMetricConfig(node, this.suffix);

    // Default config names.
    Assert.assertEquals(metricConfigDTO.getName(), DEFAULT_METRIC_NAME);

    payload = IOUtils.toString(this.getClass().getResourceAsStream("payload-good-custom.json"));
    node = this.objectMapper.readTree(payload);
    metricConfigDTO = this.anomalyDetectionResource
        .generateMetricConfig(node, this.suffix);

    // Customized config names.
    Assert.assertEquals(metricConfigDTO.getName(), "rate");
  }

  @Test
  public void testGenerateDetectionConfig() throws Exception {
    String payload = IOUtils.toString(this.getClass().getResourceAsStream("payload-good.json"));
    JsonNode payloadNode = this.objectMapper.readTree(payload);

    DatasetConfigDTO datasetConfigDTO =
        this.anomalyDetectionResource.generateDatasetConfig(payloadNode, this.suffix);
    MetricConfigDTO metricConfigDTO =
        this.anomalyDetectionResource.generateMetricConfig(payloadNode, this.suffix);

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

  @Test
  public void testDoUpdateOnlineData() throws Exception {
    String initPayload = IOUtils.toString(this.getClass().getResourceAsStream("payload-good.json"));
    JsonNode initNode = this.objectMapper.readTree(initPayload);
    String updatePayload;
    JsonNode updateNode;
    OnlineDetectionDataDTO onlineDetectionDataDTO;

    // Test1: update adhoc data and dataset config
    updatePayload = IOUtils.toString(this.getClass().getResourceAsStream("payload-update-test1.json"));
    updateNode = this.objectMapper.readTree(updatePayload);
    onlineDetectionDataDTO = updateOnlineDataWithPayload(initNode, updateNode);
    checkUpdate(onlineDetectionDataDTO, "time_update", "online_metric", 3);

    // Test2: update adhoc data and metric config
    updatePayload = IOUtils.toString(this.getClass().getResourceAsStream("payload-update-test2.json"));
    updateNode = this.objectMapper.readTree(updatePayload);
    onlineDetectionDataDTO = updateOnlineDataWithPayload(initNode, updateNode);
    checkUpdate(onlineDetectionDataDTO, "date", "metric_update", 3);

    // Test3: update adhoc data and dataset config and metric config
    updatePayload = IOUtils.toString(this.getClass().getResourceAsStream("payload-update-test3.json"));
    updateNode = this.objectMapper.readTree(updatePayload);
    onlineDetectionDataDTO = updateOnlineDataWithPayload(initNode, updateNode);
    checkUpdate(onlineDetectionDataDTO, "time_update", "metric_update", 5);
  }

  @Test
  public void testTranslateMetricToYaml() {
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setTimeColumn("timeCol");
    datasetConfigDTO.setTimeUnit(TimeUnit.DAYS);
    datasetConfigDTO.setTimeDuration(1);
    datasetConfigDTO.setTimeFormat("SIMPLE_DATE_FORMAT:yyyyMMdd");
    datasetConfigDTO.setTimezone("US/Pacific");

    String datasetConfigYaml = this.anomalyDetectionResource.translateDatasetToYaml(datasetConfigDTO);

    Map<String, Object> restoredConfigs =
        ConfigUtils.getMap(yaml.load(datasetConfigYaml));

    Assert.assertTrue(restoredConfigs.containsKey("timeColumn")
        && restoredConfigs.get("timeColumn").equals("timeCol"),
        "Incorrect timeColumn: " + restoredConfigs.get("timeColumn"));

    Assert.assertTrue(restoredConfigs.containsKey("timeUnit")
        && restoredConfigs.get("timeUnit").equals("DAYS"),
        "Incorrect timeUnit: " + restoredConfigs.get("timeUnit"));

    Assert.assertTrue(restoredConfigs.containsKey("timeDuration")
        && (restoredConfigs.get("timeDuration")).equals("1"),
        "Incorrect timeDuration: " + restoredConfigs.get("timeDuration"));

    Assert.assertTrue(restoredConfigs.containsKey("timeFormat")
        && restoredConfigs.get("timeFormat").equals("SIMPLE_DATE_FORMAT:yyyyMMdd"),
        "Incorrect timeFormat: " + restoredConfigs.get("timeFormat"));

    Assert.assertTrue(restoredConfigs.containsKey("timezone")
        && restoredConfigs.get("timezone").equals("US/Pacific"),
        "Incorrect timezone: " + restoredConfigs.get("timezone"));
  }

  @Test
  public void testTranslateDatasetToYaml() {
    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setDatatype(MetricType.DOUBLE);
    metricConfigDTO.setName("metricCol");

    String metricConfigYaml = this.anomalyDetectionResource.translateMetricToYaml(metricConfigDTO);

    Map<String, Object> restoredConfigs =
        ConfigUtils.getMap(yaml.load(metricConfigYaml));

    Assert.assertTrue(restoredConfigs.containsKey("datatype")
            && restoredConfigs.get("datatype").equals("DOUBLE"),
        "Incorrect datatype: " + restoredConfigs.get("datatype"));

    Assert.assertTrue(restoredConfigs.containsKey("metricColumn")
            && restoredConfigs.get("metricColumn").equals("metricCol"),
        "Incorrect metricColumn: " + restoredConfigs.get("metricColumn"));
  }

  private OnlineDetectionDataDTO updateOnlineDataWithPayload(JsonNode initNode, JsonNode updateNode) throws Exception {
    String suffix = this.anomalyDetectionResource.generateSuffix(this.user);

    // Pre: Build and store an online data
    DatasetConfigDTO datasetConfigDTO = this.anomalyDetectionResource
        .generateDatasetConfig(initNode, suffix);

    MetricConfigDTO metricConfigDTO = this.anomalyDetectionResource
        .generateMetricConfig(initNode, suffix);

    OnlineDetectionDataDTO onlineDetectionDataDTO =
      this.anomalyDetectionResource.saveOnlineDetectionData(initNode, datasetConfigDTO, metricConfigDTO);

    // Update based on input
    onlineDetectionDataDTO = this.anomalyDetectionResource
        .doUpdateOnlineData(updateNode, datasetConfigDTO, metricConfigDTO, onlineDetectionDataDTO);

    return onlineDetectionDataDTO;
  }

  private void checkUpdate(OnlineDetectionDataDTO onlineDetectionDataDTO,
      String expectTimeColumnName, String expectMetricColumnName, int expectDataRowNum) throws Exception {
    String datasetName = onlineDetectionDataDTO.getDataset();
    DatasetConfigDTO datasetConfigDTO = datasetDAO.findByDataset(datasetName);
    Assert.assertNotNull(datasetConfigDTO);
    Assert.assertEquals(datasetConfigDTO.getTimeColumn(), expectTimeColumnName);

    String metricName = onlineDetectionDataDTO.getMetric();
    Assert.assertEquals(metricName, expectMetricColumnName);
    MetricConfigDTO metricConfigDTO = metricDAO.findByMetricAndDataset(metricName, datasetName);
    Assert.assertNotNull(metricConfigDTO);

    String onlineDetectionData = onlineDetectionDataDTO.getOnlineDetectionData();
    JsonNode dataNode = this.objectMapper.readTree(onlineDetectionData);
    Assert.assertTrue(dataNode.has("rows"));
    Assert.assertTrue(dataNode.get("rows").isArray());
    Assert.assertEquals(dataNode.get("rows").size(), expectDataRowNum);
  }
}
