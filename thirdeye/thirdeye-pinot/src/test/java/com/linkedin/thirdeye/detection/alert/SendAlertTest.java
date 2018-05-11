package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SendAlertTest {
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_RECIPIENTS = "recipients";
  private static final List<String> PROP_RECIPIENTS_VALUE = Arrays.asList("test1@test.test", "test2@test.test");
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
  private DetectionAlertConfigDTO alertConfigDTO;
  private Long alertConfigId;
  private Long detectionConfigId;

  @BeforeMethod
  public void beforeMethod() {
    this.testDAOProvider = DAOTestBase.getInstance();

    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.alertConfigDAO = daoRegistry.getDetectionAlertConfigManager();
    this.anomalyDAO = daoRegistry.getMergedAnomalyResultDAO();
    this.detectionDAO = daoRegistry.getDetectionConfigManager();

    DetectionConfigDTO detectionConfig = new DetectionConfigDTO();
    detectionConfig.setName(DETECTION_NAME_VALUE);
    this.detectionConfigId = this.detectionDAO.save(detectionConfig);

    this.alertConfigDTO = new DetectionAlertConfigDTO();

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, "com.linkedin.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    properties.put(PROP_RECIPIENTS, PROP_RECIPIENTS_VALUE);
    properties.put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(this.detectionConfigId));

    this.alertConfigDTO.setProperties(properties);
    this.alertConfigDTO.setFromAddress(FROM_ADDRESS_VALUE);
    this.alertConfigDTO.setName(ALERT_NAME_VALUE);
    Map<Long, Long> vectorClocks = new HashMap<>();
    this.alertConfigDTO.setVectorClocks(vectorClocks);

    this.alertConfigId = this.alertConfigDAO.save(this.alertConfigDTO);

    MergedAnomalyResultDTO anomalyResultDTO = new MergedAnomalyResultDTO();
    anomalyResultDTO.setStartTime(1000L);
    anomalyResultDTO.setEndTime(2000L);
    anomalyResultDTO.setDetectionConfigId(this.detectionConfigId);
    anomalyResultDTO.setCollection(COLLECTION_VALUE);
    anomalyResultDTO.setMetric(METRIC_VALUE);
    this.anomalyDAO.save(anomalyResultDTO);

    this.taskRunner = new DetectionAlertTaskRunner();
  }

  @AfterMethod(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testSendAlert() throws Exception {
    DetectionAlertTaskInfo alertTaskInfo = new DetectionAlertTaskInfo();
    alertTaskInfo.setDetectionAlertConfigId(alertConfigId);

    TaskContext taskContext = new TaskContext();
    ThirdEyeAnomalyConfiguration thirdEyeConfig = new ThirdEyeAnomalyConfiguration();
    thirdEyeConfig.setDashboardHost(DASHBOARD_HOST_VALUE);
    taskContext.setThirdEyeAnomalyConfiguration(thirdEyeConfig);

    taskRunner.execute(alertTaskInfo, taskContext);

    DetectionAlertConfigDTO alert = alertConfigDAO.findById(this.alertConfigId);
    Assert.assertTrue(alert.getVectorClocks().get(this.detectionConfigId) == 2000L);
  }
}