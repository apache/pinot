package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskInfo;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.anomaly.alert.util.EmailHelper.*;


public class SendAlertTest {
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_RECIPIENTS = "recipients";
  private static final List<String> PROP_RECIPIENTS_VALUE = Arrays.asList("test1@test.test", "test2@test.test");
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String FROM_ADDRESS_VALUE = "test3@test.test";
  private static final String ALERT_NAME_VALUE = "alert_name";
  private static final String DASHBOARD_HOST_VALUE = "dashboard";
  private static final String APPLICATION_VALUE = "test_application";
  private static final String COLLECTION_VALUE = "test_dataset";
  private static final String DETECTION_NAME_VALUE = "test detection";
  private static final String METRIC_VALUE = "test_metric";

  private DAOTestBase testDAOProvider;
  private AlertTaskRunnerV2 taskRunner;
  private AlertConfigManager alertConfigDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private DetectionConfigManager detectionDAO;
  private AlertConfigDTO alertConfigDTO;

  @BeforeMethod
  public void beforeMethod() {
    this.testDAOProvider = DAOTestBase.getInstance();

    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.alertConfigDAO = daoRegistry.getAlertConfigDAO();
    this.anomalyDAO = daoRegistry.getMergedAnomalyResultDAO();
    this.detectionDAO = daoRegistry.getDetectionConfigManager();

    DetectionConfigDTO detectionConfig = new DetectionConfigDTO();
    detectionConfig.setName(DETECTION_NAME_VALUE);
    Long detectionConfigId = this.detectionDAO.save(detectionConfig);

    this.alertConfigDTO = new AlertConfigDTO();

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, "com.linkedin.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    properties.put(PROP_RECIPIENTS, PROP_RECIPIENTS_VALUE);
    properties.put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(detectionConfigId));

    this.alertConfigDTO.setProperties(properties);
    this.alertConfigDTO.setFromAddress(FROM_ADDRESS_VALUE);
    this.alertConfigDTO.setName(ALERT_NAME_VALUE);
    this.alertConfigDTO.setApplication(APPLICATION_VALUE);
    this.alertConfigDTO.setLastTimeStamp(0L);

    this.alertConfigDAO.save(this.alertConfigDTO);

    MergedAnomalyResultDTO anomalyResultDTO = new MergedAnomalyResultDTO();
    anomalyResultDTO.setStartTime(1000L);
    anomalyResultDTO.setEndTime(2000L);
    anomalyResultDTO.setDetectionConfigId(detectionConfigId);
    anomalyResultDTO.setCollection(COLLECTION_VALUE);
    anomalyResultDTO.setMetric(METRIC_VALUE);
    this.anomalyDAO.save(anomalyResultDTO);

    this.taskRunner = new AlertTaskRunnerV2();
  }

  @AfterMethod(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testSendAlert() throws Exception {
    AlertTaskInfo alertTaskInfo = new AlertTaskInfo();
    alertTaskInfo.setAlertConfigDTO(alertConfigDTO);

    TaskContext taskContext = new TaskContext();
    ThirdEyeAnomalyConfiguration thirdEyeConfig = new ThirdEyeAnomalyConfiguration();
    thirdEyeConfig.setDashboardHost(DASHBOARD_HOST_VALUE);
    taskContext.setThirdEyeAnomalyConfiguration(thirdEyeConfig);

    taskRunner.execute(alertTaskInfo, taskContext);
  }
}