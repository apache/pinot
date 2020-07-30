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

package org.apache.pinot.thirdeye.detection.alert.scheme;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.mail.HtmlEmail;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.common.restclient.MockThirdEyeRcaRestClient;
import org.apache.pinot.thirdeye.common.restclient.ThirdEyeRcaRestClient;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.detection.alert.filter.SubscriptionUtils;
import org.apache.pinot.thirdeye.notification.commons.EmailEntity;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.notification.content.templates.MetricAnomaliesContent;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.alert.filter.AlertFilterUtils.*;
import static org.mockito.Mockito.*;


public class DetectionEmailAlerterTest {
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String FROM_ADDRESS_VALUE = "test3@test.test";
  private static final String ALERT_NAME_VALUE = "alert_name";
  private static final String DASHBOARD_HOST_VALUE = "dashboard";
  private static final String COLLECTION_VALUE = "test_dataset";
  private static final String DETECTION_NAME_VALUE = "test detection";
  private static final String METRIC_VALUE = "test_metric";

  private DAOTestBase testDAOProvider;
  private DetectionAlertConfigManager alertConfigDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private DetectionConfigManager detectionDAO;
  private DetectionAlertConfigDTO alertConfigDTO;
  private Long detectionConfigId;
  private ThirdEyeAnomalyConfiguration thirdEyeConfig;

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
    properties.put(PROP_CLASS_NAME, "org.apache.pinot.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    properties.put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(this.detectionConfigId));

    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, PROP_TO_VALUE);
    recipients.put(PROP_CC, PROP_CC_VALUE);
    recipients.put(PROP_BCC, PROP_BCC_VALUE);

    Map<String, Object> emailScheme = new HashMap<>();
    emailScheme.put("className", "org.apache.pinot.thirdeye.detection.alert.scheme.RandomAlerter");
    emailScheme.put(PROP_RECIPIENTS, recipients);
    this.alertConfigDTO.setAlertSchemes(Collections.singletonMap("emailScheme", emailScheme));
    this.alertConfigDTO.setProperties(properties);
    this.alertConfigDTO.setFrom(FROM_ADDRESS_VALUE);
    this.alertConfigDTO.setName(ALERT_NAME_VALUE);
    Map<Long, Long> vectorClocks = new HashMap<>();
    this.alertConfigDTO.setVectorClocks(vectorClocks);
    long id = this.alertConfigDAO.save(this.alertConfigDTO);
    this.alertConfigDTO.setId(id);

    MergedAnomalyResultDTO anomalyResultDTO = new MergedAnomalyResultDTO();
    anomalyResultDTO.setStartTime(1000L);
    anomalyResultDTO.setEndTime(2000L);
    anomalyResultDTO.setDetectionConfigId(this.detectionConfigId);
    anomalyResultDTO.setCollection(COLLECTION_VALUE);
    anomalyResultDTO.setMetric(METRIC_VALUE);
    this.anomalyDAO.save(anomalyResultDTO);

    MergedAnomalyResultDTO anomalyResultDTO2 = new MergedAnomalyResultDTO();
    anomalyResultDTO2.setStartTime(3000L);
    anomalyResultDTO2.setEndTime(4000L);
    anomalyResultDTO2.setDetectionConfigId(this.detectionConfigId);
    anomalyResultDTO2.setCollection(COLLECTION_VALUE);
    anomalyResultDTO2.setMetric(METRIC_VALUE);
    this.anomalyDAO.save(anomalyResultDTO2);

    this.thirdEyeConfig = new ThirdEyeAnomalyConfiguration();
    thirdEyeConfig.setDashboardHost(DASHBOARD_HOST_VALUE);
    Map<String, Object> smtpProperties = new HashMap<>();
    smtpProperties.put("smtpHost", "test");
    smtpProperties.put("smtpPort", 25);
    Map<String, Map<String, Object>> alerterProps = new HashMap<>();
    alerterProps.put("smtpConfiguration", smtpProperties);
    thirdEyeConfig.setAlerterConfiguration(alerterProps);
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testFailAlertWithNullResult() throws Exception {
    DetectionEmailAlerter alertTaskInfo = new DetectionEmailAlerter(this.alertConfigDTO, this.thirdEyeConfig, null);
    alertTaskInfo.run();
  }

  @Test
  public void testSendEmailSuccessful() throws Exception {
    Map<DetectionAlertFilterNotification, Set<MergedAnomalyResultDTO>> result = new HashMap<>();
    DetectionAlertConfigDTO subsConfig = SubscriptionUtils.makeChildSubscriptionConfig(
        this.alertConfigDTO,
        ConfigUtils.getMap(this.alertConfigDTO.getAlertSchemes()),
        new HashMap<>());
    result.put(
        new DetectionAlertFilterNotification(subsConfig),
        new HashSet<>(this.anomalyDAO.findAll()));
    DetectionAlertFilterResult notificationResults = new DetectionAlertFilterResult(result);

    final HtmlEmail htmlEmail = mock(HtmlEmail.class);
    when(htmlEmail.send()).thenReturn("sent");

    Map<String, Object> expectedResponse = new HashMap<>();
    ThirdEyeRcaRestClient rcaClient = MockThirdEyeRcaRestClient.setupMockClient(expectedResponse);
    MetricAnomaliesContent metricAnomaliesContent = new MetricAnomaliesContent(rcaClient);

    DetectionEmailAlerter emailAlerter = new DetectionEmailAlerter(this.alertConfigDTO, this.thirdEyeConfig, notificationResults) {
      @Override
      protected HtmlEmail getHtmlContent(EmailEntity emailEntity) {
        return htmlEmail;
      }

      @Override
      protected BaseNotificationContent getNotificationContent(Properties emailClientConfigs) {
        return metricAnomaliesContent;
      }
    };
    // Executes successfully without errors
    emailAlerter.run();
  }
}