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

package org.apache.pinot.thirdeye.detection.alert.filter;

import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilter;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.alert.filter.AlertFilterUtils.*;


public class ToAllRecipientsDetectionAlertFilterTest {

  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_EMAIL_SCHEME = "emailScheme";
  private static final String PROP_JIRA_SCHEME = "jiraScheme";
  private static final String PROP_ASSIGNEE = "assignee";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";
  private static final Set<String> PROP_EMPTY_TO_VALUE = new HashSet<>();
  private static final Set<String> PROP_TO_VALUE = new HashSet<>(Arrays.asList("test@test.com", "test@test.org"));
  private static final Set<String> PROP_CC_VALUE = new HashSet<>(Arrays.asList("cctest@test.com", "cctest@test.org"));
  private static final Set<String> PROP_BCC_VALUE = new HashSet<>(Arrays.asList("bcctest@test.com", "bcctest@test.org"));
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private DAOTestBase testDAOProvider = null;

  private DetectionAlertFilter alertFilter;
  private List<MergedAnomalyResultDTO> detection1Anomalies;
  private List<MergedAnomalyResultDTO> detection2Anomalies;
  private List<MergedAnomalyResultDTO> detection3Anomalies;
  private MockDataProvider provider;
  private DetectionAlertConfigDTO alertConfig;
  private long detectionConfigId1;
  private long detectionConfigId2;
  private long detectionConfigId3;
  private long detectionConfigId;
  private long baseTime;
  private static List<Long> PROP_ID_VALUE;

  @BeforeMethod
  public void beforeMethod() throws InterruptedException {
    testDAOProvider = DAOTestBase.getInstance();

    DetectionConfigDTO detectionConfig1 = new DetectionConfigDTO();
    detectionConfig1.setName("test detection 1");
    detectionConfig1.setActive(true);
    this.detectionConfigId1 = DAORegistry.getInstance().getDetectionConfigManager().save(detectionConfig1);

    DetectionConfigDTO detectionConfig2 = new DetectionConfigDTO();
    detectionConfig2.setName("test detection 2");
    detectionConfig2.setActive(true);
    this.detectionConfigId2 = DAORegistry.getInstance().getDetectionConfigManager().save(detectionConfig2);

    DetectionConfigDTO detectionConfig3 = new DetectionConfigDTO();
    detectionConfig3.setName("test detection 3");
    detectionConfig3.setActive(true);
    this.detectionConfigId3 = DAORegistry.getInstance().getDetectionConfigManager().save(detectionConfig3);

    DetectionConfigDTO detectionConfig = new DetectionConfigDTO();
    detectionConfig.setName("test detection 0");
    detectionConfig.setActive(true);
    this.detectionConfigId = DAORegistry.getInstance().getDetectionConfigManager().save(detectionConfig);

    PROP_ID_VALUE = Arrays.asList(this.detectionConfigId1, this.detectionConfigId2);

    // Anomaly notification is tracked through create time. Start and end time doesn't matter here.
    this.detection1Anomalies = new ArrayList<>();
    this.detection2Anomalies = new ArrayList<>();
    this.detection3Anomalies = new ArrayList<>();
    this.baseTime = System.currentTimeMillis();
    Thread.sleep(100);
    this.detection1Anomalies.add(makeAnomaly(detectionConfigId1, this.baseTime,0, 100));
    this.detection2Anomalies.add(makeAnomaly(detectionConfigId2, this.baseTime,0, 100));
    Thread.sleep(50);
    this.detection2Anomalies.add(makeAnomaly(detectionConfigId2, this.baseTime,110, 150));
    Thread.sleep(50);
    this.detection1Anomalies.add(makeAnomaly(detectionConfigId1, this.baseTime,150, 200));
    Thread.sleep(250);
    this.detection2Anomalies.add(makeAnomaly(detectionConfigId2, this.baseTime,300, 450));
    this.detection3Anomalies.add(makeAnomaly(detectionConfigId3, this.baseTime,300, 450));

    Thread.sleep(100);
    this.alertConfig = createDetectionAlertConfig();
  }

  private DetectionAlertConfigDTO createDetectionAlertConfig() {
    DetectionAlertConfigDTO alertConfig = new DetectionAlertConfigDTO();

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);
    alertConfig.setProperties(properties);

    Map<String, Object> alertSchemes = new HashMap<>();
    Map<String, Object> emailScheme = new HashMap<>();
    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, PROP_TO_VALUE);
    recipients.put(PROP_CC, PROP_CC_VALUE);
    recipients.put(PROP_BCC, PROP_BCC_VALUE);
    emailScheme.put(PROP_RECIPIENTS, recipients);
    alertSchemes.put(PROP_EMAIL_SCHEME, emailScheme);
    alertConfig.setAlertSchemes(alertSchemes);

    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(detectionConfigId1, this.baseTime);
    vectorClocks.put(detectionConfigId2, this.baseTime);
    alertConfig.setVectorClocks(vectorClocks);

    return alertConfig;
  }

  private DetectionAlertConfigDTO createDetectionAlertConfigWithJira() {
    DetectionAlertConfigDTO alertConfig = new DetectionAlertConfigDTO();

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);
    alertConfig.setProperties(properties);

    Map<String, Object> alertSchemes = new HashMap<>();
    Map<String, Object> jiraScheme = new HashMap<>();
    jiraScheme.put(PROP_ASSIGNEE, "test");
    alertSchemes.put(PROP_JIRA_SCHEME, jiraScheme);
    alertConfig.setAlertSchemes(alertSchemes);

    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(detectionConfigId1, this.baseTime);
    vectorClocks.put(detectionConfigId2, this.baseTime);
    alertConfig.setVectorClocks(vectorClocks);

    return alertConfig;
  }

  /**
   * Test if all the created anomalies are picked up the the email notification filter
   */
  @Test
  public void testGetAlertFilterResult() throws Exception {
    this.alertFilter = new ToAllRecipientsDetectionAlertFilter(this.provider, this.alertConfig,this.baseTime + 350L);
    DetectionAlertFilterResult result = this.alertFilter.run();
    DetectionAlertFilterNotification notification = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);

    Assert.assertEquals(result.getResult().get(notification).size(), 4);
    Set<MergedAnomalyResultDTO> expectedAnomalies = new HashSet<>();
    expectedAnomalies.addAll(this.detection1Anomalies.subList(0, 2));
    expectedAnomalies.addAll(this.detection2Anomalies.subList(0, 2));
    Assert.assertEquals(result.getResult().get(notification), expectedAnomalies);
  }

  /**
   * Test if all the created anomalies are picked up the the jira notification filter
   */
  @Test
  public void testGetAlertFilterResultWithJira() throws Exception {
    DetectionAlertConfigDTO alertConfig = createDetectionAlertConfigWithJira();
    this.alertFilter = new ToAllRecipientsDetectionAlertFilter(this.provider, alertConfig,this.baseTime + 350L);

    DetectionAlertFilterResult result = this.alertFilter.run();
    DetectionAlertFilterNotification notification = AlertFilterUtils.makeJiraNotifications(this.alertConfig, "test");

    Assert.assertEquals(result.getResult().get(notification).size(), 4);
    Set<MergedAnomalyResultDTO> expectedAnomalies = new HashSet<>();
    expectedAnomalies.addAll(this.detection1Anomalies.subList(0, 2));
    expectedAnomalies.addAll(this.detection2Anomalies.subList(0, 2));
    Assert.assertEquals(result.getResult().get(notification), expectedAnomalies);
  }

  /**
   * Tests if the watermarks are working correctly (anomalies are not re-notified)
   */
  @Test
  public void testAlertFilterNoResend() throws Exception {
    // Assume below 2 anomalies have already been notified
    makeAnomaly(detectionConfigId1, this.baseTime, 10, 11);
    makeAnomaly(detectionConfigId1, this.baseTime, 11, 12);
    this.alertConfig.getProperties().put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(detectionConfigId1));
    this.alertConfig.setVectorClocks(Collections.singletonMap(detectionConfigId1, System.currentTimeMillis()));
    Thread.sleep(1);  // Make sure the next anomaly is not created at the same time as watermark

    // This newly detected anomaly needs to be notified to the user
    MergedAnomalyResultDTO existingFuture = makeAnomaly(detectionConfigId1, this.baseTime, 12, 13);
    Thread.sleep(1);  // Make sure the next anomaly is not created at the same time as watermark

    this.alertFilter = new ToAllRecipientsDetectionAlertFilter(this.provider, this.alertConfig, System.currentTimeMillis());

    DetectionAlertFilterResult result = this.alertFilter.run();
    DetectionAlertFilterNotification notification = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);

    Assert.assertTrue(result.getResult().containsKey(notification));
    Assert.assertEquals(result.getResult().get(notification).size(), 1);
    Assert.assertTrue(result.getResult().get(notification).contains(existingFuture));
  }

  /**
   * Test if the filter generates entries irrespective of the recipients & anomalies being present or not
   */
  @Test
  public void testGetAlertFilterResultWhenNoRecipient() throws Exception {
    Map<String, Object> properties = ConfigUtils.getMap(this.alertConfig.getProperties().get(PROP_RECIPIENTS));
    properties.put(PROP_TO, PROP_EMPTY_TO_VALUE);
    Map<String, Object> emailScheme = ConfigUtils.getMap(this.alertConfig.getAlertSchemes().get(PROP_EMAIL_SCHEME));
    Map<String, Object> recipients = ConfigUtils.getMap(emailScheme.get(PROP_RECIPIENTS));
    recipients.put(PROP_TO, PROP_EMPTY_TO_VALUE);
    emailScheme.put(PROP_RECIPIENTS, recipients);
    Map<String, Object> alertSchemes = new HashMap<>();
    alertSchemes.put(PROP_EMAIL_SCHEME, emailScheme);
    this.alertConfig.setAlertSchemes(alertSchemes);
    this.alertConfig.setProperties(properties);

    this.alertFilter = new ToAllRecipientsDetectionAlertFilter(this.provider, this.alertConfig,this.baseTime + 25L);
    DetectionAlertFilterResult result = this.alertFilter.run();

    DetectionAlertFilterNotification notification = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, new HashSet<>(), PROP_CC_VALUE, PROP_BCC_VALUE);
    Assert.assertEquals(result.getResult().size(), 1);
    Assert.assertEquals(result.getResult().get(notification), Collections.emptySet());
  }

  /**
   * Test to ensure this filter doesn't pick up anomalies with feedback (we do not want to notify them)
   */
  @Test
  public void testAlertFilterFeedback() throws Exception {
    this.alertConfig.getProperties().put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(detectionConfigId3));
    this.alertConfig.setVectorClocks(Collections.singletonMap(detectionConfigId3, this.baseTime));

    // Create feedback objects
    AnomalyFeedbackDTO feedbackAnomaly = new AnomalyFeedbackDTO();
    feedbackAnomaly.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    AnomalyFeedbackDTO feedbackNoFeedback = new AnomalyFeedbackDTO();
    feedbackNoFeedback.setFeedbackType(AnomalyFeedbackType.NO_FEEDBACK);

    // Create anomalies with various feedback type
    MergedAnomalyResultDTO anomalyWithFeedback = makeAnomaly(detectionConfigId3, this.baseTime, 5, 10, Collections.emptyMap(), feedbackAnomaly);
    MergedAnomalyResultDTO anomalyWithNoFeedback = makeAnomaly(detectionConfigId3, this.baseTime, 5, 10, Collections.emptyMap(), feedbackNoFeedback);
    MergedAnomalyResultDTO anomalyWithNullFeedback = makeAnomaly(detectionConfigId3, this.baseTime, 5, 10, Collections.emptyMap(), null);
    Thread.sleep(1);

    this.detection3Anomalies.add(anomalyWithFeedback);
    this.detection3Anomalies.add(anomalyWithNoFeedback);
    this.detection3Anomalies.add(anomalyWithNullFeedback);

    this.alertFilter = new ToAllRecipientsDetectionAlertFilter(this.provider, this.alertConfig,System.currentTimeMillis());
    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 1);

    DetectionAlertFilterNotification notification = AlertFilterUtils.makeEmailNotifications(this.alertConfig, PROP_TO_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);
    Assert.assertTrue(result.getResult().containsKey(notification));
    Assert.assertEquals(result.getResult().get(notification).size(), 3);
    // Filter should pick up all anomalies which do not have labels
    Assert.assertTrue(result.getResult().get(notification).contains(this.detection3Anomalies.get(0)));
    Assert.assertTrue(result.getResult().get(notification).contains(anomalyWithNoFeedback));
    Assert.assertTrue(result.getResult().get(notification).contains(anomalyWithNullFeedback));
    // Anomalies which have been labeled should not be picked up by the filter
    Assert.assertFalse(result.getResult().get(notification).contains(anomalyWithFeedback));
  }
}