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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilter;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.alert.filter.AlertFilterUtils.*;
import static org.apache.pinot.thirdeye.detection.alert.filter.DimensionsRecipientAlertFilter.*;


public class DimensionsRecipientAlertFilterTest {

  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";
  private static final Set<String> PROP_TO_VALUE = new HashSet<>(Arrays.asList("test@example.com", "test@example.org"));
  private static final Set<String> PROP_CC_VALUE = new HashSet<>(Arrays.asList("cctest@example.com", "cctest@example.org"));
  private static final Set<String> PROP_BCC_VALUE = new HashSet<>(Arrays.asList("bcctest@example.com", "bcctest@example.org"));
  private static final Set<String> PROP_TO_FOR_VALUE = new HashSet<>(Arrays.asList("myTest@example.com", "myTest@example.org"));
  private static final Set<String> PROP_TO_FOR_ANOTHER_VALUE = Collections.singleton("myTest@example.net");
  private static final List<Map<String, Object>> PROP_DIMENSION_RECIPIENTS_VALUE = new ArrayList<>();

  private DetectionAlertFilter alertFilter;
  private MockDataProvider provider;
  private DetectionAlertConfigDTO alertConfig;
  private DAOTestBase testDAOProvider;

  private long detectionConfigId1;
  private long detectionConfigId2;
  private long detectionConfigId3;
  private List<MergedAnomalyResultDTO> detectedAnomalies;
  private static List<Long> PROP_ID_VALUE;
  private long baseTime;

  @BeforeMethod
  public void beforeMethod() throws InterruptedException {
    testDAOProvider = DAOTestBase.getInstance();

    DetectionAlertRegistry.getInstance().registerAlertFilter("DIMENSIONS_ALERTER_PIPELINE",
        DimensionsRecipientAlertFilter.class.getName());

    ApplicationManager appDAO = DAORegistry.getInstance().getApplicationDAO();
    ApplicationDTO app = new ApplicationDTO();
    app.setApplication("test_application");
    app.setRecipients("test@thirdeye.com");
    appDAO.save(app);

    DetectionConfigManager detDAO = DAORegistry.getInstance().getDetectionConfigManager();
    DetectionConfigDTO detectionConfig1 = new DetectionConfigDTO();
    detectionConfig1.setName("test detection 1");
    detectionConfig1.setActive(true);
    this.detectionConfigId1 = detDAO.save(detectionConfig1);

    DetectionConfigDTO detectionConfig2 = new DetectionConfigDTO();
    detectionConfig2.setName("test detection 2");
    detectionConfig2.setActive(true);
    this.detectionConfigId2 = detDAO.save(detectionConfig2);

    DetectionConfigDTO detectionConfig3 = new DetectionConfigDTO();
    detectionConfig3.setName("test detection 3");
    detectionConfig3.setActive(true);
    this.detectionConfigId3 = detDAO.save(detectionConfig3);

    PROP_ID_VALUE = Arrays.asList(detectionConfigId1, detectionConfigId2);

    Map<String, Object> dimensionRecipient1 = new HashMap<>();
    Multimap<String, String> dimensionKeys1 = ArrayListMultimap.create();
    dimensionKeys1.put("key", "value");
    dimensionRecipient1.put(PROP_DIMENSION, dimensionKeys1.asMap());
    Map<String, Object> notificationValues1 = new HashMap<>();
    Map<String, Object> notificationEmailParams1 = new HashMap<>();
    Map<String, Object> recipients1 = new HashMap<>();
    recipients1.put("to", PROP_TO_FOR_VALUE);
    recipients1.put("cc", PROP_CC_VALUE);
    recipients1.put("bcc", PROP_BCC_VALUE);
    notificationEmailParams1.put(PROP_RECIPIENTS, recipients1);
    notificationValues1.put("emailScheme", notificationEmailParams1);
    dimensionRecipient1.put(PROP_NOTIFY, notificationValues1);
    Map<String, String> refLinks1 = new HashMap<>();
    refLinks1.put("link1", "value1");
    dimensionRecipient1.put(PROP_REF_LINKS, refLinks1);

    Map<String, Object> dimensionRecipient2 = new HashMap<>();
    Multimap<String, String> dimensionKeys2 = ArrayListMultimap.create();
    dimensionKeys2.put("key1", "anotherValue1");
    dimensionKeys2.put("key2", "anotherValue2");
    dimensionRecipient2.put(PROP_DIMENSION, dimensionKeys2.asMap());
    Map<String, Object> notificationValues2 = new HashMap<>();
    Map<String, Object> notificationEmailParams2 = new HashMap<>();
    Map<String, Object> recipients2 = new HashMap<>();
    recipients2.put("to", PROP_TO_FOR_ANOTHER_VALUE);
    recipients2.put("cc", PROP_CC_VALUE);
    recipients2.put("bcc", PROP_BCC_VALUE);
    notificationEmailParams2.put(PROP_RECIPIENTS, recipients2);
    notificationValues2.put("emailScheme", notificationEmailParams2);
    dimensionRecipient2.put(PROP_NOTIFY, notificationValues2);
    Map<String, String> refLinks2 = new HashMap<>();
    refLinks2.put("link2", "value2");
    dimensionRecipient2.put(PROP_REF_LINKS, refLinks2);

    PROP_DIMENSION_RECIPIENTS_VALUE.add(dimensionRecipient1);
    PROP_DIMENSION_RECIPIENTS_VALUE.add(dimensionRecipient2);

    Map<String, String> anomalousDimensions = new HashMap<>();
    anomalousDimensions.put("key1", "anotherValue1");
    anomalousDimensions.put("key2", "anotherValue2");
    anomalousDimensions.put("key3", "anotherValue3");

    this.detectedAnomalies = new ArrayList<>();
    this.baseTime = System.currentTimeMillis();
    Thread.sleep(100);
    this.detectedAnomalies.add(makeAnomaly(detectionConfigId1, this.baseTime, 0, 100, Collections.singletonMap("key", "value"), null));
    Thread.sleep(10);
    this.detectedAnomalies.add(makeAnomaly(detectionConfigId1, this.baseTime, 0, 110, anomalousDimensions, null));
    Thread.sleep(10);
    this.detectedAnomalies.add(makeAnomaly(detectionConfigId1, this.baseTime, 0, 120, Collections.singletonMap("key", "unknownValue"), null));
    Thread.sleep(30);
    this.detectedAnomalies.add(makeAnomaly(detectionConfigId2, this.baseTime, 110, 150, Collections.singletonMap("unknownKey", "value"), null));
    Thread.sleep(10);
    this.detectedAnomalies.add(makeAnomaly(detectionConfigId2, this.baseTime, 120, 160, Collections.singletonMap("key", "value"), null));
    Thread.sleep(40);
    this.detectedAnomalies.add(makeAnomaly(detectionConfigId1,this.baseTime, 150, 200, Collections.<String, String>emptyMap(), null));
    Thread.sleep(200);
    this.detectedAnomalies.add(makeAnomaly(detectionConfigId2, this.baseTime, 300, 400, Collections.singletonMap("key", "value"), null));
    this.detectedAnomalies.add(makeAnomaly(detectionConfigId3, this.baseTime, 100, 400, Collections.singletonMap("key", "value"), null));
    Thread.sleep(1);

    this.alertConfig = createDetectionAlertConfig();
  }

  private DetectionAlertConfigDTO createDetectionAlertConfig() {
    DetectionAlertConfigDTO alertConfig = new DetectionAlertConfigDTO();

    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, PROP_TO_VALUE);
    recipients.put(PROP_CC, PROP_CC_VALUE);
    recipients.put(PROP_BCC, PROP_BCC_VALUE);

    Map<String, Object> emailScheme = new HashMap<>();
    emailScheme.put(PROP_RECIPIENTS, recipients);
    alertConfig.setAlertSchemes(Collections.singletonMap("emailScheme", emailScheme));

    Map<String, Object> properties = new HashMap<>();

    properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);
    properties.put(PROP_DIMENSION_RECIPIENTS, PROP_DIMENSION_RECIPIENTS_VALUE);
    alertConfig.setProperties(properties);

    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(PROP_ID_VALUE.get(0), this.baseTime);
    vectorClocks.put(PROP_ID_VALUE.get(1), this.baseTime);
    alertConfig.setVectorClocks(vectorClocks);

    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("global_key", "global_value");
    alertConfig.setReferenceLinks(refLinks);

    return alertConfig;
  }

  /**
   * Test if the anomalies are notified to correct recipients
   */
  @Test
  public void testAlertFilterRecipients() throws Exception {
    this.alertFilter = new DimensionsRecipientAlertFilter(provider, alertConfig,this.baseTime + 350L);
    DetectionAlertFilterResult result = this.alertFilter.run();

    // Send anomalies on un-configured dimensions to default recipients
    // Anomaly 2, 3 and 5 do not fall into any of the dimensionRecipients bucket. Send them to default recipients
    DetectionAlertFilterNotification recDefault = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);
    Assert.assertEquals(result.getResult().size(), 3);
    Assert.assertEquals(result.getResult().get(recDefault).size(), 3);
    Assert.assertEquals(result.getResult().get(recDefault), makeSet(2, 3, 5));

    // Send anomalies who dimensions are configured to appropriate recipients
    DetectionAlertFilterNotification recValue = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_FOR_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);
    Multimap<String, String> dimFilters = ArrayListMultimap.create();
    dimFilters.put("key", "value");
    recValue.setDimensionFilters(dimFilters);
    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("link1", "value1");
    recValue.getSubscriptionConfig().setReferenceLinks(refLinks);
    Assert.assertTrue(result.getResult().containsKey(recValue));
    Assert.assertEquals(result.getResult().get(recValue).size(), 2);
    Assert.assertEquals(result.getResult().get(recValue), makeSet(0, 4));

    // Send alert when configured dimensions is a subset of anomaly dimensions
    // Anomaly 1 occurs on 3 dimensions (key1, key2 & key3), dimensionRecipients is configured on (key1 & key2)- send alert
    DetectionAlertFilterNotification recAnotherValue = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_FOR_ANOTHER_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);
    dimFilters.removeAll("key");
    dimFilters.put("key1", "anotherValue1");
    dimFilters.put("key2", "anotherValue2");
    recAnotherValue.setDimensionFilters(dimFilters);
    refLinks.clear();
    refLinks.put("link2", "value2");
    recAnotherValue.getSubscriptionConfig().setReferenceLinks(refLinks);
    Assert.assertTrue(result.getResult().containsKey(recAnotherValue));
    Assert.assertEquals(result.getResult().get(recAnotherValue).size(), 1);
    Assert.assertEquals(result.getResult().get(recAnotherValue), makeSet(1));
  }

  /**
   * Test to ensure the filter doesn't pick up child anomalies.
   */
  @Test
  public void testAlertFilterNoChildren() throws Exception {
    this.alertConfig.getProperties().put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(detectionConfigId2));
    this.alertFilter = new DimensionsRecipientAlertFilter(provider, alertConfig,this.baseTime + 250L);

    // Check if there are 2 anomalies in this window
    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 2);

    DetectionAlertFilterNotification recValue = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_FOR_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);
    Multimap<String, String> dimFilters = ArrayListMultimap.create();
    dimFilters.put("key", "value");
    recValue.setDimensionFilters(dimFilters);
    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("link1", "value1");
    recValue.getSubscriptionConfig().setReferenceLinks(refLinks);

    // Let's convert one of them to a child
    MergedAnomalyResultDTO anomalyResultDTO = this.detectedAnomalies.get(3);
    anomalyResultDTO.setChild(true);
    anomalyResultDTO.setDetectionConfigId(null);
    DAORegistry.getInstance().getMergedAnomalyResultDAO().update(anomalyResultDTO);

    result = this.alertFilter.run();

    // The child anomaly should not be picked up by the filter
    Assert.assertEquals(result.getResult().size(), 1);
    Assert.assertTrue(result.getResult().containsKey(recValue));
  }

  /**
   * Test to ensure labeled anomalies are filtered out and not notified
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

    this.alertFilter = new DimensionsRecipientAlertFilter(provider, alertConfig, System.currentTimeMillis());
    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 2);

    DetectionAlertFilterNotification recDefault = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);
    Assert.assertTrue(result.getResult().containsKey(recDefault));
    Assert.assertEquals(result.getResult().get(recDefault).size(), 2);
    // Filter should pick up all anomalies which do not have labels
    Assert.assertTrue(result.getResult().get(recDefault).contains(anomalyWithNoFeedback));
    Assert.assertTrue(result.getResult().get(recDefault).contains(anomalyWithNullFeedback));
    // Anomalies which have been labeled should not be picked up by the filter
    Assert.assertFalse(result.getResult().get(recDefault).contains(anomalyWithFeedback));

    DetectionAlertFilterNotification recValue = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_FOR_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);
    Multimap<String, String> dimFilters = ArrayListMultimap.create();
    dimFilters.put("key", "value");
    recValue.setDimensionFilters(dimFilters);
    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("link1", "value1");
    recValue.getSubscriptionConfig().setReferenceLinks(refLinks);
    Assert.assertTrue(result.getResult().containsKey(recValue));
    Assert.assertEquals(result.getResult().get(recValue).size(), 1);
    Assert.assertTrue(result.getResult().get(recValue).contains(this.detectedAnomalies.get(7)));
  }

  private Set<MergedAnomalyResultDTO> makeSet(int... anomalyIndices) {
    Set<MergedAnomalyResultDTO> output = new HashSet<>();
    for (int anomalyIndex : anomalyIndices) {
      output.add(this.detectedAnomalies.get(anomalyIndex));
    }
    return output;
  }
}