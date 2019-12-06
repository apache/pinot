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
import java.io.IOException;
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

import static org.apache.pinot.thirdeye.detection.DetectionTestUtils.*;
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
  private static final List<Long> PROP_ID_VALUE = new ArrayList<>();
  private static final List<Map<String, Object>> PROP_DIMENSION_RECIPIENTS_VALUE = new ArrayList<>();

  private DetectionAlertFilter alertFilter;
  private List<MergedAnomalyResultDTO> detectedAnomalies;
  private MockDataProvider provider;
  private DetectionAlertConfigDTO alertConfig;
  private DAOTestBase testDAOProvider;

  private long detectionId1;
  private long detectionId2;
  private long detectionId3;

  @BeforeMethod
  public void beforeMethod() {
    testDAOProvider = DAOTestBase.getInstance();

    DetectionAlertRegistry.getInstance().registerAlertFilter("DIMENSIONS_ALERTER_PIPELINE",
        DimensionsRecipientAlertFilter.class.getName());

    ApplicationManager appDAO = DAORegistry.getInstance().getApplicationDAO();
    ApplicationDTO app = new ApplicationDTO();
    app.setApplication("test_application");
    app.setRecipients("test@thirdeye.com");
    appDAO.save(app);

    DetectionConfigManager detDAO = DAORegistry.getInstance().getDetectionConfigManager();
    DetectionConfigDTO detection1 = new DetectionConfigDTO();
    detection1.setName("test_detection_1");
    this.detectionId1 = detDAO.save(detection1);

    DetectionConfigDTO detection2 = new DetectionConfigDTO();
    detection2.setName("test_detection_2");
    this.detectionId2 = detDAO.save(detection2);

    DetectionConfigDTO detection3 = new DetectionConfigDTO();
    detection3.setName("test_detection_3");
    this.detectionId3 = detDAO.save(detection3);

    PROP_ID_VALUE.add(detectionId1);
    PROP_ID_VALUE.add(detectionId2);

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
    this.detectedAnomalies.add(makeAnomaly(detectionId1, 1500, 2000, Collections.<String, String>emptyMap()));
    this.detectedAnomalies.add(makeAnomaly(detectionId1,0, 1000, Collections.singletonMap("key", "value")));
    this.detectedAnomalies.add(makeAnomaly(detectionId1,0, 1100, anomalousDimensions));
    this.detectedAnomalies.add(makeAnomaly(detectionId1,0, 1200, Collections.singletonMap("key", "unknownValue")));
    this.detectedAnomalies.add(makeAnomaly(detectionId2,1100, 1500, Collections.singletonMap("unknownKey", "value")));
    this.detectedAnomalies.add(makeAnomaly(detectionId2,1200, 1600, Collections.singletonMap("key", "value")));
    this.detectedAnomalies.add(makeAnomaly(detectionId2,3333, 9999, Collections.singletonMap("key", "value")));
    this.detectedAnomalies.add(makeAnomaly(detectionId3,1111, 9999, Collections.singletonMap("key", "value")));

    this.provider = new MockDataProvider()
        .setAnomalies(this.detectedAnomalies);

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
    vectorClocks.put(PROP_ID_VALUE.get(0), 0L);
    alertConfig.setVectorClocks(vectorClocks);

    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("global_key", "global_value");
    alertConfig.setReferenceLinks(refLinks);

    return alertConfig;
  }

  @Test
  public void testAlertFilterRecipients() throws Exception {
    this.alertFilter = new DimensionsRecipientAlertFilter(provider, alertConfig,2500L);

    DetectionAlertFilterResult result = this.alertFilter.run();

    // Send anomalies on un-configured dimensions to default recipients
    // Anomaly 0, 3 and 4 do not fall into any of the dimensionRecipients bucket. Send them to default recipients
    DetectionAlertFilterNotification recDefault = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);
    Assert.assertEquals(result.getResult().get(recDefault), makeSet(0, 3, 4));

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
    Assert.assertEquals(result.getResult().get(recValue), makeSet(1, 5));

    // Send alert when configured dimensions is a subset of anomaly dimensions
    // Anomaly 2 occurs on 3 dimensions (key 1, 2 & 3), dimensionRecipients is configured on (key 1 & 2) - send alert
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
    Assert.assertEquals(result.getResult().get(recAnotherValue), makeSet(2));
  }

  @Test
  public void testAlertFilterNoChildren() throws Exception {
    this.alertConfig.getProperties().put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(detectionId3));
    this.alertFilter = new DimensionsRecipientAlertFilter(provider, alertConfig,2500L);

    MergedAnomalyResultDTO child = makeAnomaly(detectionId3, 1234, 9999);
    child.setChild(true);

    this.detectedAnomalies.add(child);

    DetectionAlertFilterNotification recValue = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_FOR_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);
    Multimap<String, String> dimFilters = ArrayListMultimap.create();
    dimFilters.put("key", "value");
    recValue.setDimensionFilters(dimFilters);
    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("link1", "value1");
    recValue.getSubscriptionConfig().setReferenceLinks(refLinks);

    DetectionAlertFilterResult result = this.alertFilter.run();

    Assert.assertEquals(result.getResult().size(), 1);
    Assert.assertTrue(result.getResult().containsKey(recValue));
  }

  @Test
  public void testAlertFilterFeedback() throws Exception {
    this.alertConfig.getProperties().put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(detectionId3));
    this.alertFilter = new DimensionsRecipientAlertFilter(provider, alertConfig,2500L);

    AnomalyFeedbackDTO feedbackAnomaly = new AnomalyFeedbackDTO();
    feedbackAnomaly.setFeedbackType(AnomalyFeedbackType.ANOMALY);

    AnomalyFeedbackDTO feedbackNoFeedback = new AnomalyFeedbackDTO();
    feedbackNoFeedback.setFeedbackType(AnomalyFeedbackType.NO_FEEDBACK);

    MergedAnomalyResultDTO anomalyWithFeedback = makeAnomaly(detectionId3, 1234, 9999);
    anomalyWithFeedback.setFeedback(feedbackAnomaly);

    MergedAnomalyResultDTO anomalyWithoutFeedback = makeAnomaly(detectionId3, 1235, 9999);
    anomalyWithoutFeedback.setFeedback(feedbackNoFeedback);

    MergedAnomalyResultDTO anomalyWithNull = makeAnomaly(detectionId3, 1236, 9999);
    anomalyWithNull.setFeedback(null);

    this.detectedAnomalies.add(anomalyWithFeedback);
    this.detectedAnomalies.add(anomalyWithoutFeedback);
    this.detectedAnomalies.add(anomalyWithNull);

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 2);

    DetectionAlertFilterNotification recDefault = AlertFilterUtils.makeEmailNotifications(
        this.alertConfig, PROP_TO_VALUE, PROP_CC_VALUE, PROP_BCC_VALUE);
    Assert.assertTrue(result.getResult().containsKey(recDefault));
    Assert.assertEquals(result.getResult().get(recDefault).size(), 2);
    Assert.assertTrue(result.getResult().get(recDefault).contains(anomalyWithoutFeedback));
    Assert.assertTrue(result.getResult().get(recDefault).contains(anomalyWithNull));

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
  }

  private Set<MergedAnomalyResultDTO> makeSet(int... anomalyIndices) {
    Set<MergedAnomalyResultDTO> output = new HashSet<>();
    for (int anomalyIndex : anomalyIndices) {
      output.add(this.detectedAnomalies.get(anomalyIndex));
    }
    return output;
  }
}