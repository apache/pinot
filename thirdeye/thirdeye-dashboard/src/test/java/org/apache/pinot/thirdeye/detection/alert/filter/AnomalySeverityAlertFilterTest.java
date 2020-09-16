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
 *
 */

package org.apache.pinot.thirdeye.detection.alert.filter;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.thirdeye.anomaly.AnomalySeverity;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalySubscriptionGroupNotificationManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalySubscriptionGroupNotificationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilter;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.alert.filter.AlertFilterUtils.*;


public class AnomalySeverityAlertFilterTest {
  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_EMAIL_SCHEME = "emailScheme";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";
  private static final Set<String> PROP_TO_FOR_VALUE =
      new HashSet<>(Arrays.asList("myTest@example.com", "myTest@example.org"));
  private static final Set<String> PROP_TO_FOR_ANOTHER_VALUE =
      new HashSet<>(Arrays.asList("myTest@example.net", "myTest@example.com"));
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String PROP_SEVERITY_TO = "severityRecipients";
  private static final List<Object> severityProperty = new ArrayList<>();

  private DetectionAlertFilter alertFilter;

  private DAOTestBase testDAOProvider;
  private DetectionAlertConfigDTO alertConfig;
  private List<MergedAnomalyResultDTO> detectionAnomalies;
  private long baseTime;
  private List<Long> detectionConfigIds;
  private MergedAnomalyResultDTO renotifyAnomaly;
  private final MockDataProvider provider = new MockDataProvider();
  private final Map<String, Object> notify1 = new HashMap<>();
  private final Map<String, Object> notify2 = new HashMap<>();
  private final Map<String, Object> defaultScheme = new HashMap<>();

  @BeforeMethod
  public void beforeMethod() throws InterruptedException {
    testDAOProvider = DAOTestBase.getInstance();

    DetectionConfigDTO detectionConfig1 = new DetectionConfigDTO();
    detectionConfig1.setName("test detection 1");
    detectionConfig1.setActive(true);
    long detectionConfigId1 = DAORegistry.getInstance().getDetectionConfigManager().save(detectionConfig1);

    DetectionConfigDTO detectionConfig2 = new DetectionConfigDTO();
    detectionConfig2.setName("test detection 2");
    detectionConfig2.setActive(true);
    long detectionConfigId2 = DAORegistry.getInstance().getDetectionConfigManager().save(detectionConfig2);

    detectionConfigIds = Arrays.asList(detectionConfigId1, detectionConfigId2);

    // Anomaly notification is tracked through create time. Start and end time doesn't matter here.
    this.detectionAnomalies = new ArrayList<>();
    renotifyAnomaly =
        makeAnomaly(detectionConfigId1, System.currentTimeMillis(), 0, 50, Collections.singletonMap("key", "value"),
            null, AnomalySeverity.LOW);
    Thread.sleep(100);
    this.baseTime = System.currentTimeMillis();
    Thread.sleep(100);
    this.detectionAnomalies.add(
        makeAnomaly(detectionConfigId1, this.baseTime, 0, 100, Collections.singletonMap("key", "value"), null,
            AnomalySeverity.LOW));
    Thread.sleep(10);
    this.detectionAnomalies.add(
        makeAnomaly(detectionConfigId1, this.baseTime, 0, 110, Collections.singletonMap("key", "anotherValue"), null,
            AnomalySeverity.MEDIUM));
    Thread.sleep(20);
    this.detectionAnomalies.add(
        makeAnomaly(detectionConfigId1, this.baseTime, 0, 120, Collections.singletonMap("key", "unknownValue"), null,
            AnomalySeverity.HIGH));
    Thread.sleep(30);
    this.detectionAnomalies.add(
        makeAnomaly(detectionConfigId2, this.baseTime, 110, 150, Collections.singletonMap("unknownKey", "value"),
            null));
    Thread.sleep(10);
    this.detectionAnomalies.add(
        makeAnomaly(detectionConfigId2, this.baseTime, 120, 160, Collections.singletonMap("key", "value"), null));
    Thread.sleep(40);
    this.detectionAnomalies.add(
        makeAnomaly(detectionConfigId1, this.baseTime, 150, 200, Collections.<String, String>emptyMap(), null));
    Thread.sleep(200);
    this.detectionAnomalies.add(
        makeAnomaly(detectionConfigId2, this.baseTime, 300, 400, Collections.singletonMap("key", "value"), null));
    Thread.sleep(100);

    this.alertConfig = createDetectionAlertConfig();
  }

  private DetectionAlertConfigDTO createDetectionAlertConfig() {
    DetectionAlertConfigDTO alertConfig = new DetectionAlertConfigDTO();

    notify1.put("severity", Arrays.asList("LOW", "MEDIUM"));
    notify1.put("notify", ImmutableMap.of("emailScheme", ImmutableMap.of("recipients", PROP_TO_FOR_VALUE)));
    notify2.put("severity", Collections.singleton("HIGH"));
    notify2.put("notify", ImmutableMap.of("emailScheme", ImmutableMap.of("recipients", PROP_TO_FOR_ANOTHER_VALUE)));
    severityProperty.add(notify1);
    severityProperty.add(notify2);

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_DETECTION_CONFIG_IDS, detectionConfigIds);
    properties.put(PROP_SEVERITY_TO, severityProperty);
    alertConfig.setProperties(properties);

    Map<String, Object> emailScheme = new HashMap<>();
    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, AlertFilterUtils.PROP_TO_VALUE);
    recipients.put(PROP_CC, AlertFilterUtils.PROP_CC_VALUE);
    recipients.put(PROP_BCC, AlertFilterUtils.PROP_BCC_VALUE);
    emailScheme.put(PROP_RECIPIENTS, recipients);
    defaultScheme.put(PROP_EMAIL_SCHEME, emailScheme);
    alertConfig.setAlertSchemes(defaultScheme);

    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(detectionConfigIds.get(0), this.baseTime);
    vectorClocks.put(detectionConfigIds.get(1), this.baseTime);
    alertConfig.setVectorClocks(vectorClocks);

    return alertConfig;
  }

  @Test
  public void testAlertFilterRecipients() throws Exception {
    this.alertFilter = new AnomalySeverityAlertFilter(provider, alertConfig, this.baseTime + 350L);

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 3);

    int verifiedResult = 0;
    for (Map.Entry<DetectionAlertFilterNotification, Set<MergedAnomalyResultDTO>> entry : result.getResult()
        .entrySet()) {
      if (entry.getValue().equals(makeSet(0, 1))) {
        Assert.assertEquals(entry.getKey().getSubscriptionConfig().getAlertSchemes(), notify1.get("notify"));
        verifiedResult++;
      } else if (entry.getValue().equals(makeSet(2))) {
        Assert.assertEquals(entry.getKey().getSubscriptionConfig().getAlertSchemes(), notify2.get("notify"));
        verifiedResult++;
      } else if (entry.getValue().equals(makeSet(3, 4, 5))) {
        Assert.assertEquals(entry.getKey().getSubscriptionConfig().getAlertSchemes(), defaultScheme);
        verifiedResult++;
      }
    }
    Assert.assertEquals(verifiedResult, 3);
  }

  @Test
  public void testRenotifyAnomaly() throws Exception {
    AnomalySubscriptionGroupNotificationManager renotificationManager =
        DAORegistry.getInstance().getAnomalySubscriptionGroupNotificationManager();
    AnomalySubscriptionGroupNotificationDTO anomalySubscriptionGroupNotification =
        new AnomalySubscriptionGroupNotificationDTO();
    anomalySubscriptionGroupNotification.setAnomalyId(renotifyAnomaly.getId());
    anomalySubscriptionGroupNotification.setDetectionConfigId(renotifyAnomaly.getDetectionConfigId());
    renotificationManager.save(anomalySubscriptionGroupNotification);

    this.alertFilter = new AnomalySeverityAlertFilter(provider, alertConfig, this.baseTime + 350L);

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 3);

    int verifiedResult = 0;
    for (Map.Entry<DetectionAlertFilterNotification, Set<MergedAnomalyResultDTO>> entry : result.getResult()
        .entrySet()) {
      if (entry.getValue().equals(makeSet(renotifyAnomaly, 0, 1))) {
        Assert.assertEquals(entry.getKey().getSubscriptionConfig().getAlertSchemes(), notify1.get("notify"));
        verifiedResult++;
      } else if (entry.getValue().equals(makeSet(2))) {
        Assert.assertEquals(entry.getKey().getSubscriptionConfig().getAlertSchemes(), notify2.get("notify"));
        verifiedResult++;
      } else if (entry.getValue().equals(makeSet(3, 4, 5))) {
        Assert.assertEquals(entry.getKey().getSubscriptionConfig().getAlertSchemes(), defaultScheme);
        verifiedResult++;
      }
    }
    Assert.assertEquals(verifiedResult, 3);
  }

  private Set<MergedAnomalyResultDTO> makeSet(MergedAnomalyResultDTO anomaly, int... anomalyIndices) {
    Set<MergedAnomalyResultDTO> set = makeSet(anomalyIndices);
    set.add(anomaly);
    return set;
  }

  private Set<MergedAnomalyResultDTO> makeSet(int... anomalyIndices) {
    Set<MergedAnomalyResultDTO> output = new HashSet<>();
    for (int anomalyIndex : anomalyIndices) {
      output.add(this.detectionAnomalies.get(anomalyIndex));
    }
    return output;
  }
}
