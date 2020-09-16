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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
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


public class PerUserDimensionAlertFilterTest {

  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_EMAIL_SCHEME = "emailScheme";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";
  private static final Set<String> PROP_TO_FOR_VALUE = new HashSet<>(Arrays.asList("myTest@example.com", "myTest@example.org"));
  private static final Set<String> PROP_TO_FOR_ANOTHER_VALUE = new HashSet<>(Arrays.asList("myTest@example.net", "myTest@example.com"));
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String PROP_DIMENSION = "dimension";
  private static final String PROP_DIMENSION_VALUE = "key";
  private static final String PROP_DIMENSION_TO = "dimensionRecipients";
  private static final Map<String, Collection<String>> PROP_DIMENSION_TO_VALUE = new HashMap<>();
  static {
    PROP_DIMENSION_TO_VALUE.put("value", PROP_TO_FOR_VALUE);
    PROP_DIMENSION_TO_VALUE.put("anotherValue", PROP_TO_FOR_ANOTHER_VALUE);
  }

  private DetectionAlertFilter alertFilter;

  private DAOTestBase testDAOProvider = null;
  private MockDataProvider provider;
  private DetectionAlertConfigDTO alertConfig;
  private List<MergedAnomalyResultDTO> detectionAnomalies;
  private long detectionConfigId1;
  private long detectionConfigId2;
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

    PROP_ID_VALUE = Arrays.asList(this.detectionConfigId1, this.detectionConfigId2);

    // Anomaly notification is tracked through create time. Start and end time doesn't matter here.
    this.detectionAnomalies = new ArrayList<>();
    this.baseTime = System.currentTimeMillis();
    Thread.sleep(100);
    this.detectionAnomalies.add(makeAnomaly(detectionConfigId1, this.baseTime, 0, 100, Collections.singletonMap("key", "value"), null));
    Thread.sleep(10);
    this.detectionAnomalies.add(makeAnomaly(detectionConfigId1, this.baseTime,0, 110, Collections.singletonMap("key", "anotherValue"), null));
    Thread.sleep(20);
    this.detectionAnomalies.add(makeAnomaly(detectionConfigId1, this.baseTime,0, 120, Collections.singletonMap("key", "unknownValue"), null));
    Thread.sleep(30);
    this.detectionAnomalies.add(makeAnomaly(detectionConfigId2, this.baseTime, 110, 150, Collections.singletonMap("unknownKey", "value"), null));
    Thread.sleep(10);
    this.detectionAnomalies.add(makeAnomaly(detectionConfigId2, this.baseTime,120, 160, Collections.singletonMap("key", "value"), null));
    Thread.sleep(40);
    this.detectionAnomalies.add(makeAnomaly(detectionConfigId1, this.baseTime,150, 200, Collections.<String, String>emptyMap(), null));
    Thread.sleep(200);
    this.detectionAnomalies.add(makeAnomaly(detectionConfigId2, this.baseTime,300, 400, Collections.singletonMap("key", "value"), null));

    Thread.sleep(100);
    this.alertConfig = createDetectionAlertConfig();
  }

  private DetectionAlertConfigDTO createDetectionAlertConfig() {
    DetectionAlertConfigDTO alertConfig = new DetectionAlertConfigDTO();

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);
    properties.put(PROP_DIMENSION, PROP_DIMENSION_VALUE);
    properties.put(PROP_DIMENSION_TO, PROP_DIMENSION_TO_VALUE);
    alertConfig.setProperties(properties);

    Map<String, Object> alertSchemes = new HashMap<>();
    Map<String, Object> emailScheme = new HashMap<>();
    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, AlertFilterUtils.PROP_TO_VALUE);
    recipients.put(PROP_CC, AlertFilterUtils.PROP_CC_VALUE);
    recipients.put(PROP_BCC, AlertFilterUtils.PROP_BCC_VALUE);
    emailScheme.put(PROP_RECIPIENTS, recipients);
    alertSchemes.put(PROP_EMAIL_SCHEME, emailScheme);
    alertConfig.setAlertSchemes(alertSchemes);

    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(PROP_ID_VALUE.get(0), this.baseTime);
    vectorClocks.put(PROP_ID_VALUE.get(1), this.baseTime);
    alertConfig.setVectorClocks(vectorClocks);

    return alertConfig;
  }

  @Test
  public void testAlertFilterRecipients() throws Exception {
    this.alertFilter = new PerUserDimensionAlertFilter(provider, alertConfig,this.baseTime + 350L);

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 3);

    DetectionAlertFilterNotification notification1 = AlertFilterUtils.makeEmailNotifications(this.alertConfig, Collections.singleton("myTest@example.com"));
    Assert.assertEquals(result.getResult().get(notification1), makeSet(0, 1, 4));

    DetectionAlertFilterNotification notification2 = AlertFilterUtils.makeEmailNotifications(this.alertConfig, Collections.singleton("myTest@example.org"));
    Assert.assertEquals(result.getResult().get(notification2), makeSet(0, 4));

    DetectionAlertFilterNotification notification3 = AlertFilterUtils.makeEmailNotifications(this.alertConfig, Collections.singleton("myTest@example.net"));
    Assert.assertEquals(result.getResult().get(notification3), makeSet(1));
  }

  private Set<MergedAnomalyResultDTO> makeSet(int... anomalyIndices) {
    Set<MergedAnomalyResultDTO> output = new HashSet<>();
    for (int anomalyIndex : anomalyIndices) {
      output.add(this.detectionAnomalies.get(anomalyIndex));
    }
    return output;
  }
}