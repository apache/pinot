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

package com.linkedin.thirdeye.detection.alert.filter;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilter;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.detection.DetectionTestUtils.*;


public class DimensionDetectionAlertFilterTest {

  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";
  private static final Set<String> PROP_TO_VALUE = new HashSet<>(Arrays.asList("test@example.com", "test@example.org"));
  private static final Set<String> PROP_CC_VALUE = new HashSet<>(Arrays.asList("cctest@example.com", "cctest@example.org"));
  private static final Set<String> PROP_BCC_VALUE = new HashSet<>(Arrays.asList("bcctest@example.com", "bcctest@example.org"));
  private static final Set<String> PROP_TO_FOR_VALUE = new HashSet<>(Arrays.asList("myTest@example.com", "myTest@example.org"));
  private static final Set<String> PROP_TO_FOR_ANOTHER_VALUE = Collections.singleton("myTest@example.net");
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final List<Long> PROP_ID_VALUE = Arrays.asList(1001L, 1002L);
  private static final String PROP_DIMENSION = "dimension";
  private static final String PROP_DIMENSION_VALUE = "key";
  private static final String PROP_DIMENSION_TO = "dimensionRecipients";
  private static final Map<String, Collection<String>> PROP_DIMENSION_TO_VALUE = new HashMap<>();
  static {
    PROP_DIMENSION_TO_VALUE.put("value", PROP_TO_FOR_VALUE);
    PROP_DIMENSION_TO_VALUE.put("anotherValue", PROP_TO_FOR_ANOTHER_VALUE);
  }

  private DetectionAlertFilter alertFilter;
  private List<MergedAnomalyResultDTO> detectedAnomalies;

  private Map<String, Object> properties;
  private MockDataProvider provider;
  private DetectionAlertConfigDTO alertConfig;

  @BeforeMethod
  public void beforeMethod() {
    this.detectedAnomalies = new ArrayList<>();
    this.detectedAnomalies.add(makeAnomaly(1001L, 1500, 2000, Collections.<String, String>emptyMap()));
    this.detectedAnomalies.add(makeAnomaly(1001L,0, 1000, Collections.singletonMap("key", "value")));
    this.detectedAnomalies.add(makeAnomaly(1001L,0, 1100, Collections.singletonMap("key", "anotherValue")));
    this.detectedAnomalies.add(makeAnomaly(1001L,0, 1200, Collections.singletonMap("key", "unknownValue")));
    this.detectedAnomalies.add(makeAnomaly(1002L,1100, 1500, Collections.singletonMap("unknownKey", "value")));
    this.detectedAnomalies.add(makeAnomaly(1002L,1200, 1600, Collections.singletonMap("key", "value")));
    this.detectedAnomalies.add(makeAnomaly(1002L,3333, 9999, Collections.singletonMap("key", "value")));
    this.detectedAnomalies.add(makeAnomaly(1003L,1111, 9999, Collections.singletonMap("key", "value")));

    this.provider = new MockDataProvider().setAnomalies(this.detectedAnomalies);

    this.alertConfig = new DetectionAlertConfigDTO();

    this.properties = new HashMap<>();
    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, PROP_TO_VALUE);
    recipients.put(PROP_CC, PROP_CC_VALUE);
    recipients.put(PROP_BCC, PROP_BCC_VALUE);
    this.properties.put(PROP_RECIPIENTS, recipients);
    this.properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);
    this.properties.put(PROP_DIMENSION, PROP_DIMENSION_VALUE);
    this.properties.put(PROP_DIMENSION_TO, PROP_DIMENSION_TO_VALUE);

    this.alertConfig.setProperties(this.properties);

    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(PROP_ID_VALUE.get(0), 0L);
    this.alertConfig.setVectorClocks(vectorClocks);
  }

  @Test
  public void testAlertFilterRecipients() throws Exception {
    this.alertFilter = new DimensionDetectionAlertFilter(provider, alertConfig,2500L);

    DetectionAlertFilterRecipients recDefault = makeRecipients();
    DetectionAlertFilterRecipients recValue = makeRecipients(PROP_TO_FOR_VALUE);
    DetectionAlertFilterRecipients recAnotherValue = makeRecipients(PROP_TO_FOR_ANOTHER_VALUE);

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().get(recDefault), makeSet(0, 3, 4));
    Assert.assertEquals(result.getResult().get(recValue), makeSet(1, 5));
    Assert.assertEquals(result.getResult().get(recAnotherValue), makeSet(2));
    // 6, 7 are out of search range
  }

  @Test
  public void testAlertFilterNoChildren() throws Exception {
    this.properties.put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(1003L));
    this.alertFilter = new DimensionDetectionAlertFilter(provider, alertConfig,2500L);

    MergedAnomalyResultDTO child = makeAnomaly(1003L, 1234, 9999);
    child.setChild(true);

    this.detectedAnomalies.add(child);

    DetectionAlertFilterRecipients recValue = makeRecipients(PROP_TO_FOR_VALUE);

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 1);
    Assert.assertTrue(result.getResult().containsKey(recValue));
  }

  @Test
  public void testAlertFilterFeedback() throws Exception {
    this.properties.put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(1003L));
    this.alertFilter = new DimensionDetectionAlertFilter(provider, alertConfig,2500L);

    AnomalyFeedbackDTO feedbackAnomaly = new AnomalyFeedbackDTO();
    feedbackAnomaly.setFeedbackType(AnomalyFeedbackType.ANOMALY);

    AnomalyFeedbackDTO feedbackNoFeedback = new AnomalyFeedbackDTO();
    feedbackNoFeedback.setFeedbackType(AnomalyFeedbackType.NO_FEEDBACK);

    MergedAnomalyResultDTO anomalyWithFeedback = makeAnomaly(1003L, 1234, 9999);
    anomalyWithFeedback.setFeedback(feedbackAnomaly);

    MergedAnomalyResultDTO anomalyWithoutFeedback = makeAnomaly(1003L, 1235, 9999);
    anomalyWithoutFeedback.setFeedback(feedbackNoFeedback);

    MergedAnomalyResultDTO anomalyWithNull = makeAnomaly(1003L, 1236, 9999);
    anomalyWithNull.setFeedback(null);

    this.detectedAnomalies.add(anomalyWithFeedback);
    this.detectedAnomalies.add(anomalyWithoutFeedback);
    this.detectedAnomalies.add(anomalyWithNull);

    DetectionAlertFilterRecipients recDefault = makeRecipients();
    DetectionAlertFilterRecipients recValue = makeRecipients(PROP_TO_FOR_VALUE);

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 2);
    Assert.assertTrue(result.getResult().containsKey(recDefault));
    Assert.assertEquals(result.getResult().get(recDefault).size(), 2);
    Assert.assertTrue(result.getResult().get(recDefault).contains(anomalyWithoutFeedback));
    Assert.assertTrue(result.getResult().get(recDefault).contains(anomalyWithNull));
    Assert.assertTrue(result.getResult().containsKey(recValue));
  }

  private Set<MergedAnomalyResultDTO> makeSet(int... anomalyIndices) {
    Set<MergedAnomalyResultDTO> output = new HashSet<>();
    for (int anomalyIndex : anomalyIndices) {
      output.add(this.detectedAnomalies.get(anomalyIndex));
    }
    return output;
  }

  private static DetectionAlertFilterRecipients makeRecipients() {
    return makeRecipients(new HashSet<String>());
  }

  private static DetectionAlertFilterRecipients makeRecipients(Set<String> to) {
    Set<String> newTo = new HashSet<>(PROP_TO_VALUE);
    newTo.addAll(to);
    return new DetectionAlertFilterRecipients(newTo, new HashSet<>(PROP_CC_VALUE), new HashSet<>(PROP_BCC_VALUE));
  }
}