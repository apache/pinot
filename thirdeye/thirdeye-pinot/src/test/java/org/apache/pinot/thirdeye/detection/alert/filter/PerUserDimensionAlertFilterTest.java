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
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilter;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.DetectionTestUtils.*;


public class PerUserDimensionAlertFilterTest {

  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";
  private static final Set<String> PROP_TO_VALUE = new HashSet<>(Arrays.asList("test@example.com", "test@example.org"));
  private static final Set<String> PROP_CC_VALUE = new HashSet<>(Arrays.asList("cctest@example.com", "cctest@example.org"));
  private static final Set<String> PROP_BCC_VALUE = new HashSet<>(Arrays.asList("bcctest@example.com", "bcctest@example.org"));
  private static final Set<String> PROP_TO_FOR_VALUE = new HashSet<>(Arrays.asList("myTest@example.com", "myTest@example.org"));
  private static final Set<String> PROP_TO_FOR_ANOTHER_VALUE = new HashSet<>(Arrays.asList("myTest@example.net", "myTest@example.com"));
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

    this.provider = new MockDataProvider()
        .setAnomalies(this.detectedAnomalies);

    this.alertConfig = createDetectionAlertConfig();
  }

  private DetectionAlertConfigDTO createDetectionAlertConfig() {
    DetectionAlertConfigDTO alertConfig = new DetectionAlertConfigDTO();

    Map<String, Object> properties = new HashMap<>();
    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, PROP_TO_VALUE);
    recipients.put(PROP_CC, PROP_CC_VALUE);
    recipients.put(PROP_BCC, PROP_BCC_VALUE);
    properties.put(PROP_RECIPIENTS, recipients);
    properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);
    properties.put(PROP_DIMENSION, PROP_DIMENSION_VALUE);
    properties.put(PROP_DIMENSION_TO, PROP_DIMENSION_TO_VALUE);

    alertConfig.setProperties(properties);

    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(PROP_ID_VALUE.get(0), 0L);
    alertConfig.setVectorClocks(vectorClocks);

    return alertConfig;
  }

  @Test
  public void testAlertFilterRecipients() throws Exception {
    this.alertFilter = new PerUserDimensionAlertFilter(provider, alertConfig,2500L);

    DetectionAlertFilterRecipients recipient1 = makeRecipients(Collections.singleton("myTest@example.com"));
    DetectionAlertFilterRecipients recipient2 = makeRecipients(Collections.singleton("myTest@example.org"));
    DetectionAlertFilterRecipients recipient3 = makeRecipients(Collections.singleton("myTest@example.net"));

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 3);
    Assert.assertEquals(result.getResult().get(recipient1), makeSet(1, 2, 5));
    Assert.assertEquals(result.getResult().get(recipient2), makeSet(1, 5));
    Assert.assertEquals(result.getResult().get(recipient3), makeSet(2));
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