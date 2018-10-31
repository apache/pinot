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


public class ToAllRecipientsDetectionAlertFilterTest {

  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";
  private static final Set<String> PROP_TO_VALUE = new HashSet<>(Arrays.asList("test@test.com", "test@test.org"));
  private static final Set<String> PROP_CC_VALUE = new HashSet<>(Arrays.asList("cctest@test.com", "cctest@test.org"));
  private static final Set<String> PROP_BCC_VALUE = new HashSet<>(Arrays.asList("bcctest@test.com", "bcctest@test.org"));
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final List<Long> PROP_ID_VALUE = Arrays.asList(1001L, 1002L);
  private static final String PROP_SEND_ONCE = "sendOnce";

  private static final DetectionAlertFilterRecipients RECIPIENTS = new DetectionAlertFilterRecipients(
      new HashSet<>(PROP_TO_VALUE), new HashSet<>(PROP_CC_VALUE), new HashSet<>(PROP_BCC_VALUE));

  private DetectionAlertFilter alertFilter;
  private List<MergedAnomalyResultDTO> detectedAnomalies;

  private Map<String, Object> properties;
  private MockDataProvider provider;
  private DetectionAlertConfigDTO alertConfig;

  @BeforeMethod
  public void beforeMethod() {
    this.detectedAnomalies = new ArrayList<>();
    this.detectedAnomalies.add(makeAnomaly(1001L, 1500, 2000));
    this.detectedAnomalies.add(makeAnomaly(1001L,0, 1000));
    this.detectedAnomalies.add(makeAnomaly(1002L,0, 1000));
    this.detectedAnomalies.add(makeAnomaly(1002L,1100, 1500));
    this.detectedAnomalies.add(makeAnomaly(1002L,3333, 9999));
    this.detectedAnomalies.add(makeAnomaly(1003L,1100, 1500));

    this.provider = new MockDataProvider().setAnomalies(this.detectedAnomalies);

    this.alertConfig = new DetectionAlertConfigDTO();

    this.properties = new HashMap<>();
    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, PROP_TO_VALUE);
    recipients.put(PROP_CC, PROP_CC_VALUE);
    recipients.put(PROP_BCC, PROP_BCC_VALUE);
    this.properties.put(PROP_RECIPIENTS, recipients);
    this.properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);

    this.alertConfig.setProperties(properties);
    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(PROP_ID_VALUE.get(0), 0L);
    this.alertConfig.setVectorClocks(vectorClocks);

  }

  @Test
  public void testGetAlertFilterResult() throws Exception {
    this.alertFilter = new ToAllRecipientsDetectionAlertFilter(this.provider, this.alertConfig,2500L);

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().get(RECIPIENTS), new HashSet<>(this.detectedAnomalies.subList(0, 4)));
  }

  @Test
  public void testAlertFilterFeedback() throws Exception {
    this.properties.put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(1003L));
    this.alertFilter = new ToAllRecipientsDetectionAlertFilter(this.provider, this.alertConfig,2500L);

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

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().size(), 1);
    Assert.assertTrue(result.getResult().containsKey(RECIPIENTS));
    Assert.assertEquals(result.getResult().get(RECIPIENTS).size(), 3);
    Assert.assertTrue(result.getResult().get(RECIPIENTS).contains(this.detectedAnomalies.get(5)));
    Assert.assertTrue(result.getResult().get(RECIPIENTS).contains(anomalyWithoutFeedback));
    Assert.assertTrue(result.getResult().get(RECIPIENTS).contains(anomalyWithNull));
  }

  @Test
  public void testAlertFilterNoResend() throws Exception {
    MergedAnomalyResultDTO existingOld = makeAnomaly(1001L, 1000, 1100);
    existingOld.setId(5L);

    MergedAnomalyResultDTO existingNew = makeAnomaly(1001L, 1100, 1200);
    existingNew.setId(6L);

    MergedAnomalyResultDTO existingFuture = makeAnomaly(1001L, 1200, 1300);
    existingFuture.setId(7L);

    this.detectedAnomalies.clear();
    this.detectedAnomalies.add(existingOld);
    this.detectedAnomalies.add(existingNew);
    this.detectedAnomalies.add(existingFuture);

    this.alertConfig.setHighWaterMark(6L);
    this.alertConfig.setVectorClocks(Collections.singletonMap(1001L, 1100L));

    this.alertFilter = new ToAllRecipientsDetectionAlertFilter(this.provider, this.alertConfig,2500L);

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().get(RECIPIENTS).size(), 1);
    Assert.assertTrue(result.getResult().get(RECIPIENTS).contains(existingFuture));
  }

  @Test
  public void testAlertFilterResend() throws Exception {
    MergedAnomalyResultDTO existingOld = makeAnomaly(1001L, 1000, 1100);
    existingOld.setId(5L);

    MergedAnomalyResultDTO existingNew = makeAnomaly(1001L, 1100, 1200);
    existingNew.setId(6L);

    MergedAnomalyResultDTO existingFuture = makeAnomaly(1001L, 1200, 1300);
    existingFuture.setId(7L);

    this.detectedAnomalies.clear();
    this.detectedAnomalies.add(existingOld);
    this.detectedAnomalies.add(existingNew);
    this.detectedAnomalies.add(existingFuture);

    this.alertConfig.setHighWaterMark(5L);
    this.alertConfig.setVectorClocks(Collections.singletonMap(1001L, 1100L));
    this.alertConfig.getProperties().put(PROP_SEND_ONCE, false);

    this.alertFilter = new ToAllRecipientsDetectionAlertFilter(this.provider, this.alertConfig,2500L);

    DetectionAlertFilterResult result = this.alertFilter.run();
    Assert.assertEquals(result.getResult().get(RECIPIENTS).size(), 2);
    Assert.assertTrue(result.getResult().get(RECIPIENTS).contains(existingNew));
    Assert.assertTrue(result.getResult().get(RECIPIENTS).contains(existingFuture));
  }
}