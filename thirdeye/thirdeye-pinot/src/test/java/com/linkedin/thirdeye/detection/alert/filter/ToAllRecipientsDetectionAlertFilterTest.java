package com.linkedin.thirdeye.detection.alert.filter;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilter;
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
  private static final Set<String> PROP_RECIPIENTS_VALUE = new HashSet<>(Arrays.asList("test@test.com", "test@test.org"));
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final List<Long> PROP_ID_VALUE = Arrays.asList(1001L, 1002L);

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
    this.properties.put(PROP_RECIPIENTS, PROP_RECIPIENTS_VALUE);
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
    Assert.assertEquals(result.getResult().get(PROP_RECIPIENTS_VALUE), new HashSet<>(this.detectedAnomalies.subList(0, 4)));
    Assert.assertTrue(result.getVectorClocks().get(PROP_ID_VALUE.get(0)) == 2000L);
    Assert.assertTrue(result.getVectorClocks().get(PROP_ID_VALUE.get(1)) == 1500L);
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
    Assert.assertTrue(result.getResult().containsKey(PROP_RECIPIENTS_VALUE));
    Assert.assertEquals(result.getResult().get(PROP_RECIPIENTS_VALUE).size(), 3);
    Assert.assertTrue(result.getResult().get(PROP_RECIPIENTS_VALUE).contains(this.detectedAnomalies.get(5)));
    Assert.assertTrue(result.getResult().get(PROP_RECIPIENTS_VALUE).contains(anomalyWithoutFeedback));
    Assert.assertTrue(result.getResult().get(PROP_RECIPIENTS_VALUE).contains(anomalyWithNull));
  }

}