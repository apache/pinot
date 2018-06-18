package com.linkedin.thirdeye.detection.alert.filter;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilter;
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
  private static final Set<String> PROP_RECIPIENTS_VALUE = new HashSet<>(Arrays.asList("test@example.com", "test@example.org"));
  private static final Set<String> PROP_RECIPIENTS_FOR_VALUE = new HashSet<>(Arrays.asList("myTest@example.com", "myTest@example.org"));
  private static final Set<String> PROP_RECIPIENTS_FOR_ANOTHER_VALUE = Collections.singleton("myTest@example.net");
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final List<Long> PROP_ID_VALUE = Arrays.asList(1001L, 1002L);
  private static final String PROP_DIMENSION = "dimension";
  private static final String PROP_DIMENSION_VALUE = "key";
  private static final String PROP_DIMENSION_RECIPIENTS = "dimensionRecipients";
  private static final Map<String, Collection<String>> PROP_DIMENSION_RECIPIENTS_VALUE = new HashMap<>();
  static {
    PROP_DIMENSION_RECIPIENTS_VALUE.put("value", PROP_RECIPIENTS_FOR_VALUE);
    PROP_DIMENSION_RECIPIENTS_VALUE.put("anotherValue", PROP_RECIPIENTS_FOR_ANOTHER_VALUE);
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
    this.properties.put(PROP_RECIPIENTS, PROP_RECIPIENTS_VALUE);
    this.properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);
    this.properties.put(PROP_DIMENSION, PROP_DIMENSION_VALUE);
    this.properties.put(PROP_DIMENSION_RECIPIENTS, PROP_DIMENSION_RECIPIENTS_VALUE);

    this.alertConfig.setProperties(this.properties);

    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(PROP_ID_VALUE.get(0), 0L);
    this.alertConfig.setVectorClocks(vectorClocks);
  }

  @Test
  public void testAlertFilterRecipients() throws Exception {
    this.alertFilter = new DimensionDetectionAlertFilter(provider, alertConfig,2500L);

    Set<String> recDefault = PROP_RECIPIENTS_VALUE;

    Set<String> recValue = new HashSet<>(PROP_RECIPIENTS_VALUE);
    recValue.addAll(PROP_RECIPIENTS_FOR_VALUE);

    Set<String> recAnotherValue = new HashSet<>(PROP_RECIPIENTS_VALUE);
    recAnotherValue.addAll(PROP_RECIPIENTS_FOR_ANOTHER_VALUE);

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

    Set<String> recValue = new HashSet<>(PROP_RECIPIENTS_VALUE);
    recValue.addAll(PROP_RECIPIENTS_FOR_VALUE);

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

    Set<String> recDefault = PROP_RECIPIENTS_VALUE;

    Set<String> recValue = new HashSet<>(PROP_RECIPIENTS_VALUE);
    recValue.addAll(PROP_RECIPIENTS_FOR_VALUE);

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
}