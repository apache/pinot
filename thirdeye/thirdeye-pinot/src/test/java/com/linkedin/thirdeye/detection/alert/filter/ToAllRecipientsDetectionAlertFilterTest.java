package com.linkedin.thirdeye.detection.alert.filter;

import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilter;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import java.util.ArrayList;
import java.util.Arrays;
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

  private DetectionAlertFilter toAllAlertFilter;
  private List<MergedAnomalyResultDTO> detectedAnomalies;

  @BeforeMethod
  public void beforeMethod() {
    this.detectedAnomalies = new ArrayList<>();
    this.detectedAnomalies.add(makeAnomaly(1001L, 1500, 2000));
    this.detectedAnomalies.add(makeAnomaly(1001L,0, 1000));
    this.detectedAnomalies.add(makeAnomaly(1002L,0, 1000));
    this.detectedAnomalies.add(makeAnomaly(1002L,1100, 1500));
    this.detectedAnomalies.add(makeAnomaly(1002L,3333, 9999));
    this.detectedAnomalies.add(makeAnomaly(1003L,1100, 1500));

    DataProvider mockDataProvider = new MockDataProvider().setAnomalies(this.detectedAnomalies);

    DetectionAlertConfigDTO alertConfig = new DetectionAlertConfigDTO();
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_RECIPIENTS, PROP_RECIPIENTS_VALUE);
    properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);
    alertConfig.setProperties(properties);
    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(PROP_ID_VALUE.get(0), 0L);
    alertConfig.setVectorClocks(vectorClocks);

    this.toAllAlertFilter = new ToAllRecipientsDetectionAlertFilter(mockDataProvider, alertConfig,2500L);
  }

  @Test
  public void testGetAlertFilterResult() throws Exception {
    DetectionAlertFilterResult result = this.toAllAlertFilter.run();
    Assert.assertEquals(result.getResult().get(PROP_RECIPIENTS_VALUE), new HashSet<>(this.detectedAnomalies.subList(0, 4)));
    Assert.assertTrue(result.getVectorClocks().get(PROP_ID_VALUE.get(0)) == 2000L);
    Assert.assertTrue(result.getVectorClocks().get(PROP_ID_VALUE.get(1)) == 1500L);
  }
}