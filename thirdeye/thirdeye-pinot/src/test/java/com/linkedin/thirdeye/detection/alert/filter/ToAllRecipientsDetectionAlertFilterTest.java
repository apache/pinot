package com.linkedin.thirdeye.detection.alert.filter;

import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilter;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.detection.DetectionTestUtils.*;


public class ToAllRecipientsDetectionAlertFilterTest {

  private static final String PROP_RECIPIENTS = "recipients";
  private static final List<String> PROP_RECIPIENTS_VALUE = Arrays.asList("test@test.com", "test@test.org");
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final List<Long> PROP_ID_VALUE = Collections.singletonList(1000L);

  private DetectionAlertFilter toAllAlertFilter;
  private List<MergedAnomalyResultDTO> detectedAnomalies;

  @BeforeMethod
  public void beforeMethod() {
    this.detectedAnomalies = new ArrayList<>();
    this.detectedAnomalies.add(makeAnomaly(0, 1000));
    this.detectedAnomalies.add(makeAnomaly(1500, 2000));

    DataProvider mockDataProvider = new MockDataProvider().setAnomalies(this.detectedAnomalies);

    AlertConfigDTO alertConfig = new AlertConfigDTO();
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_RECIPIENTS, PROP_RECIPIENTS_VALUE);
    properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);
    alertConfig.setProperties(properties);

    this.toAllAlertFilter = new ToAllRecipientsDetectionAlertFilter(mockDataProvider, alertConfig, 0L, 2500L);
  }

  @Test
  public void testGetAlertFilterResult() throws Exception {
    DetectionAlertFilterResult result = this.toAllAlertFilter.run();
    Assert.assertEquals(result.getResult().get(this.detectedAnomalies), PROP_RECIPIENTS_VALUE);
  }
}