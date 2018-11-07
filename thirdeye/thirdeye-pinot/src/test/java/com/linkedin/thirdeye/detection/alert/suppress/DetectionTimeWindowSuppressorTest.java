package com.linkedin.thirdeye.detection.alert.suppress;

import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.detection.alert.suppress.DetectionAlertTimeWindowSuppressor.*;


public class DetectionTimeWindowSuppressorTest {

  private DAOTestBase testDAOProvider;
  private Set<MergedAnomalyResultDTO> anomalies;
  private DetectionAlertConfigDTO config;

  private Map<String, Object> createSuppressWindow(long startTime, long endTime, boolean isThreshold, double expectedChange,
      double acceptableDeviation) {
    Map<String, Object> suppressWindowProps = new HashMap<>();
    suppressWindowProps.put(WINDOW_START_TIME_KEY, startTime);
    suppressWindowProps.put(WINDOW_END_TIME_KEY, endTime);
    suppressWindowProps.put(IS_THRESHOLD_KEY, isThreshold);
    suppressWindowProps.put(EXPECTED_CHANGE_KEY, expectedChange);
    suppressWindowProps.put(ACCEPTABLE_DEVIATION_KEY, acceptableDeviation);
    return suppressWindowProps;
  }

  private MergedAnomalyResultDTO createAnomaly(long id, long startTime, long endTime, double weight) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setId(id);
    anomaly.setStartTime(startTime);
    anomaly.setEndTime(endTime);
    anomaly.setWeight(weight);
    return anomaly;
  }

  private void initDetectionAlertConfig() {
    config = new DetectionAlertConfigDTO();

    List<Map<String, Object>> suppressWindowList = new ArrayList<>();
    suppressWindowList.add(createSuppressWindow(1000, 3000, true, 0.5, 0.1));
    suppressWindowList.add(createSuppressWindow(4500, 6000, true, 0.6, 0.2));

    Map<String, Object> params = new HashMap<>();
    params.put(TIME_WINDOWS_KEY, suppressWindowList);

    Map<String, Map<String, Object>> alertSuppressors = new HashMap<>();
    alertSuppressors.put(TIME_WINDOW_SUPPRESSOR_KEY, params);
    config.setAlertSuppressors(alertSuppressors);
  }

  private void initAnomalies() {
    anomalies = new HashSet<>();

    anomalies.add(createAnomaly(1l, 500, 900, 0.5));
    anomalies.add(createAnomaly(2l, 700, 1000, 0.8));
    anomalies.add(createAnomaly(3l, 500, 1500, 0.2));
    anomalies.add(createAnomaly(4l, 1000, 1500, 0.4));
    anomalies.add(createAnomaly(5l, 1500, 2500, 0.6));
    anomalies.add(createAnomaly(6l, 2500, 3000, 0.7));
    anomalies.add(createAnomaly(7l, 2000, 3500, 0.5));
    anomalies.add(createAnomaly(8l, 3000, 3500, 0.6));
    anomalies.add(createAnomaly(9l, 3500, 4000, 0.1));
    anomalies.add(createAnomaly(10l, 5000, 5500, 0.5));
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    this.testDAOProvider = DAOTestBase.getInstance();
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    initAnomalies();
    initDetectionAlertConfig();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  /**
   * Anomaly distribution along with suppression windows.
   *
   * Anomalies 4, 5, 7, and 10 should be suppressed (not notified).
   * Anomaly 6 is not suppressed because it falls outside the suppression region.
   *
   *      *-----3----*    *------7-------*
   *      |
   *      | *-2-*    *----5----*    *--8-*
   *      |     |                   |
   *      *-1-* *--4-*         *--6-*    *--9-*        *---10---*
   *      |     |                   |
   * _____|_____|___________________|______________|________________|
   *      |     |                   |              |                |
   *    500     |                   |              |                |
   *          1000----<window1>----3000          4500--<window2>--6000
   */
  @Test
  public void testTimeWindowSuppressorWithThreshold() throws Exception {

    DetectionAlertFilterResult result = new DetectionAlertFilterResult();
    result.addMapping(new DetectionAlertFilterRecipients(Collections.singleton("test@test")), anomalies);

    DetectionAlertTimeWindowSuppressor suppressor = new DetectionAlertTimeWindowSuppressor(config);
    DetectionAlertFilterResult resultsAfterSuppress = suppressor.run(result);

    Set<Long> filteredAnomalyIds = new HashSet<>(Arrays.asList(1l, 2l, 3l, 6l, 8l, 9l));

    Assert.assertEquals(resultsAfterSuppress.getAllAnomalies().size(), 6);
    for (MergedAnomalyResultDTO anomaly : resultsAfterSuppress.getAllAnomalies()) {
      Assert.assertTrue(filteredAnomalyIds.contains(anomaly.getId()));
    }
  }

  /**
   * Overlapping time window suppressor without thresholds
   */
  @Test
  public void testTimeWindowSuppressor() throws Exception {
    List<Map<String, Object>> suppressWindowList = new ArrayList<>();
    suppressWindowList.add(createSuppressWindow(500, 6000, false, 0, 0));

    Map<String, Object> params = new HashMap<>();
    params.put(TIME_WINDOWS_KEY, suppressWindowList);

    Map<String, Map<String, Object>> alertSuppressors = new HashMap<>();
    alertSuppressors.put(TIME_WINDOW_SUPPRESSOR_KEY, params);
    config.setAlertSuppressors(alertSuppressors);

    DetectionAlertFilterResult result = new DetectionAlertFilterResult();
    result.addMapping(new DetectionAlertFilterRecipients(Collections.singleton("test@test")), anomalies);

    DetectionAlertTimeWindowSuppressor suppressor = new DetectionAlertTimeWindowSuppressor(config);
    DetectionAlertFilterResult resultsAfterSuppress = suppressor.run(result);

    Assert.assertEquals(resultsAfterSuppress.getAllAnomalies().size(), 0);
  }

}
