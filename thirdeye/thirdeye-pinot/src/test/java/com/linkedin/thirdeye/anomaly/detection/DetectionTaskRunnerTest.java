package com.linkedin.thirdeye.anomaly.detection;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.anomalydetection.context.RawAnomalyResult;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DetectionTaskRunnerTest {

  @Test
  public void testSimpleDuplicateRawAnomalies() {
    List<AnomalyResult> anomalyList = new ArrayList<>();

    AnomalyResult anomaly = new RawAnomalyResult();
    anomaly.setStartTime(1L);
    anomaly.setEndTime(2L);
    anomalyList.add(anomaly);

    AnomalyResult duplicate = new RawAnomalyResult();
    duplicate.setStartTime(1L);
    duplicate.setEndTime(2L);
    anomalyList.add(duplicate);

    List<AnomalyResult> expectedAnomalyList = new ArrayList<>();
    expectedAnomalyList.add(anomaly);

    anomalyList = DetectionTaskRunner.cleanUpDuplicateRawAnomalies(anomalyList);
    Assert.assertEquals(anomalyList, expectedAnomalyList);
  }

  @Test
  public void testOverlappedRawAnomalies() {
    List<AnomalyResult> anomalyList = new ArrayList<>();

    AnomalyResult anomaly1 = new RawAnomalyResult();
    anomaly1.setStartTime(1L);
    anomaly1.setEndTime(2L);
    anomalyList.add(anomaly1);

    AnomalyResult anomaly2 = new RawAnomalyResult();
    anomaly2.setStartTime(1L);
    anomaly2.setEndTime(3L);
    anomalyList.add(anomaly2);

    List<AnomalyResult> expectedAnomalyList = new ArrayList<>();
    expectedAnomalyList.add(anomaly2);

    anomalyList = DetectionTaskRunner.cleanUpDuplicateRawAnomalies(anomalyList);
    Assert.assertEquals(anomalyList, expectedAnomalyList);
  }

  @Test
  public void testSingleRawAnomalyList() {
    List<AnomalyResult> anomalyList = new ArrayList<>();

    AnomalyResult anomaly = new RawAnomalyResult();
    anomaly.setStartTime(1L);
    anomaly.setEndTime(2L);
    anomalyList.add(anomaly);

    List<AnomalyResult> expectedAnomalyList = new ArrayList<>();
    expectedAnomalyList.add(anomaly);

    anomalyList = DetectionTaskRunner.cleanUpDuplicateRawAnomalies(anomalyList);
    Assert.assertEquals(anomalyList, expectedAnomalyList);
  }

  @Test
  public void testNoDuplicationRawAnomalies() {
    List<AnomalyResult> anomalyList = new ArrayList<>();

    AnomalyResult anomaly1 = new RawAnomalyResult();
    anomaly1.setStartTime(1L);
    anomaly1.setEndTime(2L);
    anomalyList.add(anomaly1);

    AnomalyResult anomaly2 = new RawAnomalyResult();
    anomaly2.setStartTime(2L);
    anomaly2.setEndTime(4L);
    anomalyList.add(anomaly2);

    AnomalyResult anomaly3 = new RawAnomalyResult();
    anomaly3.setStartTime(3L);
    anomaly3.setEndTime(5L);
    anomalyList.add(anomaly3);

    List<AnomalyResult> expectedAnomalyList = new ArrayList<>();
    expectedAnomalyList.add(anomaly1);
    expectedAnomalyList.add(anomaly2);
    expectedAnomalyList.add(anomaly3);

    anomalyList = DetectionTaskRunner.cleanUpDuplicateRawAnomalies(anomalyList);
    Assert.assertEquals(anomalyList, expectedAnomalyList);
  }
}