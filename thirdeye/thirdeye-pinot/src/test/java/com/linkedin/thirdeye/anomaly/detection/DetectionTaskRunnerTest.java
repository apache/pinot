package com.linkedin.thirdeye.anomaly.detection;

import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DetectionTaskRunnerTest {

  @Test
  public void testSimpleDuplicateRawAnomalies() {
    List<RawAnomalyResultDTO> anomalyList = new ArrayList<>();

    RawAnomalyResultDTO anomaly = new RawAnomalyResultDTO();
    anomaly.setStartTime(1L);
    anomaly.setEndTime(2L);
    anomalyList.add(anomaly);

    RawAnomalyResultDTO duplicate = new RawAnomalyResultDTO();
    duplicate.setStartTime(1L);
    duplicate.setEndTime(2L);
    anomalyList.add(duplicate);

    List<RawAnomalyResultDTO> expectedAnomalyList = new ArrayList<>();
    expectedAnomalyList.add(anomaly);

    anomalyList = DetectionTaskRunner.cleanUpDuplicateRawAnomalies(anomalyList);
    Assert.assertEquals(anomalyList, expectedAnomalyList);
  }

  @Test
  public void testOverlappedRawAnomalies() {
    List<RawAnomalyResultDTO> anomalyList = new ArrayList<>();

    RawAnomalyResultDTO anomaly1 = new RawAnomalyResultDTO();
    anomaly1.setStartTime(1L);
    anomaly1.setEndTime(2L);
    anomalyList.add(anomaly1);

    RawAnomalyResultDTO anomaly2 = new RawAnomalyResultDTO();
    anomaly2.setStartTime(1L);
    anomaly2.setEndTime(3L);
    anomalyList.add(anomaly2);

    List<RawAnomalyResultDTO> expectedAnomalyList = new ArrayList<>();
    expectedAnomalyList.add(anomaly2);

    anomalyList = DetectionTaskRunner.cleanUpDuplicateRawAnomalies(anomalyList);
    Assert.assertEquals(anomalyList, expectedAnomalyList);
  }

  @Test
  public void testSingleRawAnomalyList() {
    List<RawAnomalyResultDTO> anomalyList = new ArrayList<>();

    RawAnomalyResultDTO anomaly = new RawAnomalyResultDTO();
    anomaly.setStartTime(1L);
    anomaly.setEndTime(2L);
    anomalyList.add(anomaly);

    List<RawAnomalyResultDTO> expectedAnomalyList = new ArrayList<>();
    expectedAnomalyList.add(anomaly);

    anomalyList = DetectionTaskRunner.cleanUpDuplicateRawAnomalies(anomalyList);
    Assert.assertEquals(anomalyList, expectedAnomalyList);
  }

  @Test
  public void testNoDuplicationRawAnomalies() {
    List<RawAnomalyResultDTO> anomalyList = new ArrayList<>();

    RawAnomalyResultDTO anomaly1 = new RawAnomalyResultDTO();
    anomaly1.setStartTime(1L);
    anomaly1.setEndTime(2L);
    anomalyList.add(anomaly1);

    RawAnomalyResultDTO anomaly2 = new RawAnomalyResultDTO();
    anomaly2.setStartTime(2L);
    anomaly2.setEndTime(4L);
    anomalyList.add(anomaly2);

    RawAnomalyResultDTO anomaly3 = new RawAnomalyResultDTO();
    anomaly3.setStartTime(3L);
    anomaly3.setEndTime(5L);
    anomalyList.add(anomaly3);

    List<RawAnomalyResultDTO> expectedAnomalyList = new ArrayList<>();
    expectedAnomalyList.add(anomaly1);
    expectedAnomalyList.add(anomaly2);
    expectedAnomalyList.add(anomaly3);

    anomalyList = DetectionTaskRunner.cleanUpDuplicateRawAnomalies(anomalyList);
    Assert.assertEquals(anomalyList, expectedAnomalyList);
  }
}