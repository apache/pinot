package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.StaticDetectionPipeline;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class ThresholdAlgorithmTest {
  private DataProvider testDataProvider;
  private StaticDetectionPipeline thresholdAlgorithm;

  @BeforeMethod
  public void beforeMethod() {
    Map<MetricSlice, DataFrame> timeSeries = new HashMap<>();
    timeSeries.put(MetricSlice.from(123L, 0, 5),
        new DataFrame().addSeries(COL_VALUE, 0, 100, 200, 500, 1000).addSeries(COL_TIME, 0, 1, 2, 3, 4));

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(123L);
    metricConfigDTO.setName("thirdeye-test");
    metricConfigDTO.setDataset("thirdeye-test-dataset");

    DetectionConfigDTO detectionConfigDTO = new DetectionConfigDTO();
    detectionConfigDTO.setId(124L);
    Map<String, Object> properties = new HashMap<>();
    properties.put("min", 100);
    properties.put("max", 500);
    properties.put("metricUrn", "thirdeye:metric:123");
    detectionConfigDTO.setProperties(properties);

    this.testDataProvider = new MockDataProvider()
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setTimeseries(timeSeries);
    this.thresholdAlgorithm = new ThresholdAlgorithm(this.testDataProvider, detectionConfigDTO, 0, 5);
  }

  @Test
  public void testThresholdAlgorithmRun() throws Exception {
    DetectionPipelineResult result = thresholdAlgorithm.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 4);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 0);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 1);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 4);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 5);
  }
}
