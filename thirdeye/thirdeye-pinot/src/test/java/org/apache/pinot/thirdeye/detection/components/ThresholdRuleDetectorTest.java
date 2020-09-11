/*
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

package org.apache.pinot.thirdeye.detection.components;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.spec.ThresholdRuleDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.exception.DetectorException;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ThresholdRuleDetectorTest {
  private DataProvider testDataProvider;

  @BeforeMethod
  public void beforeMethod() {
    Map<MetricSlice, DataFrame> timeSeries = new HashMap<>();
    timeSeries.put(MetricSlice.from(123L, 0, 10),
        new DataFrame().addSeries(DataFrame.COL_VALUE, 0, 100, 200, 500, 1000).addSeries(
            DataFrame.COL_TIME, 0, 2, 4, 6, 8));
    timeSeries.put(MetricSlice.from(123L, 1546214400000L, 1551398400000L),
        new DataFrame().addSeries(DataFrame.COL_TIME, 1546214400000L, 1548892800000L, 1551312000000L).addSeries(
            DataFrame.COL_VALUE, 100, 200, 300));

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(123L);
    metricConfigDTO.setName("thirdeye-test");
    metricConfigDTO.setDataset("thirdeye-test-dataset");

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setId(124L);
    datasetConfigDTO.setDataset("thirdeye-test-dataset");
    datasetConfigDTO.setTimeDuration(2);
    datasetConfigDTO.setTimeUnit(TimeUnit.MILLISECONDS);
    datasetConfigDTO.setTimezone("UTC");

    DetectionConfigDTO detectionConfigDTO = new DetectionConfigDTO();
    detectionConfigDTO.setId(125L);
    Map<String, Object> detectorSpecs = new HashMap<>();
    detectorSpecs.put("className", ThresholdRuleDetector.class.getName());
    Map<String, Object> properties = new HashMap<>();
    properties.put("metricUrn", "thirdeye:metric:123");
    properties.put("detector", "$threshold");
    detectionConfigDTO.setProperties(properties);
    Map<String, Object> componentSpecs = new HashMap<>();
    componentSpecs.put("threshold", detectorSpecs);
    detectionConfigDTO.setComponentSpecs(componentSpecs);

    this.testDataProvider = new MockDataProvider()
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setTimeseries(timeSeries);
  }

  @Test
  public void testThresholdAlgorithmRun() throws DetectorException {
    AnomalyDetector thresholdAlgorithm = new ThresholdRuleDetector();
    ThresholdRuleDetectorSpec spec = new ThresholdRuleDetectorSpec();
    spec.setMin(100);
    spec.setMax(500);
    thresholdAlgorithm.init(spec, new DefaultInputDataFetcher(testDataProvider, -1));
    DetectionResult result = thresholdAlgorithm.runDetection(new Interval(0, 10), "thirdeye:metric:123");
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 0);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 2);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 8);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 10);
    TimeSeries ts = result.getTimeseries();
    Assert.assertEquals(ts.getPredictedUpperBound(), DoubleSeries.fillValues(ts.size(), 500));
    Assert.assertEquals(ts.getPredictedLowerBound(), DoubleSeries.fillValues(ts.size(), 100));
    Assert.assertEquals(ts.getPredictedBaseline().values(), new double[]{100, 100L, 200L, 500L, 500L});
  }

  @Test
  public void testMonthlyDetectionThreshold() throws DetectorException {
    AnomalyDetector thresholdRule = new ThresholdRuleDetector();
    ThresholdRuleDetectorSpec spec = new ThresholdRuleDetectorSpec();
    spec.setMin(200);
    spec.setMonitoringGranularity("1_MONTHS");
    thresholdRule.init(spec, new DefaultInputDataFetcher(testDataProvider, -1));
    List<MergedAnomalyResultDTO> anomalies = thresholdRule.runDetection(new Interval(1546214400000L, 1551398400000L), "thirdeye:metric:123").getAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1546214400000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 1548892800000L);
  }

  @Test
  public void testMonthlyDetectionThresholdMax() throws DetectorException {
    AnomalyDetector thresholdRule = new ThresholdRuleDetector();
    ThresholdRuleDetectorSpec spec = new ThresholdRuleDetectorSpec();
    spec.setMax(200);
    spec.setMonitoringGranularity("1_MONTHS");
    thresholdRule.init(spec, new DefaultInputDataFetcher(testDataProvider, -1));
    List<MergedAnomalyResultDTO> anomalies = thresholdRule.runDetection(new Interval(1546214400000L, 1551398400000L), "thirdeye:metric:123").getAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1551312000000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 1553731200000L);
  }

}