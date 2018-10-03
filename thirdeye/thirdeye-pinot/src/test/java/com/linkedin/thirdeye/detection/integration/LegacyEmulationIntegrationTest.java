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

package com.linkedin.thirdeye.detection.integration;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineLoader;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.algorithm.LegacyAlertFilterWrapper;
import com.linkedin.thirdeye.detection.algorithm.LegacyAnomalyFunctionAlgorithm;
import com.linkedin.thirdeye.detection.algorithm.LegacyDimensionWrapper;
import com.linkedin.thirdeye.detection.algorithm.LegacyMergeWrapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class LegacyEmulationIntegrationTest {
  private DataProvider testDataProvider;
  private DetectionPipeline detectionPipeline;
  private DetectionConfigDTO config;
  private Map<String, Object> properties;

  @BeforeMethod
  public void beforeMethod() {
    Map<MetricSlice, DataFrame> timeSeries = new HashMap<>();
    timeSeries.put(MetricSlice.from(123L, 0, 10),
        new DataFrame().addSeries(COL_VALUE, 0, 50, 100, 200, 500, 1000).addSeries(COL_TIME, 0, 1, 2, 4, 6, 8));

    Map<MetricSlice, DataFrame> aggregates = new HashMap<>();
    aggregates.put(MetricSlice.from(123L, 0, 10),
        new DataFrame().addSeries(COL_VALUE, 1850));

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

    Map<String, Object> specs = new HashMap<>();
    specs.put("properties", "min=100;max=500");
    specs.put("metric", "thirdeye-test");
    specs.put("metricId", 123);
    specs.put("bucketSize", 1);
    specs.put("bucketUnit", "MILLISECONDS");

    this.properties = new HashMap<>();
    this.properties.put("specs", specs);
    this.properties.put("anomalyFunctionClassName", "com.linkedin.thirdeye.anomalydetection.function.MinMaxThresholdFunction");
    this.properties.put("alertFilterLookBack", "0");

    this.config = new DetectionConfigDTO();
    this.config.setId(125L);
    this.config.setProperties(properties);

    this.testDataProvider = new MockDataProvider()
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAnomalies(Collections.<MergedAnomalyResultDTO>emptyList())
        .setAggregates(aggregates)
        .setTimeseries(timeSeries)
        .setLoader(new DetectionPipelineLoader());

    this.detectionPipeline = null;
  }

  @Test
  public void testLegacyAnomalyFunction() throws Exception {
    this.detectionPipeline = new LegacyAnomalyFunctionAlgorithm(this.testDataProvider, this.config, 0, 10);
    DetectionPipelineResult result = this.detectionPipeline.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 9);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 2);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 8);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 9);
  }

  @Test
  public void testLegacyDimensionWrapper() throws Exception {
    this.detectionPipeline = new LegacyDimensionWrapper(this.testDataProvider, this.config, 0, 10);
    DetectionPipelineResult result = this.detectionPipeline.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 9);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 2);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 8);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 9);
  }

  @Test
  public void testLegacyMergeWrapper() throws Exception {
    this.detectionPipeline = new LegacyMergeWrapper(this.testDataProvider, this.config, 0, 10);
    DetectionPipelineResult result = this.detectionPipeline.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 9);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 9);
  }

  @Test
  public void testLegacyAlertFilterWrapper() throws Exception {
    this.detectionPipeline = new LegacyAlertFilterWrapper(this.testDataProvider, this.config, 0, 10);
    DetectionPipelineResult result = this.detectionPipeline.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 9);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 9);
  }
}
