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

import com.google.common.collect.ImmutableMap;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionTestUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.MockPipeline;
import org.apache.pinot.thirdeye.detection.MockPipelineLoader;
import org.apache.pinot.thirdeye.detection.MockPipelineOutput;
import org.apache.pinot.thirdeye.detection.wrapper.AnomalyFilterWrapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public class ThresholdRuleAnomalyFilterTest {
  private static final String METRIC_URN = "thirdeye:metric:123";

  private List<MergedAnomalyResultDTO> anomalies;
  private MockPipelineLoader loader;
  private List<MockPipeline> runs;
  private DataProvider testDataProvider;
  private AnomalyFilterWrapper thresholdRuleFilter;
  private Map<String, Object> properties;
  private DetectionConfigDTO config;
  private Map<String, Object> specs;

  @BeforeMethod
  public void beforeMethod() {
    Map<MetricSlice, DataFrame> aggregates = new HashMap<>();
    aggregates.put(MetricSlice.from(123L, 1551186000000L, 1551189600000L),
        new DataFrame().addSeries(COL_VALUE, 0));
    aggregates.put(MetricSlice.from(123L, 1551189600000L, 1551193200000L),
        new DataFrame().addSeries(COL_VALUE, 200));
    aggregates.put(MetricSlice.from(123L, 1551193200000L, 1551196800000L),
        new DataFrame().addSeries(COL_VALUE, 500));
    aggregates.put(MetricSlice.from(123L, 1551196800000L, 1551200400000L),
        new DataFrame().addSeries(COL_VALUE, 1000));

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

    this.config = new DetectionConfigDTO();
    this.config.setId(125L);
    this.properties = new HashMap<>();
    this.properties.put("nested", Collections.singletonList(Collections.singletonMap("className", "dummy")));
    this.properties.put("filter", "$abc");

    this.specs = new HashMap<>();
    this.specs.put("className", ThresholdRuleAnomalyFilter.class.getName());
    this.config.setComponentSpecs(ImmutableMap.of("abc", this.specs));
    this.config.setProperties(this.properties);

    this.anomalies = Arrays.asList(makeAnomaly(1551186000000L, 1551189600000L), makeAnomaly(1551189600000L, 1551193200000L), makeAnomaly(1551193200000L, 1551196800000L), makeAnomaly(1551196800000L, 1551200400000L));

    this.runs = new ArrayList<>();

    this.loader = new MockPipelineLoader(this.runs, Collections.singletonList(
        new MockPipelineOutput(this.anomalies, 1551200400000L)));

    this.testDataProvider = new MockDataProvider()
        .setLoader(this.loader)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAggregates(aggregates);
  }

  @Test(priority = 0)
  public void testThresholdRuleFilterNone() throws Exception {
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 1551186000000L, 1551189600000L);

    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 1551200400000L);
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(0));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(1));
    Assert.assertEquals(anomalies.get(2), this.anomalies.get(2));
    Assert.assertEquals(anomalies.get(3), this.anomalies.get(3));
  }

  @Test(priority = 1)
  public void testThresholdRuleFilterMin() throws Exception {
    this.specs.put("minValueHourly", 200);
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 1551186000000L, 1551189600000L);

    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 1551200400000L);
    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(1));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(2));
    Assert.assertEquals(anomalies.get(2), this.anomalies.get(3));
  }

  @Test(priority = 2)
  public void testThresholdRuleFilterMax() throws Exception {
    this.specs.put("maxValueHourly", 500);
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 1551186000000L, 1551189600000L);

    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 1551200400000L);
    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(0));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(1));
    Assert.assertEquals(anomalies.get(2), this.anomalies.get(2));
  }

  @Test(priority = 3)
  public void testThresholdRuleFilterBoth() throws Exception {
    this.specs.put("minValueHourly", 200);
    this.specs.put("maxValueHourly", 500);
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 1551186000000L, 1551189600000L);

    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 1551200400000L);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(1));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(2));
  }

  @Test(priority = 4)
  public void testThresholdRuleFilterMinDaily() throws Exception {
    this.specs.put("minValueDaily", 2400);
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 1551186000000L, 1551189600000L);

    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 1551200400000L);
    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(1));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(2));
    Assert.assertEquals(anomalies.get(2), this.anomalies.get(3));
  }

  @Test(priority = 5)
  public void testThresholdRuleFilterMaxDaily() throws Exception {
    this.specs.put("maxValueDaily", 12000);
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 1551186000000L, 1551189600000L);

    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 1551200400000L);
    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(0));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(1));
    Assert.assertEquals(anomalies.get(2), this.anomalies.get(2));
  }

  @Test(priority = 6)
  public void testThresholdRuleFilterCurrentMinAndMax() throws Exception {
    this.specs.put("minValue", 300);
    this.specs.put("maxValue", 500);
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 1551186000000L, 1551189600000L);
    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 1551200400000L);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(2));
  }


  private static MergedAnomalyResultDTO makeAnomaly(long start, long end) {
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(125L, start, end);
    anomaly.setMetricUrn(METRIC_URN);
    return anomaly;
  }
}
