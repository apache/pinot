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
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionTestUtils;
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
    aggregates.put(MetricSlice.from(123L, 0, 2),
        new DataFrame().addSeries(COL_VALUE, 0));
    aggregates.put(MetricSlice.from(123L, 4, 6),
        new DataFrame().addSeries(COL_VALUE, 200));
    aggregates.put(MetricSlice.from(123L, 6, 8),
        new DataFrame().addSeries(COL_VALUE, 500));
    aggregates.put(MetricSlice.from(123L, 8, 10),
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

    this.anomalies = Arrays.asList(makeAnomaly(0, 2), makeAnomaly(4, 6), makeAnomaly(6, 8), makeAnomaly(8, 10));

    this.runs = new ArrayList<>();

    this.loader = new MockPipelineLoader(this.runs, Collections.singletonList(
        new MockPipelineOutput(this.anomalies, 10)));

    this.testDataProvider = new MockDataProvider()
        .setLoader(this.loader)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAggregates(aggregates);
  }

  @Test(priority = 0)
  public void testThresholdRuleFilterNone() throws Exception {
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 0, 10);

    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 10);
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(0));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(1));
    Assert.assertEquals(anomalies.get(2), this.anomalies.get(2));
    Assert.assertEquals(anomalies.get(3), this.anomalies.get(3));
  }

  @Test(priority = 1)
  public void testThresholdRuleFilterMin() throws Exception {
    this.specs.put("min", 200);
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 0, 10);

    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 10);
    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(1));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(2));
    Assert.assertEquals(anomalies.get(2), this.anomalies.get(3));
  }

  @Test(priority = 2)
  public void testThresholdRuleFilterMax() throws Exception {
    this.specs.put("max", 500);
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 0, 10);

    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 10);
    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(0));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(1));
    Assert.assertEquals(anomalies.get(2), this.anomalies.get(2));
  }

  @Test(priority = 3)
  public void testThresholdRuleFilterBoth() throws Exception {
    this.specs.put("min", 200);
    this.specs.put("max", 500);
    this.thresholdRuleFilter = new AnomalyFilterWrapper(this.testDataProvider, this.config, 0, 10);

    DetectionPipelineResult result = this.thresholdRuleFilter.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 10);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(1));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(2));
  }

  private static MergedAnomalyResultDTO makeAnomaly(long start, long end) {
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(125L, start, end);
    anomaly.setMetricUrn(METRIC_URN);
    return anomaly;
  }
}
