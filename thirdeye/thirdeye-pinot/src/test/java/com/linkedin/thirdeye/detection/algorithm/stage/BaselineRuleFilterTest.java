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

package com.linkedin.thirdeye.detection.algorithm.stage;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.DetectionTestUtils;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.MockPipeline;
import com.linkedin.thirdeye.detection.MockPipelineLoader;
import com.linkedin.thirdeye.detection.MockPipelineOutput;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class BaselineRuleFilterTest {
  private static final String METRIC_URN = "thirdeye:metric:123";

  private List<MergedAnomalyResultDTO> anomalies;
  private MockPipelineLoader loader;
  private List<MockPipeline> runs;
  private DataProvider testDataProvider;
  private AnomalyFilterStageWrapper baselineRulefilterWrapper;
  private Map<String, Object> properties;
  private DetectionConfigDTO config;
  private Map<String, Object> specs;
  private Baseline baseline;

  @BeforeMethod
  public void beforeMethod() {
    this.baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MEDIAN, 1, 1, DateTimeZone.forID("UTC"));

    MetricSlice slice1 = MetricSlice.from(123L, 0, 2);
    MetricSlice baselineSlice1 = this.baseline.scatter(slice1).get(0);
    MetricSlice slice2 = MetricSlice.from(123L, 4, 6);
    MetricSlice baselineSlice2 = this.baseline.scatter(slice2).get(0);

    Map<MetricSlice, DataFrame> aggregates = new HashMap<>();
    aggregates.put(slice1,
        new DataFrame().addSeries(COL_VALUE, 150));
    aggregates.put(baselineSlice1,
        new DataFrame().addSeries(COL_VALUE, 200));
    aggregates.put(slice2,
        new DataFrame().addSeries(COL_VALUE, 500));
    aggregates.put(baselineSlice2,
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
    this.properties.put("stageClassName", BaselineRuleFilterStage.class.getName());
    this.properties.put("offset", "median1w");
    this.specs = new HashMap<>();

    this.properties.put("specs", specs);
    this.config.setProperties(this.properties);

    this.anomalies = Arrays.asList(makeAnomaly(0, 2), makeAnomaly(4, 6));

    this.runs = new ArrayList<>();

    this.loader = new MockPipelineLoader(this.runs, Collections.singletonList(
        new MockPipelineOutput(this.anomalies, 10)));

    this.testDataProvider = new MockDataProvider()
        .setLoader(this.loader)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAggregates(aggregates);
  }

  @Test
  public void testBaselineRuleFilterNone() throws Exception {
    this.baselineRulefilterWrapper = new AnomalyFilterStageWrapper(this.testDataProvider, this.config, 0, 10);

    DetectionPipelineResult result = this.baselineRulefilterWrapper.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 6);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(0));
    Assert.assertEquals(anomalies.get(1), this.anomalies.get(1));
  }

  @Test
  public void testBaselineRuleFilterChangePercentage() throws Exception {
    this.specs.put("changeThreshold", 0.5);
    this.baselineRulefilterWrapper = new AnomalyFilterStageWrapper(this.testDataProvider, this.config, 0, 10);

    DetectionPipelineResult result = this.baselineRulefilterWrapper.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 6);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(1));
  }

  @Test
  public void testBaselineRuleFilterDifference() throws Exception {
    this.specs.put("differenceThreshold", 100);
    this.baselineRulefilterWrapper = new AnomalyFilterStageWrapper(this.testDataProvider, this.config, 0, 10);

    DetectionPipelineResult result = this.baselineRulefilterWrapper.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 6);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(1));
  }

  @Test
  public void testSiteWideImpactRuleFilter() throws Exception {
    this.specs.put("siteWideImpactThreshold", 0.5);
    this.baselineRulefilterWrapper = new AnomalyFilterStageWrapper(this.testDataProvider, this.config, 0, 10);

    DetectionPipelineResult result = this.baselineRulefilterWrapper.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 6);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0), this.anomalies.get(1));
  }

  private static MergedAnomalyResultDTO makeAnomaly(long start, long end) {
    Map<String, String> dimensions = new HashMap<>();
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(125L, start, end, dimensions);
    anomaly.setMetricUrn(METRIC_URN);
    return anomaly;
  }
}
