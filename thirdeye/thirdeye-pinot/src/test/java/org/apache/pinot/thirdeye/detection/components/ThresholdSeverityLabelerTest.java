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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.anomaly.AnomalySeverity;
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
import org.apache.pinot.thirdeye.detection.wrapper.AnomalyLabelerWrapper;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;
import static org.apache.pinot.thirdeye.detection.spec.SeverityThresholdLabelerSpec.Threshold;

public class ThresholdSeverityLabelerTest {
  private static final String METRIC_URN = "thirdeye:metric:123";
  private static final long CONFIG_ID = 125L;

  private List<MergedAnomalyResultDTO> anomalies;
  private MockPipelineLoader loader;
  private List<MockPipeline> runs;
  private DataProvider testDataProvider;
  private Map<String, Object> properties;
  private DetectionConfigDTO config;
  private Map<String, Object> specs;
  private AnomalyLabelerWrapper thresholdSeverityLabeler;

  @BeforeMethod
  public void beforeMethod(){
    Map<MetricSlice, DataFrame> aggregates = new HashMap<>();
    aggregates.put(MetricSlice.from(123L, 1000L, 2000L), new DataFrame().addSeries(COL_VALUE, 1200));
    aggregates.put(MetricSlice.from(123L, 2000L, 3000L), new DataFrame().addSeries(COL_VALUE, 1600));
    aggregates.put(MetricSlice.from(123L, 3000L, 4000L), new DataFrame().addSeries(COL_VALUE, 4800));
    aggregates.put(MetricSlice.from(123L, 4000L, 6000L), new DataFrame().addSeries(COL_VALUE, 2500));

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
    this.config.setId(CONFIG_ID);
    this.properties = new HashMap<>();
    this.properties.put("nested", Collections.singletonList(Collections.singletonMap("className", "dummy")));
    this.properties.put("labeler", "$test_labeler");
    this.specs = new HashMap<>();
    this.specs.put("className", ThresholdSeverityLabeler.class.getName());

    this.config.setComponentSpecs(ImmutableMap.of("test_labeler", this.specs));
    this.config.setProperties(this.properties);

    this.anomalies =
        Arrays.asList(DetectionTestUtils.makeAnomaly(1000L, 2000L, METRIC_URN, 1200, 1000),
            DetectionTestUtils.makeAnomaly(2000L, 3000, METRIC_URN, 1700, 2000),
            DetectionTestUtils.makeAnomaly(3000L, 4000L, METRIC_URN, 4800, 5000),
            DetectionTestUtils.makeAnomaly(4000L, 6000L, METRIC_URN, 2500, 3000));
    this.runs = new ArrayList<>();
    this.loader = new MockPipelineLoader(this.runs,
        Collections.singletonList(new MockPipelineOutput(this.anomalies, 6000L)));
    this.testDataProvider = new MockDataProvider().setLoader(this.loader)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAggregates(aggregates);
  }


  @Test
  public void testLabeling() throws Exception {
    Map<String, Object> severityMap = new HashMap<>();
    severityMap.put(AnomalySeverity.CRITICAL.toString(), new Threshold(0.2, 3000));
    severityMap.put(AnomalySeverity.HIGH.toString(), new Threshold(0.15, 2000));
    severityMap.put(AnomalySeverity.MEDIUM.toString(), new Threshold(0.12, 1500));
    this.specs.put("severity", severityMap);
    this.thresholdSeverityLabeler = new AnomalyLabelerWrapper(this.testDataProvider, this.config, 1000L, 6000L);
    DetectionPipelineResult result = this.thresholdSeverityLabeler.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(anomalies.get(0).getSeverityLabel(), AnomalySeverity.CRITICAL);
    Assert.assertEquals(anomalies.get(1).getSeverityLabel(), AnomalySeverity.HIGH);
    Assert.assertEquals(anomalies.get(2).getSeverityLabel(), AnomalySeverity.DEFAULT);
    Assert.assertEquals(anomalies.get(3).getSeverityLabel(), AnomalySeverity.HIGH);
  }

  @Test
  public void testLabelingSingleThreshold() throws Exception {
    Map<String, Object> severityMap = new HashMap<>();
    Threshold singleThreshold = new Threshold();
    singleThreshold.duration = 2000L;
    severityMap.put(AnomalySeverity.CRITICAL.toString(), singleThreshold);
    severityMap.put(AnomalySeverity.HIGH.toString(), new Threshold(0.15, 2000));
    this.specs.put("severity", severityMap);
    this.thresholdSeverityLabeler = new AnomalyLabelerWrapper(this.testDataProvider, this.config, 1000L, 6000L);
    DetectionPipelineResult result = this.thresholdSeverityLabeler.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(anomalies.get(0).getSeverityLabel(), AnomalySeverity.HIGH);
    Assert.assertEquals(anomalies.get(1).getSeverityLabel(), AnomalySeverity.HIGH);
    Assert.assertEquals(anomalies.get(2).getSeverityLabel(), AnomalySeverity.DEFAULT);
    Assert.assertEquals(anomalies.get(3).getSeverityLabel(), AnomalySeverity.CRITICAL);
  }

  @Test
  public void testLabelingHigherSeverity() throws Exception {
    Map<String, Object> severityMap = new HashMap<>();
    severityMap.put(AnomalySeverity.CRITICAL.toString(), new Threshold(0.2, 3000));
    severityMap.put(AnomalySeverity.HIGH.toString(), new Threshold(0.15, 2000));
    severityMap.put(AnomalySeverity.MEDIUM.toString(), new Threshold(0.12, 1500));
    this.specs.put("severity", severityMap);
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(4000L, 6000L, METRIC_URN, 2400, 3000);
    anomaly.setSeverityLabel(AnomalySeverity.HIGH);
    anomaly.setId(125L);
    this.anomalies.set(this.anomalies.size() - 1, anomaly);
    this.thresholdSeverityLabeler = new AnomalyLabelerWrapper(this.testDataProvider, this.config, 1000L, 6000L);
    DetectionPipelineResult result = this.thresholdSeverityLabeler.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(anomalies.get(3).getSeverityLabel(), AnomalySeverity.CRITICAL);
    Assert.assertTrue(anomalies.get(3).isRenotify());

  }

  @Test
  public void testLabelingLowerSeverity() throws Exception {
    Map<String, Object> severityMap = new HashMap<>();
    severityMap.put(AnomalySeverity.CRITICAL.toString(), new Threshold(0.2, 3000));
    severityMap.put(AnomalySeverity.HIGH.toString(), new Threshold(0.15, 2000));
    severityMap.put(AnomalySeverity.MEDIUM.toString(), new Threshold(0.10, 1500));
    this.specs.put("severity", severityMap);
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(4200L, 6000L, METRIC_URN, 2700, 3000);
    anomaly.setSeverityLabel(AnomalySeverity.HIGH);
    anomaly.setId(125L);
    this.anomalies.set(this.anomalies.size() - 1, anomaly);
    this.thresholdSeverityLabeler = new AnomalyLabelerWrapper(this.testDataProvider, this.config, 1000L, 6000L);
    DetectionPipelineResult result = this.thresholdSeverityLabeler.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(anomalies.get(3).getSeverityLabel(), AnomalySeverity.MEDIUM);
    Assert.assertFalse(anomalies.get(3).isRenotify());
  }
}
