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
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.algorithm.AlgorithmUtils;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class BaselineRuleDetectionStageTest {
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_OFFSET = "offset";
  private static final String PROP_CHANGE = "change";
  private static final String PROP_DIFFERENCE = "difference";

  private DataProvider provider;
  private DetectionPipeline detectionPipeline;
  private DataFrame data;
  private Map<String, Object> properties;
  private DetectionConfigDTO config;
  private Map<String, Object> stageSpecs;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    try (Reader dataReader = new InputStreamReader(AlgorithmUtils.class.getResourceAsStream("timeseries-4w.csv"))) {
      this.data = DataFrame.fromCsv(dataReader);
      this.data.setIndex(COL_TIME);
      this.data.addSeries(COL_TIME, this.data.getLongs(COL_TIME).multiply(1000));
    }

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(1L);
    metricConfigDTO.setName("thirdeye-test");
    metricConfigDTO.setDataset("thirdeye-test-dataset");

    Map<MetricSlice, DataFrame> timeseries = new HashMap<>();
    timeseries.put(MetricSlice.from(1L, 0L, 604800000L), this.data);
    timeseries.put(MetricSlice.from(1L, 604800000L, 1209600000L), this.data);
    timeseries.put(MetricSlice.from(1L, 1209600000L, 1814400000L), this.data);
    timeseries.put(MetricSlice.from(1L, 1814400000L, 2419200000L), this.data);

    this.properties = new HashMap<>();
    this.properties.put("stageClassName", BaselineRuleDetectionStage.class.getName());
    this.stageSpecs = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put("specs", this.stageSpecs);

    this.config = new DetectionConfigDTO();
    this.config.setProperties(properties);

    this.provider = new MockDataProvider()
        .setTimeseries(timeseries)
        .setMetrics(Collections.singletonList(metricConfigDTO));
  }

  @Test
  public void testWeekOverWeekDifference() throws Exception {
    this.stageSpecs.put(PROP_DIFFERENCE, 400);
    this.detectionPipeline = new AnomalyDetectionStageWrapper(this.provider, this.config, 1814400000L, 2419200000L);

    DetectionPipelineResult result = this.detectionPipeline.run();

    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 2376000000L);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 2372400000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 2376000000L);
  }

  @Test
  public void testWeekOverWeekChange() throws Exception {
    this.stageSpecs.put(PROP_CHANGE, 0.4);
    this.detectionPipeline = new AnomalyDetectionStageWrapper(this.provider, this.config, 1814400000L, 2419200000L);

    DetectionPipelineResult result = this.detectionPipeline.run();

    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 2383200000L);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 2372400000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 2376000000L);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 2379600000L);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 2383200000L);
  }

  @Test
  public void testThreeWeekMedianChange() throws Exception {
    this.stageSpecs.put(PROP_OFFSET, "median3w");
    this.stageSpecs.put(PROP_CHANGE, 0.3);
    this.detectionPipeline = new AnomalyDetectionStageWrapper(this.provider, this.config, 1814400000L, 2419200000L);

    DetectionPipelineResult result = this.detectionPipeline.run();

    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 2325600000L);
    Assert.assertEquals(anomalies.size(), 5);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 2005200000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 2008800000L);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 2134800000L);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 2138400000L);
    Assert.assertEquals(anomalies.get(2).getStartTime(), 2152800000L);
    Assert.assertEquals(anomalies.get(2).getEndTime(), 2156400000L);
    Assert.assertEquals(anomalies.get(3).getStartTime(), 2181600000L);
    Assert.assertEquals(anomalies.get(3).getEndTime(), 2185200000L);
    Assert.assertEquals(anomalies.get(4).getStartTime(), 2322000000L);
    Assert.assertEquals(anomalies.get(4).getEndTime(), 2325600000L);
  }

}
