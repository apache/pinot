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

package org.apache.pinot.thirdeye.detection.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionTestUtils;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MergeDimensionThresholdIntegrationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String METRIC = "myMetric2";
  private static final String DATASET = "myDataset2";
  private static final Map<String, String> ONE_TWO = new HashMap<>();
  static {
    ONE_TWO.put("a", "1");
    ONE_TWO.put("b", "2");
  }

  private Map<String, Object> properties;
  private DetectionPipelineLoader loader;
  private MockDataProvider provider;
  private DetectionConfigDTO config;

  private DataFrame data1;
  private DataFrame data2;
  private MetricConfigDTO metric2;
  private DatasetConfigDTO dataset;

  private Map<MetricSlice, DataFrame> timeseries;
  private Map<MetricSlice, DataFrame> aggregates;
  private List<MetricConfigDTO> metrics;
  private List<MergedAnomalyResultDTO> anomalies;
  private List<DatasetConfigDTO> datasets;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    URL url = this.getClass().getResource("mergeDimensionThresholdProperties.json");
    this.properties = MAPPER.readValue(url, Map.class);

    try (Reader dataReader = new InputStreamReader(this.getClass().getResourceAsStream("timeseries.csv"))) {
      this.data1 = DataFrame.fromCsv(dataReader);
    }

    try (Reader dataReader = new InputStreamReader(this.getClass().getResourceAsStream("timeseries.csv"))) {
      this.data2 = DataFrame.fromCsv(dataReader);
    }

    this.loader = new DetectionPipelineLoader();

    this.aggregates = new HashMap<>();
    this.aggregates.put(MetricSlice.from(1, 0, 18000), this.data1);

    this.timeseries = new HashMap<>();
    this.timeseries.put(MetricSlice.from(2, 0, 18000), this.data2);

    this.metric2 = new MetricConfigDTO();
    this.metric2.setId(2L);
    this.metric2.setName(METRIC);
    this.metric2.setDataset(DATASET);

    this.metrics = new ArrayList<>();
    this.metrics.add(this.metric2);

    this.dataset = new DatasetConfigDTO();
    this.dataset.setId(3L);
    this.dataset.setDataset(DATASET);
    this.dataset.setTimeDuration(1);
    this.dataset.setTimeUnit(TimeUnit.HOURS);
    this.dataset.setTimezone("UTC");

    this.datasets = new ArrayList<>();
    this.datasets.add(this.dataset);

    this.anomalies = new ArrayList<>();
    this.anomalies.add(makeAnomaly(9500, 10800, "thirdeye:metric:2"));

    this.provider = new MockDataProvider()
        .setLoader(this.loader)
        .setAggregates(this.aggregates)
        .setTimeseries(this.timeseries)
        .setMetrics(this.metrics)
        .setDatasets(this.datasets)
        .setAnomalies(this.anomalies);

    this.config = new DetectionConfigDTO();
    this.config.setProperties(this.properties);
    this.config.setId(-1L);
  }

  @Test
  public void testMergeDimensionThreshold() throws Exception {
    DetectionPipeline pipeline = this.loader.from(this.provider, this.config, 0, 18000);
    DetectionPipelineResult result = pipeline.run();

    Assert.assertEquals(result.getAnomalies().size(), 3);

    Assert.assertTrue(result.getAnomalies().contains(makeAnomaly(9500, 18000, "thirdeye:metric:2")));
    Assert.assertTrue(result.getAnomalies().contains(makeAnomaly(0, 7200, "thirdeye:metric:2:a%3D1:b%3D2")));
    Assert.assertTrue(result.getAnomalies().contains(makeAnomaly(14400, 18000, "thirdeye:metric:2:a%3D1:b%3D2")));
  }

  private static MergedAnomalyResultDTO makeAnomaly(long start, long end, String metricUrn) {
    MetricEntity me = MetricEntity.fromURN(metricUrn);

    Map<String, String> dimensions = new HashMap<>();
    for (Map.Entry<String, String> entry : me.getFilters().entries()) {
      dimensions.put(entry.getKey(), entry.getValue());
    }

    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(-1L, start, end, METRIC, DATASET, dimensions);
    anomaly.setMetricUrn(metricUrn);
    return anomaly;
  }
}
