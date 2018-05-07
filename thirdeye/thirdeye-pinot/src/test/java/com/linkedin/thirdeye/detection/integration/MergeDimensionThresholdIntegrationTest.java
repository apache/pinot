package com.linkedin.thirdeye.detection.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineLoader;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.DetectionTestUtils;
import com.linkedin.thirdeye.detection.MockDataProvider;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MergeDimensionThresholdIntegrationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

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

  private Map<MetricSlice, DataFrame> timeseries;
  private Map<MetricSlice, DataFrame> aggregates;
  private List<MetricConfigDTO> metrics;
  private List<MergedAnomalyResultDTO> anomalies;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    URL url = this.getClass().getResource("mergeDimensionThresholdProperties.json");
    this.properties = MAPPER.readValue(url, Map.class);

    Reader dataReader1 = new InputStreamReader(this.getClass().getResourceAsStream("timeseries1.csv"));
    this.data1 = DataFrame.fromCsv(dataReader1);

    Reader dataReader2 = new InputStreamReader(this.getClass().getResourceAsStream("timeseries2.csv"));
    this.data2 = DataFrame.fromCsv(dataReader2);

    this.loader = new DetectionPipelineLoader();

    this.aggregates = new HashMap<>();
    this.aggregates.put(MetricSlice.from(1, 0, 18000), this.data1);

    this.timeseries = new HashMap<>();
    this.timeseries.put(MetricSlice.from(2, 0, 18000), this.data2);

    this.metric2 = new MetricConfigDTO();
    this.metric2.setId(2L);
    this.metric2.setName("myMetric2");
    this.metric2.setDataset("myDataset2");

    this.metrics = new ArrayList<>();
    this.metrics.add(this.metric2);

    this.anomalies = new ArrayList<>();

    this.provider = new MockDataProvider()
        .setLoader(this.loader)
        .setAggregates(this.aggregates)
        .setTimeseries(this.timeseries)
        .setMetrics(this.metrics)
        .setAnomalies(this.anomalies);

    this.config = new DetectionConfigDTO();
    this.config.setClassName("com.linkedin.thirdeye.detection.algorithm.MergeWrapper");
    this.config.setProperties(this.properties);
  }

  @Test(enabled = false)
  public void testMergeDimensionThreshold() throws Exception {
    DetectionPipeline pipeline = this.loader.from(this.provider, this.config, 0, 18000);
    DetectionPipelineResult result = pipeline.run();

    // TODO debug and complete this test

    Assert.assertTrue(result.getAnomalies().contains(
        DetectionTestUtils.makeAnomaly(null, 10800, 14400, Collections.<String, String>emptyMap())));
    Assert.assertTrue(result.getAnomalies().contains(
        DetectionTestUtils.makeAnomaly(null, 0, 3600, ONE_TWO)));
    Assert.assertTrue(result.getAnomalies().contains(
        DetectionTestUtils.makeAnomaly(null, 3600, 7200, ONE_TWO)));
    Assert.assertTrue(result.getAnomalies().contains(
        DetectionTestUtils.makeAnomaly(null, 14400, 14401, ONE_TWO)));

    Assert.assertEquals(result.getLastTimestamp(), 14400);

    Assert.fail("not implemented yet");
  }

}
