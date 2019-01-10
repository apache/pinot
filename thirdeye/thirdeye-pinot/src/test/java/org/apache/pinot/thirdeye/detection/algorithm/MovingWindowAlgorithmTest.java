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

package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.DetectionTestUtils;
import com.linkedin.thirdeye.detection.MockDataProvider;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class MovingWindowAlgorithmTest {
  private static final String METRIC_NAME = "myMetric";
  private static final String DATASET_NAME = "myDataset";

  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_WINDOW_SIZE = "windowSize";
  private static final String PROP_LOOKBACK_PERIOD = "lookbackPeriod";
  private static final String PROP_REWORK_PERIOD = "reworkPeriod";
  private static final String PROP_QUANTILE_MIN = "quantileMin";
  private static final String PROP_QUANTILE_MAX = "quantileMax";
  private static final String PROP_ZSCORE_MIN = "zscoreMin";
  private static final String PROP_ZSCORE_MAX = "zscoreMax";
  private static final String PROP_KERNEL_SIZE = "kernelSize";
  private static final String PROP_ZSCORE_OUTLIER = "zscoreOutlier";
  private static final String PROP_CHANGE_DURATION = "changeDuration";
  private static final String PROP_BASELINE_WEEKS = "baselineWeeks";

  private DataProvider provider;
  private MovingWindowAlgorithm algorithm;
  private DataFrame data;
  private Map<String, Object> properties;
  private DetectionConfigDTO config;
  private List<MergedAnomalyResultDTO> anomalies;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    try (Reader dataReader = new InputStreamReader(this.getClass().getResourceAsStream("timeseries-4w.csv"))) {
      this.data = DataFrame.fromCsv(dataReader);
      this.data.setIndex(COL_TIME);
      this.data.addSeries(COL_TIME, this.data.getLongs(COL_TIME).multiply(1000));
    }

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(1L);
    metricConfigDTO.setName(METRIC_NAME);
    metricConfigDTO.setDataset(DATASET_NAME);

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setId(2L);
    datasetConfigDTO.setDataset(DATASET_NAME);
    datasetConfigDTO.setTimeDuration(1);
    datasetConfigDTO.setTimeUnit(TimeUnit.HOURS);

    Map<MetricSlice, DataFrame> timeseries = new HashMap<>();
    timeseries.put(MetricSlice.from(1L, 100000L, 300000L), this.data);
    timeseries.put(MetricSlice.from(1L, 0L, 2419200000L), this.data);
    timeseries.put(MetricSlice.from(1L, 0L, 1209600000L), this.data);
    timeseries.put(MetricSlice.from(1L, 604800000L, 2419200000L), this.data);

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_WINDOW_SIZE, "1week");
    this.properties.put(PROP_LOOKBACK_PERIOD, "0");
    this.properties.put(PROP_REWORK_PERIOD, "0");
    this.properties.put(PROP_ZSCORE_OUTLIER, Double.NaN);
    this.properties.put(PROP_CHANGE_DURATION, "0");

    this.config = new DetectionConfigDTO();
    this.config.setProperties(properties);
    this.config.setId(-1L);

    this.anomalies = new ArrayList<>();

    this.provider = new MockDataProvider()
        .setTimeseries(timeseries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAnomalies(this.anomalies);

  }

  //
  // quantile min
  //

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testQuantileMinTooLow() {
    this.properties.put(PROP_QUANTILE_MIN, -0.001);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testQuantileMinTooHigh() {
    this.properties.put(PROP_QUANTILE_MIN, 1.001);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
  }

  @Test
  public void testQuantileMinLimit() throws Exception {
    this.properties.put(PROP_QUANTILE_MIN, 0.0);
    this.properties.put(PROP_ZSCORE_OUTLIER, Double.NaN);
    this.properties.put(PROP_CHANGE_DURATION, "0");
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 12);
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(669600000L, 673200000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(896400000L, 900000000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(921600000L, 928800000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(954000000L, 961200000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1767600000L, 1771200000L)));
  }

  @Test
  public void testQuantileDiffMin() throws Exception {
    this.properties.put(PROP_QUANTILE_MIN, 0.01);
    this.properties.put(PROP_BASELINE_WEEKS, 1);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 1209600000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 9);
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1555200000L, 1558800000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1767600000L, 1771200000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2181600000L, 2185200000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2368800000L, 2372400000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2412000000L, 2415600000L)));
  }

  //
  // quantile max
  //

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testQuantileMaxTooLow() {
    this.properties.put(PROP_QUANTILE_MAX, -0.001);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testQuantileMaxTooHigh() {
    this.properties.put(PROP_QUANTILE_MAX, 1.001);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
  }

  @Test
  public void testQuantileMax() throws Exception {
    this.properties.put(PROP_QUANTILE_MAX, 0.975);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 12);
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(626400000L, 630000000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(705600000L, 709200000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(846000000L, 849600000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(892800000L, 896400000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(903600000L, 907200000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(918000000L, 921600000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(950400000L, 954000000L)));
  }

  @Test
  public void testQuantileMaxLimit() throws Exception {
    this.properties.put(PROP_QUANTILE_MAX, 1.0);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 15);
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(626400000L, 630000000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(846000000L, 849600000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(892800000L, 896400000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(903600000L, 907200000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(950400000L, 954000000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1576800000L, 1580400000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1756800000L, 1767600000L)));
  }

  //
  // zscore
  //

  @Test
  public void testZScoreMin() throws Exception {
    this.properties.put(PROP_ZSCORE_MIN, -2.5);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 5);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(921600000L, 928800000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(939600000L, 950400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(954000000L, 968400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1767600000L, 1778400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1792800000L, 1796400000L)));
  }

  @Test
  public void testZScoreMax() throws Exception {
    this.properties.put(PROP_ZSCORE_MAX, 2.5);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 2);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(950400000L, 954000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1756800000L, 1767600000L)));
  }

  @Test
  public void testZScoreDiffMax() throws Exception {
    this.properties.put(PROP_ZSCORE_MAX, 2.25);
    this.properties.put(PROP_KERNEL_SIZE, 4);
    this.properties.put(PROP_BASELINE_WEEKS, 1);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 1209600000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 4);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1278000000L, 1328400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1540800000L, 1594800000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1756800000L, 1767600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2372400000L, 2383200000L)));
  }

  //
  // kernel
  //

  @Test
  public void testKernelMin() throws Exception {
    this.properties.put(PROP_ZSCORE_MIN, -2.5);
    this.properties.put(PROP_KERNEL_SIZE, 4);
    this.properties.put(PROP_BASELINE_WEEKS, 1);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 1209600000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 2);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2221200000L, 2235600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2404800000L, 2415600000L)));
  }

  @Test
  public void testKernelMax() throws Exception {
    this.properties.put(PROP_ZSCORE_MAX, 2.5);
    this.properties.put(PROP_KERNEL_SIZE, 4);
    this.properties.put(PROP_BASELINE_WEEKS, 1);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 1209600000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 2);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1540800000L, 1594800000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2372400000L, 2379600000L)));
  }

  //
  // smoothing, outliers and change point correction
  //

  @Test
  public void testOutlierCorrection() throws Exception {
    this.data.set(COL_VALUE, this.data.getDoubles(COL_TIME).between(86400000L, 172800000L),
        this.data.getDoubles(COL_VALUE).add(500));

    this.properties.put(PROP_QUANTILE_MAX, 0.975);
    this.properties.put(PROP_ZSCORE_OUTLIER, 1.5);

    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 12);
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(626400000L, 630000000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(705600000L, 709200000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(846000000L, 849600000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(892800000L, 896400000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(903600000L, 907200000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(918000000L, 921600000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(928800000L, 932400000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(950400000L, 954000000L)));
//    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(972000000L, 975600000L)));
  }

  @Test
  public void testChangePointFromData() throws Exception {
    this.data.set(COL_VALUE, this.data.getDoubles(COL_TIME).between(298800000L, 903600000L),
        this.data.getDoubles(COL_VALUE).multiply(1.77));
    this.data.set(COL_VALUE, this.data.getDoubles(COL_TIME).gte(903600000L),
        this.data.getDoubles(COL_VALUE).multiply(3.61));

    this.properties.put(PROP_ZSCORE_MAX, 1.5);
    this.properties.put(PROP_ZSCORE_OUTLIER, 3.0);
    this.properties.put(PROP_CHANGE_DURATION, "2d");
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 2);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(892800000L, 896400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(903600000L, 1015200000L)));
  }

  @Test
  public void testChangePointFromAnomaly() throws Exception {
    this.data.set(COL_VALUE, this.data.getDoubles(COL_TIME).between(298800000L, 903600000L),
        this.data.getDoubles(COL_VALUE).multiply(1.77));
    this.data.set(COL_VALUE, this.data.getDoubles(COL_TIME).gte(903600000L),
        this.data.getDoubles(COL_VALUE).multiply(3.61));

    this.config.setId(1L);

    this.anomalies.add(makeAnomalyChangePoint(1L, 298800000L, 300000000L));
    this.anomalies.add(makeAnomalyChangePoint(1L, 903600000L, 910000000L));

    this.properties.put(PROP_ZSCORE_MAX, 2.5);
    this.properties.put(PROP_ZSCORE_OUTLIER, 3.0);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 3);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1L, 903600000L, 907200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1L, 918000000L, 921600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1L, 950400000L, 954000000L)));
  }

  //
  // baselines
  //

  // TODO test baseline aggregation over multiple weeks

  //
  // edge cases
  //

  @Test
  public void testNoData() throws Exception {
    this.properties.put(PROP_QUANTILE_MIN, 0.0);
    this.properties.put(PROP_QUANTILE_MAX, 1.0);
    this.properties.put(PROP_ZSCORE_MIN, -3.0);
    this.properties.put(PROP_ZSCORE_MAX, 3.0);
    this.properties.put(PROP_WINDOW_SIZE, "100secs");
    this.properties.put(PROP_LOOKBACK_PERIOD, "0");
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 200000L, 300000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 0);
  }

  @Test
  public void testMinLookback() throws Exception {
    this.properties.put(PROP_WINDOW_SIZE, "100secs");
    this.properties.put(PROP_LOOKBACK_PERIOD, 100000);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 300000L, 300000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 0);
  }

  //
  // utils
  //

  private static MergedAnomalyResultDTO makeAnomaly(long start, long end) {
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(-1L, start, end, METRIC_NAME, DATASET_NAME, Collections.<String, String>emptyMap());
    anomaly.setMetricUrn("thirdeye:metric:1");
    return anomaly;
  }

  private static MergedAnomalyResultDTO makeAnomaly(Long configId, long start, long end) {
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(configId, start, end, METRIC_NAME, DATASET_NAME, Collections.<String, String>emptyMap());
    anomaly.setMetricUrn("thirdeye:metric:1");
    return anomaly;
  }

  private static MergedAnomalyResultDTO makeAnomalyChangePoint(Long configId, long start, long end) {
    AnomalyFeedback feedback = new AnomalyFeedbackDTO();
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY_NEW_TREND);

    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(configId, start, end, METRIC_NAME, DATASET_NAME, Collections.<String, String>emptyMap());
    anomaly.setFeedback(feedback);

    return anomaly;
  }
}
