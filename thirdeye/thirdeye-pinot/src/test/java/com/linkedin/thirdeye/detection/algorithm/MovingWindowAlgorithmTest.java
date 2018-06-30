package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
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
import org.joda.time.DurationFieldType;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class MovingWindowAlgorithmTest {
  private static final String METRIC_NAME = "myMetric";
  private static final String DATASET_NAME = "myDataset";

  private static final String COL_STD = "std";
  private static final String COL_MEAN = "mean";
  private static final String COL_QUANTILE_MIN = "quantileMin";
  private static final String COL_QUANTILE_MAX = "quantileMax";
  private static final String COL_OUTLIER = "outlier";

  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_WINDOW_SIZE = "windowSize";
  private static final String PROP_MIN_LOOKBACK = "minLookback";
  private static final String PROP_QUANTILE_MIN = "quantileMin";
  private static final String PROP_QUANTILE_MAX = "quantileMax";
  private static final String PROP_ZSCORE_MIN = "zscoreMin";
  private static final String PROP_ZSCORE_MAX = "zscoreMax";
  private static final String PROP_KERNEL_MIN = "kernelMin";
  private static final String PROP_KERNEL_MAX = "kernelMax";
  private static final String PROP_OUTLIER_DURATION = "outlierDuration";
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

    Map<MetricSlice, DataFrame> timeseries = new HashMap<>();
    timeseries.put(MetricSlice.from(1L, 100000L, 300000L), this.data);
    timeseries.put(MetricSlice.from(1L, 0L, 2419200000L), this.data);
    timeseries.put(MetricSlice.from(1L, 0L, 1209600000L), this.data);
    timeseries.put(MetricSlice.from(1L, 604800000L, 2419200000L), this.data);

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_WINDOW_SIZE, "1week");

    this.config = new DetectionConfigDTO();
    this.config.setProperties(properties);

    this.anomalies = new ArrayList<>();

    this.provider = new MockDataProvider()
        .setTimeseries(timeseries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
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
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 5);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(669600000L, 673200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(896400000L, 900000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(921600000L, 928800000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(954000000L, 961200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1767600000L, 1771200000L)));
  }

  @Test
  public void testQuantileDiffMin() throws Exception {
    this.properties.put(PROP_QUANTILE_MIN, 0.01);
    this.properties.put(PROP_BASELINE_WEEKS, 1);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 1209600000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 5);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1555200000L, 1558800000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1767600000L, 1771200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2181600000L, 2185200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2368800000L, 2372400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2412000000L, 2415600000L)));
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

    Assert.assertEquals(res.getAnomalies().size(), 7);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(626400000L, 630000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(705600000L, 709200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(846000000L, 849600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(892800000L, 896400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(903600000L, 907200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(918000000L, 921600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(950400000L, 954000000L)));
  }

  @Test
  public void testQuantileMaxLimit() throws Exception {
    this.properties.put(PROP_QUANTILE_MAX, 1.0);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 7);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(626400000L, 630000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(846000000L, 849600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(892800000L, 896400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(903600000L, 907200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(950400000L, 954000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1576800000L, 1580400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1756800000L, 1767600000L)));
  }

  //
  // zscore
  //

  @Test
  public void testZScoreMin() throws Exception {
    this.properties.put(PROP_ZSCORE_MIN, -2.5);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 4);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(925200000L, 928800000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(957600000L, 961200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1767600000L, 1774800000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1792800000L, 1796400000L)));
  }

  @Test
  public void testZScoreMax() throws Exception {
    this.properties.put(PROP_ZSCORE_MAX, 2.5);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 5);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(903600000L, 907200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(918000000L, 921600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(950400000L, 954000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1764000000L, 1767600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1803600000L, 1807200000L)));
  }

  @Test
  public void testZScoreDiffMax() throws Exception {
    this.properties.put(PROP_ZSCORE_MAX, 2.0);
    this.properties.put(PROP_BASELINE_WEEKS, 1);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 1209600000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 8);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1274400000L, 1278000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1335600000L, 1339200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1551600000L, 1555200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1562400000L, 1566000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1580400000L, 1584000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1760400000L, 1764000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2372400000L, 2376000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2379600000L, 2383200000L)));
  }

  //
  // kernel
  //

  @Test
  public void testKernelMin() throws Exception {
    this.properties.put(PROP_KERNEL_MIN, -1.0);
    this.properties.put(PROP_BASELINE_WEEKS, 1);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 1209600000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 2);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2224800000L, 2228400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(2404800000L, 2412000000L)));
  }

  @Test
  public void testKernelMax() throws Exception {
    this.properties.put(PROP_KERNEL_MAX, 1.0);
    this.properties.put(PROP_BASELINE_WEEKS, 1);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 1209600000L, 2419200000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 1);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1278000000L, 1285200000L)));
  }

  //
  // smoothing, outliers and change point correction
  //

  @Test
  public void testOutlierCorrection() throws Exception {
    this.data.set(COL_VALUE, this.data.getDoubles(COL_TIME).between(86400000L, 172800000L),
        this.data.getDoubles(COL_VALUE).add(500));

    this.properties.put(PROP_QUANTILE_MAX, 0.975);
    this.properties.put(PROP_OUTLIER_DURATION, "12h");
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 8);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(626400000L, 630000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(705600000L, 709200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(813600000L, 817200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(846000000L, 849600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(892800000L, 896400000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(903600000L, 907200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(918000000L, 921600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(950400000L, 954000000L)));
  }

  @Test
  public void testChangePointFromData() throws Exception {
    this.data.set(COL_VALUE, this.data.getDoubles(COL_TIME).between(298800000L, 903600000L),
        this.data.getDoubles(COL_VALUE).multiply(1.77));
    this.data.set(COL_VALUE, this.data.getDoubles(COL_TIME).gte(903600000L),
        this.data.getDoubles(COL_VALUE).multiply(3.61));

    this.properties.put(PROP_QUANTILE_MAX, 0.975);
    this.properties.put(PROP_CHANGE_DURATION, "2d");
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 6);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(626400000L, 630000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(705600000L, 709200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(813600000L, 817200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(846000000L, 849600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(903600000L, 907200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(950400000L, 954000000L)));
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

    this.properties.put(PROP_QUANTILE_MAX, 0.975);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 604800000L, 1209600000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 7);
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1L, 626400000L, 630000000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1L, 705600000L, 709200000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1L, 846000000L, 849600000L)));
    Assert.assertTrue(res.getAnomalies().contains(makeAnomaly(1L, 892800000L, 896400000L)));
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
    this.properties.put(PROP_MIN_LOOKBACK, "0");
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 200000L, 300000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 0);
  }

  @Test
  public void testMinLookback() throws Exception {
    this.properties.put(PROP_WINDOW_SIZE, "100secs");
    this.properties.put(PROP_MIN_LOOKBACK, 100000);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 250000L, 300000L);
    DetectionPipelineResult res = this.algorithm.run();

    Assert.assertEquals(res.getAnomalies().size(), 0);
  }

  //
  // helper tests
  //

//  @Test
//  public void testWindow() {
//    this.properties.put(PROP_WINDOW_SIZE, "2hours");
//    this.properties.put(PROP_QUANTILE_MIN, 0.25);
//    this.properties.put(PROP_QUANTILE_MAX, 0.75);
//    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 3600000L, 9999999L);
//
//    DataFrame input = new DataFrame(COL_TIME, LongSeries.buildFrom(0L, 3600000L, 7200000L, 14400000L))
//        .addSeries(COL_VALUE, 1, 2, 3, 5)
//        .addSeries(COL_OUTLIER, false, false, false, false);
//
//    DataFrame output = new DataFrame(COL_TIME, LongSeries.buildFrom(0L, 3600000L, 7200000L, 14400000L))
//        .addSeries(COL_VALUE, 1, 2, 3, 5)
//        .addSeries(COL_STD, Double.NaN, Double.NaN, 0.7071067811865476, Double.NaN) // TODO avoid exact double compare
//        .addSeries(COL_MEAN, Double.NaN, 1, 1.5, Double.NaN)
//        .addSeries(COL_QUANTILE_MIN, Double.NaN, 1, 1.25, Double.NaN)
//        .addSeries(COL_QUANTILE_MAX, Double.NaN, 1, 1.75, Double.NaN)
//        .addSeries(COL_OUTLIER, false, false, false, false);
//
//    DataFrame window = this.algorithm.applyMovingWindow(input, new TreeSet<Long>());
//
//    Assert.assertEquals(window, output);
//  }

  //
  // utils
  //

  private static MergedAnomalyResultDTO makeAnomaly(long start, long end) {
    return DetectionTestUtils.makeAnomaly(null, start, end, METRIC_NAME, DATASET_NAME, Collections.<String, String>emptyMap());
  }

  private static MergedAnomalyResultDTO makeAnomaly(Long configId, long start, long end) {
    return DetectionTestUtils.makeAnomaly(configId, start, end, METRIC_NAME, DATASET_NAME, Collections.<String, String>emptyMap());
  }

  private static MergedAnomalyResultDTO makeAnomalyChangePoint(Long configId, long start, long end) {
    AnomalyFeedback feedback = new AnomalyFeedbackDTO();
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY_NEW_TREND);

    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(configId, start, end, METRIC_NAME, DATASET_NAME, Collections.<String, String>emptyMap());
    anomaly.setFeedback(feedback);

    return anomaly;
  }
}
