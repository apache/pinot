package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.MockDataProvider;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class MovingWindowAlgorithmTest {
  private static final String COL_CURR = "current";
  private static final String COL_BASE = "baseline";
  private static final String COL_STD = "std";
  private static final String COL_MEAN = "mean";
  private static final String COL_QUANTILE = "quantile";
  private static final String COL_ZSCORE = "zscore";

  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_LOOKBACK = "lookback";
  private static final String PROP_QUANTILE = "quantile";
  private static final String PROP_ZSCORE = "zscore";
  private static final String PROP_WEEK_OVER_WEEK = "weekOverWeek";
  private static final String PROP_TIMEZONE = "timezone";

  private DataProvider provider;
  private MovingWindowAlgorithm algorithm;
  private DataFrame data;
  private Map<String, Object> properties;
  private DetectionConfigDTO config;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    try (Reader dataReader = new InputStreamReader(this.getClass().getResourceAsStream("timeseries-4w.csv"))) {
      this.data = DataFrame.fromCsv(dataReader);
      this.data.setIndex(COL_TIME);
      this.data.addSeries(COL_TIME, this.data.getLongs(COL_TIME).multiply(1000));
    }

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(1L);
    metricConfigDTO.setName("thirdeye-test");
    metricConfigDTO.setDataset("thirdeye-test-dataset");

    Map<MetricSlice, DataFrame> timeseries = new HashMap<>();
    timeseries.put(MetricSlice.from(1L, 0L, 2419200000L), this.data);
    timeseries.put(MetricSlice.from(1L, 604800000L, 2419200000L), this.data);

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_LOOKBACK, "2weeks");

    this.config = new DetectionConfigDTO();
    this.config.setProperties(properties);

    this.provider = new MockDataProvider()
        .setTimeseries(timeseries)
        .setMetrics(Collections.singletonList(metricConfigDTO));

  }

  // TODO complete test with data

  @Test
  public void testWindow() {
    this.properties.put(PROP_LOOKBACK, "2hours");
    this.properties.put(PROP_QUANTILE, 0.75);
    this.algorithm = new MovingWindowAlgorithm(this.provider, this.config, 3600000L, 9999999L);

    DataFrame input = new DataFrame(COL_TIME, LongSeries.buildFrom(0L, 3600000L, 7200000L, 14400000L))
        .addSeries(COL_VALUE, 1, 2, 3, 5);

    DataFrame output = new DataFrame(COL_TIME, LongSeries.buildFrom(0L, 3600000L, 7200000L, 14400000L))
        .addSeries(COL_VALUE, 1, 2, 3, 5)
        .addSeries(COL_STD, Double.NaN, Double.NaN, 0.7071067811865476, Double.NaN) // TODO avoid exact double compare
        .addSeries(COL_MEAN, Double.NaN, 1, 1.5, 3)
        .addSeries(COL_QUANTILE, Double.NaN, 1, 1.75, 3);

    DataFrame window = this.algorithm.applyMovingWindow(input);

    Assert.assertEquals(window, output);
  }

  @Test
  public void testLookbackParser() {
    Assert.assertEquals(
        MovingWindowAlgorithm.parseLookback("3600"), new Period().withField(DurationFieldType.millis(), 3600));
    Assert.assertEquals(MovingWindowAlgorithm.parseLookback("1d"), new Period().withField(DurationFieldType.days(), 1));
    Assert.assertEquals(
        MovingWindowAlgorithm.parseLookback("2hours"), new Period().withField(DurationFieldType.hours(), 2));
    Assert.assertEquals(
        MovingWindowAlgorithm.parseLookback("24 hrs"), new Period().withField(DurationFieldType.hours(), 24));
    Assert.assertEquals(
        MovingWindowAlgorithm.parseLookback("1 year"), new Period().withField(DurationFieldType.years(), 1));
    Assert.assertEquals(
        MovingWindowAlgorithm.parseLookback("  3   w  "), new Period().withField(DurationFieldType.weeks(), 3));
  }

  @Test
  public void testPeriodParser() {
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("ms"), PeriodType.millis());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("millis"), PeriodType.millis());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("s"), PeriodType.seconds());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("sec"), PeriodType.seconds());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("secs"), PeriodType.seconds());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("seconds"), PeriodType.seconds());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("m"), PeriodType.minutes());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("min"), PeriodType.minutes());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("mins"), PeriodType.minutes());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("minutes"), PeriodType.minutes());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("h"), PeriodType.hours());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("hour"), PeriodType.hours());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("hours"), PeriodType.hours());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("d"), PeriodType.days());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("day"), PeriodType.days());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("days"), PeriodType.days());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("w"), PeriodType.weeks());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("week"), PeriodType.weeks());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("weeks"), PeriodType.weeks());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("mon"), PeriodType.months());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("mons"), PeriodType.months());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("month"), PeriodType.months());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("months"), PeriodType.months());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("y"), PeriodType.years());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("year"), PeriodType.years());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("years"), PeriodType.years());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("a"), PeriodType.years());
    Assert.assertEquals(MovingWindowAlgorithm.parsePeriodType("ans"), PeriodType.years());
  }

//  @Test
//  public void testWeekOverWeekDifference() throws Exception {
//    this.properties.put(PROP_DIFFERENCE, 400);
//    this.algorithm = new BaselineAlgorithm(this.provider, this.config, 1814400000L, 2419200000L);
//
//    DetectionPipelineResult result = this.algorithm.run();
//
//    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
//    Assert.assertEquals(result.getLastTimestamp(), 2415600000L);
//    Assert.assertEquals(anomalies.size(), 1);
//    Assert.assertEquals(anomalies.get(0).getStartTime(), 2372400000L);
//    Assert.assertEquals(anomalies.get(0).getEndTime(), 2376000000L);
//  }
//
//  @Test
//  public void testWeekOverWeekChange() throws Exception {
//    this.properties.put(PROP_CHANGE, 0.4);
//    this.algorithm = new BaselineAlgorithm(this.provider, this.config, 1814400000L, 2419200000L);
//
//    DetectionPipelineResult result = this.algorithm.run();
//
//    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
//    Assert.assertEquals(result.getLastTimestamp(), 2415600000L);
//    Assert.assertEquals(anomalies.size(), 2);
//    Assert.assertEquals(anomalies.get(0).getStartTime(), 2372400000L);
//    Assert.assertEquals(anomalies.get(0).getEndTime(), 2376000000L);
//    Assert.assertEquals(anomalies.get(1).getStartTime(), 2379600000L);
//    Assert.assertEquals(anomalies.get(1).getEndTime(), 2383200000L);
//  }
//
//  @Test
//  public void testThreeWeekMedianChange() throws Exception {
//    this.properties.put(PROP_WEEKS, 3);
//    this.properties.put(PROP_AGGREGATION, BaselineAggregateType.MEDIAN.toString());
//    this.properties.put(PROP_CHANGE, 0.3);
//    this.algorithm = new BaselineAlgorithm(this.provider, this.config, 1814400000L, 2419200000L);
//
//    DetectionPipelineResult result = this.algorithm.run();
//
//    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
//    Assert.assertEquals(result.getLastTimestamp(), 2415600000L);
//    Assert.assertEquals(anomalies.size(), 5);
//    Assert.assertEquals(anomalies.get(0).getStartTime(), 2005200000L);
//    Assert.assertEquals(anomalies.get(0).getEndTime(), 2008800000L);
//    Assert.assertEquals(anomalies.get(1).getStartTime(), 2134800000L);
//    Assert.assertEquals(anomalies.get(1).getEndTime(), 2138400000L);
//    Assert.assertEquals(anomalies.get(2).getStartTime(), 2152800000L);
//    Assert.assertEquals(anomalies.get(2).getEndTime(), 2156400000L);
//    Assert.assertEquals(anomalies.get(3).getStartTime(), 2181600000L);
//    Assert.assertEquals(anomalies.get(3).getEndTime(), 2185200000L);
//    Assert.assertEquals(anomalies.get(4).getStartTime(), 2322000000L);
//    Assert.assertEquals(anomalies.get(4).getEndTime(), 2325600000L);
//  }

}
