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

import java.io.InputStreamReader;
import java.io.Reader;
import java.time.DayOfWeek;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.algorithm.AlgorithmUtils;
import org.apache.pinot.thirdeye.detection.spec.PercentageChangeRuleDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.exception.DetectorException;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PercentageChangeRuleDetectorTest {

  private DataProvider provider;
  private DataFrame data;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    try (Reader dataReader = new InputStreamReader(AlgorithmUtils.class.getResourceAsStream("timeseries-4w.csv"))) {
      this.data = DataFrame.fromCsv(dataReader);
      this.data.setIndex(DataFrame.COL_TIME);
      this.data.addSeries(DataFrame.COL_TIME, this.data.getLongs(DataFrame.COL_TIME).multiply(1000));
    }

    DataFrame weeklyData;
    try (Reader dataReader = new InputStreamReader(AlgorithmUtils.class.getResourceAsStream("timeseries-2y.csv"))) {
      weeklyData = DataFrame.fromCsv(dataReader);
      weeklyData.setIndex(DataFrame.COL_TIME);
    }

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(1L);
    metricConfigDTO.setName("thirdeye-test");
    metricConfigDTO.setDataset("thirdeye-test-dataset");

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setTimeUnit(TimeUnit.HOURS);
    datasetConfigDTO.setDataset("thirdeye-test-dataset");
    datasetConfigDTO.setTimeDuration(1);

    Map<MetricSlice, DataFrame> timeseries = new HashMap<>();
    timeseries.put(MetricSlice.from(1L, 0L, 604800000L), this.data);
    timeseries.put(MetricSlice.from(1L, 604800000L, 1209600000L), this.data);
    timeseries.put(MetricSlice.from(1L, 1209600000L, 1814400000L), this.data);
    timeseries.put(MetricSlice.from(1L, 1814400000L, 2419200000L), this.data);
    timeseries.put(MetricSlice.from(1L, 1560816000000L, 1562025600000L), weeklyData);
    timeseries.put(MetricSlice.from(1L, 1561420800000L, 1562630400000L), weeklyData);

    timeseries.put(MetricSlice.from(1L, 1546214400000L, 1551312000000L),
        new DataFrame().addSeries(DataFrame.COL_TIME, 1546214400000L, 1548892800000L).addSeries(
            DataFrame.COL_VALUE, 100, 200));
    timeseries.put(MetricSlice.from(1L, 1543536000000L, 1548633600000L),
        new DataFrame().addSeries(DataFrame.COL_TIME, 1543536000000L, 1546214400000L)
            .addSeries(DataFrame.COL_VALUE, 100, 100));
    timeseries.put(MetricSlice.from(1L, 1551398400000L, 1551571200000L),
        new DataFrame().addSeries(DataFrame.COL_TIME, 1551398400000L, 1551484800000L).addSeries(
            DataFrame.COL_VALUE, 0, 200));
    timeseries.put(MetricSlice.from(1L, 1550793600000L, 1550966400000L),
        new DataFrame().addSeries(DataFrame.COL_TIME, 1550793600000L, 1550880000000L).addSeries(
            DataFrame.COL_VALUE, 0, 0));

    this.provider = new MockDataProvider()
        .setTimeseries(timeseries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO));
  }

  @Test
  public void testWeekOverWeekChange() {
    PercentageChangeRuleDetector detector = new PercentageChangeRuleDetector();
    PercentageChangeRuleDetectorSpec spec = new PercentageChangeRuleDetectorSpec();
    spec.setPattern("up");
    double percentageChange = 0.4;
    spec.setPercentageChange(percentageChange);
    detector.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    DetectionResult detectionResult = detector.runDetection(new Interval(1814400000L, 2419200000L), "thirdeye:metric:1");
    List<MergedAnomalyResultDTO> anomalies = detectionResult.getAnomalies();
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 2372400000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 2376000000L);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 2379600000L);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 2383200000L);
    TimeSeries ts = detectionResult.getTimeseries();
    checkPercentageUpperBounds(ts, percentageChange);
    Assert.assertEquals(ts.getPredictedLowerBound(), DoubleSeries.zeros(ts.size()));
  }

  @Test
  public void testThreeWeekMedianChange() {
    PercentageChangeRuleDetector detector = new PercentageChangeRuleDetector();
    PercentageChangeRuleDetectorSpec spec = new PercentageChangeRuleDetectorSpec();
    double percentageChange = 0.3;
    spec.setPercentageChange(percentageChange);
    spec.setOffset("median3w");
    spec.setPattern("up");
    detector.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    DetectionResult detectionResult = detector.runDetection(new Interval(1814400000L, 2419200000L), "thirdeye:metric:1");
    List<MergedAnomalyResultDTO> anomalies = detectionResult.getAnomalies();
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 2005200000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 2008800000L);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 2134800000L);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 2138400000L);
    Assert.assertEquals(anomalies.get(2).getStartTime(), 2152800000L);
    Assert.assertEquals(anomalies.get(2).getEndTime(), 2156400000L);
    Assert.assertEquals(anomalies.get(3).getStartTime(), 2322000000L);
    Assert.assertEquals(anomalies.get(3).getEndTime(), 2325600000L);
    TimeSeries ts = detectionResult.getTimeseries();
    checkPercentageUpperBounds(ts, percentageChange);
    Assert.assertEquals(ts.getPredictedLowerBound(), DoubleSeries.zeros(ts.size()));
  }

  @Test
  public void testThreeWeekMedianChangeDown() {
    PercentageChangeRuleDetector detector = new PercentageChangeRuleDetector();
    PercentageChangeRuleDetectorSpec spec = new PercentageChangeRuleDetectorSpec();
    double percentageChange = 0.3;
    spec.setPercentageChange(percentageChange);
    spec.setOffset("median3w");
    spec.setPattern("down");
    detector.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    DetectionResult result = detector.runDetection(new Interval(1814400000L, 2419200000L), "thirdeye:metric:1");
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 2181600000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 2185200000L);
    TimeSeries ts = result.getTimeseries();
    checkPercentageLowerBounds(ts, percentageChange);
    Assert.assertEquals(ts.getPredictedUpperBound(), DoubleSeries.fillValues(ts.size(), Double.POSITIVE_INFINITY));
  }

  @Test
  public void testThreeWeekMedianChangeUporDown() {
    PercentageChangeRuleDetector detector = new PercentageChangeRuleDetector();
    PercentageChangeRuleDetectorSpec spec = new PercentageChangeRuleDetectorSpec();
    double percentageChange = 0.3;
    spec.setPercentageChange(percentageChange);
    spec.setOffset("median3w");
    spec.setPattern("up_or_down");
    detector.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    DetectionResult result = detector.runDetection(new Interval(1814400000L, 2419200000L), "thirdeye:metric:1");
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
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
    checkPercentageUpperBounds(result.getTimeseries(), percentageChange);
    checkPercentageLowerBounds(result.getTimeseries(), percentageChange);
  }

  @Test
  public void testMonthlyDetectionPercentage() throws DetectorException {
    AnomalyDetector percentageRule = new PercentageChangeRuleDetector();
    PercentageChangeRuleDetectorSpec spec = new PercentageChangeRuleDetectorSpec();
    spec.setOffset("mo1m");
    spec.setTimezone("UTC");
    spec.setPercentageChange(0.4);
    spec.setMonitoringGranularity("1_MONTHS");
    percentageRule.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    List<MergedAnomalyResultDTO> anomalies = percentageRule.runDetection(new Interval(1546214400000L, 1551312000000L), "thirdeye:metric:1").getAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1548892800000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 1551312000000L);
  }

  @Test
  public void testZeroDivide() throws DetectorException {
    AnomalyDetector percentageRule = new PercentageChangeRuleDetector();
    PercentageChangeRuleDetectorSpec spec = new PercentageChangeRuleDetectorSpec();
    spec.setOffset("wo1w");
    spec.setPercentageChange(0.1);
    percentageRule.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    List<MergedAnomalyResultDTO> anomalies = percentageRule.runDetection(new Interval(1551398400000L, 1551571200000L), "thirdeye:metric:1").getAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1551484800000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 1551488400000L);
  }

  @Test
  public void testWeeklyDetection() throws DetectorException {
    AnomalyDetector<PercentageChangeRuleDetectorSpec> percentageRule = new PercentageChangeRuleDetector();
    PercentageChangeRuleDetectorSpec spec = new PercentageChangeRuleDetectorSpec();
    spec.setWeekStart(DayOfWeek.TUESDAY.toString());
    spec.setPercentageChange(0.01);
    spec.setOffset("wo1w");
    spec.setMonitoringGranularity("1_WEEKS");
    percentageRule.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    List<MergedAnomalyResultDTO> anomalies = percentageRule.runDetection(new Interval(1562025600000L, 1562630400000L, DateTimeZone.UTC), "thirdeye:metric:1").getAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1561420800000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 1562025600000L);
  }

  private void checkPercentageUpperBounds(TimeSeries ts, double percentageChange) {
    for (int i = 0; i < ts.getDataFrame().size(); i++) {
      Assert.assertEquals(ts.getPredictedUpperBound().get(i), ts.getPredictedBaseline().get(i) * (1 + percentageChange));
    }
  }

  private void checkPercentageLowerBounds(TimeSeries ts, double percentageChange) {
    for (int i = 0; i < ts.getDataFrame().size(); i++) {
      Assert.assertEquals(ts.getPredictedLowerBound().get(i), ts.getPredictedBaseline().get(i) * (1 - percentageChange));
    }
  }
}
