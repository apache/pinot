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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.Math;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.Pattern;
import org.apache.pinot.thirdeye.detection.algorithm.AlgorithmUtils;
import org.apache.pinot.thirdeye.detection.spec.MeanVarianceRuleDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;

public class MeanVarianceRuleDetectorTest {

  private DataProvider provider;
  private DataFrame data;
  private Multimap<String, String> filters =  TreeMultimap.create();

  @BeforeMethod
  public void setUp() throws Exception {
    try (Reader dataReader = new InputStreamReader(AlgorithmUtils.class.getResourceAsStream("timeseries-2y.csv"))) {
      this.data = DataFrame.fromCsv(dataReader);
      this.data.setIndex(DataFrame.COL_TIME);
    }

    MetricConfigDTO dailyMetricConfig = new MetricConfigDTO();
    dailyMetricConfig.setId(1L);
    dailyMetricConfig.setName("thirdeye-test-daily");
    dailyMetricConfig.setDataset("thirdeye-test-dataset-daily");

    DatasetConfigDTO dailyDatasetConfig = new DatasetConfigDTO();
    dailyDatasetConfig.setTimeUnit(TimeUnit.DAYS);
    dailyDatasetConfig.setDataset("thirdeye-test-dataset-daily");
    dailyDatasetConfig.setTimeDuration(1);

    Map<MetricSlice, DataFrame> timeseries = new HashMap<>();
    timeseries.put(MetricSlice.from(1L, 1499126400000L, 1562630400000L, filters, TimeGranularity.fromString("7_DAYS")), this.data);

    this.provider = new MockDataProvider()
        .setTimeseries(timeseries)
        .setMetrics(Collections.singletonList(dailyMetricConfig))
        .setDatasets(Collections.singletonList(dailyDatasetConfig));
  }

  @Test
  public void testComputePredictedTimeSeriesDaily() {
    MeanVarianceRuleDetector detector = new MeanVarianceRuleDetector();
    MeanVarianceRuleDetectorSpec spec = new MeanVarianceRuleDetectorSpec();
    spec.setMonitoringGranularity("7_DAYS");
    spec.setLookback(52);
    spec.setSensitivity(5);
    detector.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    Interval window = new Interval(1530576000000L, 1562630400000L);
    String metricUrn = "thirdeye:metric:1";
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    MetricSlice slice = MetricSlice.from(me.getId(), window.getStartMillis(), window.getEndMillis(), me.getFilters(), TimeGranularity
        .fromString(spec.getMonitoringGranularity()));
    TimeSeries timeSeries = detector.computePredictedTimeSeries(slice);
    Assert.assertEquals(timeSeries.getPredictedBaseline().size(), 52);
    // prediction assertion. Expected value calculated offline using Python.
    Assert.assertEquals(Math.round(timeSeries.getPredictedBaseline().getDouble(0) * 1000.0) / 1000.0,174431.234);
    Assert.assertEquals(Math.round(timeSeries.getPredictedBaseline().getDouble(1) * 1000.0) / 1000.0,147184.878);
    Assert.assertEquals(Math.round(timeSeries.getPredictedBaseline().getDouble(2) * 1000.0) / 1000.0,153660.665);
  }

  @Test
  public void testWeekOverWeekLookbackChange() {
    MeanVarianceRuleDetector detector = new MeanVarianceRuleDetector();
    MeanVarianceRuleDetectorSpec spec = new MeanVarianceRuleDetectorSpec();
    spec.setMonitoringGranularity("7_DAYS");
    spec.setLookback(52);
    spec.setSensitivity(5);
    detector.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    DetectionResult detectionResult = detector.runDetection(new Interval(1530576000000L, 1562630400000L), "thirdeye:metric:1");
    List<MergedAnomalyResultDTO> anomalies = detectionResult.getAnomalies();
    Assert.assertEquals(anomalies.size(), 6);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1530576000000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 1531180800000L);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 1540252800000L);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 1541462400000L);
    Assert.assertEquals(anomalies.get(2).getStartTime(), 1545696000000L);
    Assert.assertEquals(anomalies.get(2).getEndTime(), 1546300800000L);
    Assert.assertEquals(anomalies.get(3).getStartTime(), 1546905600000L);
    Assert.assertEquals(anomalies.get(3).getEndTime(), 1547510400000L);
    Assert.assertEquals(anomalies.get(4).getStartTime(), 1553558400000L);
    Assert.assertEquals(anomalies.get(4).getEndTime(), 1554768000000L);
    Assert.assertEquals(anomalies.get(5).getStartTime(), 1555372800000L);
    Assert.assertEquals(anomalies.get(5).getEndTime(), 1557187200000L);
  }

  @Test
  public void testWeekOverWeekLookbackChangeDown() {
    MeanVarianceRuleDetector detector = new MeanVarianceRuleDetector();
    MeanVarianceRuleDetectorSpec spec = new MeanVarianceRuleDetectorSpec();
    spec.setMonitoringGranularity("7_DAYS");
    spec.setLookback(52);
    spec.setSensitivity(5);
    spec.setPattern(Pattern.DOWN);
    detector.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    DetectionResult result = detector.runDetection(new Interval(1530576000000L, 1562630400000L), "thirdeye:metric:1");
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1530576000000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 1531180800000L);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 1540857600000L);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 1541462400000L);
    Assert.assertEquals(anomalies.get(2).getStartTime(), 1545696000000L);
    Assert.assertEquals(anomalies.get(2).getEndTime(), 1546300800000L);
    Assert.assertEquals(anomalies.get(3).getStartTime(), 1554163200000L);
    Assert.assertEquals(anomalies.get(3).getEndTime(), 1554768000000L);
  }

  @Test
  public void testWeekOverWeekLookbackChangeUp() {
    MeanVarianceRuleDetector detector = new MeanVarianceRuleDetector();
    MeanVarianceRuleDetectorSpec spec = new MeanVarianceRuleDetectorSpec();
    spec.setMonitoringGranularity("7_DAYS");
    spec.setLookback(52);
    spec.setSensitivity(5);
    spec.setPattern(Pattern.UP);
    detector.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    DetectionResult result = detector.runDetection(new Interval(1530576000000L, 1562630400000L), "thirdeye:metric:1");
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 1540252800000L);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 1540857600000L);
    Assert.assertEquals(anomalies.get(1).getStartTime(),1546905600000L);
    Assert.assertEquals(anomalies.get(1).getEndTime(),1547510400000L);
    Assert.assertEquals(anomalies.get(2).getStartTime(),1553558400000L);
    Assert.assertEquals(anomalies.get(2).getEndTime(),1554163200000L);
    Assert.assertEquals(anomalies.get(3).getStartTime(),1555372800000L);
    Assert.assertEquals(anomalies.get(3).getEndTime(),1557187200000L);
  }

}
