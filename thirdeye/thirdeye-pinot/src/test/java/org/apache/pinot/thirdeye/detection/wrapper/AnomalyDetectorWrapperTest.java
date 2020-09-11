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

package org.apache.pinot.thirdeye.detection.wrapper;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleDetector;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator.*;


public class AnomalyDetectorWrapperTest {
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_MOVING_WINDOW_DETECTION = "isMovingWindowDetection";
  private static final String PROP_DETECTOR = "detector";
  private static final String PROP_TIMEZONE = "timezone";

  private MockDataProvider provider;
  private Map<String, Object> properties;
  private DetectionConfigDTO config;

  @BeforeMethod
  public void setUp() {
    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_DETECTOR, "$testDetector");
    this.properties.put(PROP_SUB_ENTITY_NAME, "test_detector");
    this.config = new DetectionConfigDTO();
    this.config.setComponents(ImmutableMap.of("testDetector", new ThresholdRuleDetector()));
    this.config.setProperties(properties);
    this.config.setId(-1L);

    this.provider = new MockDataProvider();
    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setId(1L);
    metric.setDataset("test");
    this.provider.setMetrics(Collections.singletonList(metric));
    DatasetConfigDTO dataset = new DatasetConfigDTO();
    dataset.setDataset("test");
    dataset.setTimeUnit(TimeUnit.DAYS);
    dataset.setTimeDuration(1);
    this.provider.setDatasets(Collections.singletonList(dataset));
    this.provider.setTimeseries(ImmutableMap.of(
        MetricSlice.from(1L, 1546646400000L, 1546732800000L),
        new DataFrame().addSeries(DataFrame.COL_VALUE, 500, 1000).addSeries(DataFrame.COL_TIME, 1546646400000L, 1546732800000L),
        MetricSlice.from(1L, 1546819200000L, 1546905600000L),
        DataFrame.builder(DataFrame.COL_TIME, DataFrame.COL_VALUE).build(),
        MetricSlice.from(1L, 1546300800000L, 1546560000000L),
        new DataFrame().addSeries(DataFrame.COL_VALUE, 500, 1000).addSeries(DataFrame.COL_TIME, 1546300800000L, 1546387200000L),
        MetricSlice.from(1L, 1540147725000L - TimeUnit.DAYS.toMillis(90), 1540493325000L, HashMultimap.create(),
            new TimeGranularity(1, TimeUnit.DAYS)),
        new DataFrame().addSeries(DataFrame.COL_VALUE, 500, 1000).addSeries(DataFrame.COL_TIME, 1546646400000L, 1546732800000L),
        MetricSlice.from(1L, 1540080000000L - TimeUnit.DAYS.toMillis(90), 1540425600000L, HashMultimap.create(),
            new TimeGranularity(1, TimeUnit.DAYS)),
        new DataFrame().addSeries(DataFrame.COL_VALUE, 500, 1000).addSeries(DataFrame.COL_TIME, 1546646400000L, 1546732800000L)));
  }

  @Test
  public void testMonitoringWindow() {
    AnomalyDetectorWrapper detectionPipeline =
        new AnomalyDetectorWrapper(this.provider, this.config, 1538418436000L, 1540837636000L);
    List<Interval> monitoringWindows = detectionPipeline.getMonitoringWindows();
    for (Interval window : monitoringWindows) {
      Assert.assertEquals(window, new Interval(1538418436000L, 1540837636000L, DateTimeZone.forID(TimeSpec.DEFAULT_TIMEZONE)));
    }
  }

  @Test
  public void testMovingMonitoringWindow() {
    this.properties.put(PROP_MOVING_WINDOW_DETECTION, true);
    this.properties.put(PROP_TIMEZONE, TimeSpec.DEFAULT_TIMEZONE);
    AnomalyDetectorWrapper detectionPipeline =
        new AnomalyDetectorWrapper(this.provider, this.config, 1540147725000L, 1540493325000L);
    List<Interval> monitoringWindows = detectionPipeline.getMonitoringWindows();
    DateTimeZone timeZone = DateTimeZone.forID(TimeSpec.DEFAULT_TIMEZONE);
    Assert.assertEquals(monitoringWindows,
        Arrays.asList(new Interval(1540080000000L, 1540166400000L, timeZone), new Interval(1540166400000L, 1540252800000L, timeZone),
            new Interval(1540252800000L, 1540339200000L, timeZone), new Interval(1540339200000L, 1540425600000L, timeZone)));
  }

  @Test
  public void testMovingMonitoringWindowBoundary() {
    this.properties.put(PROP_MOVING_WINDOW_DETECTION, true);
    this.properties.put(PROP_TIMEZONE, TimeSpec.DEFAULT_TIMEZONE);
    AnomalyDetectorWrapper detectionPipeline =
        new AnomalyDetectorWrapper(this.provider, this.config, 1540080000000L, 1540425600000L);
    List<Interval> monitoringWindows = detectionPipeline.getMonitoringWindows();
    DateTimeZone timeZone = DateTimeZone.forID(TimeSpec.DEFAULT_TIMEZONE);
    Assert.assertEquals(monitoringWindows,
        Arrays.asList(new Interval(1540080000000L, 1540166400000L, timeZone), new Interval(1540166400000L, 1540252800000L, timeZone),
            new Interval(1540252800000L, 1540339200000L, timeZone), new Interval(1540339200000L, 1540425600000L, timeZone)));
  }

  @Test
  public void testMovingMonitoringWindowDayLightSaving() {
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    this.properties.put(PROP_MOVING_WINDOW_DETECTION, true);
    this.properties.put(PROP_TIMEZONE, timeZone.toString());
    AnomalyDetectorWrapper detectionPipeline =
        new AnomalyDetectorWrapper(this.provider, this.config, 1552118400000L, 1552287600000L);
    List<Interval> monitoringWindows = detectionPipeline.getMonitoringWindows();
    Assert.assertEquals(monitoringWindows,
        Arrays.asList(new Interval(1552118400000L, 1552204800000L, timeZone), new Interval(1552204800000L, 1552287600000L, timeZone)));
  }

  @Test
  public void testMovingMonitoringWindowDayLightSaving2() {
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    this.properties.put(PROP_MOVING_WINDOW_DETECTION, true);
    this.properties.put(PROP_TIMEZONE, timeZone.toString());
    AnomalyDetectorWrapper detectionPipeline =
        new AnomalyDetectorWrapper(this.provider, this.config, 1541228400000L, 1541404800000L);
    List<Interval> monitoringWindows = detectionPipeline.getMonitoringWindows();
    Assert.assertEquals(monitoringWindows,
        Arrays.asList(new Interval(1541228400000L, 1541314800000L, timeZone), new Interval(1541314800000L, 1541404800000L, timeZone)));
  }


  @Test
  public void testGetLastTimestampWithEstimate() {
    AnomalyDetectorWrapper detectionPipeline =
        new AnomalyDetectorWrapper(this.provider, this.config, 1546300800000L, 1546560000000L);
    Assert.assertEquals(detectionPipeline.getLastTimeStamp(), 1546473600000L);
  }

  @Test
  public void testGetLastTimestampTruncate() {
    AnomalyDetectorWrapper detectionPipeline =
        new AnomalyDetectorWrapper(this.provider, this.config, 1546646400000L, 1546732800000L);
    Assert.assertEquals(detectionPipeline.getLastTimeStamp(), 1546732800000L);
  }

  @Test
  public void testGetLastTimestampNoData() {
    AnomalyDetectorWrapper detectionPipeline =
        new AnomalyDetectorWrapper(this.provider, this.config, 1546819200000L, 1546905600000L);
    Assert.assertEquals(detectionPipeline.getLastTimeStamp(), -1);
  }

  @Test
  public void testConsolidateTimeSeries() {
    TimeSeries ts1 =
        new TimeSeries(LongSeries.buildFrom(1L, 2L, 3L, 4L, 5L), DoubleSeries.buildFrom(1.0, 20.0, 30.0, 40.0, 50.0),
            DoubleSeries.buildFrom(1.0, 2.0, 3.0, 4.0, 5.0), DoubleSeries.buildFrom(1.0, 2.0, 3.0, 4.0, 5.0),
            DoubleSeries.buildFrom(1.0, 2.0, 3.0, 4.0, 5.0));
    TimeSeries ts2 =
        new TimeSeries(LongSeries.buildFrom(2L, 3L, 4L, 5L, 6L), DoubleSeries.buildFrom(1.0, 2.0, 3.0, 4.0, 5.0),
            DoubleSeries.buildFrom(2.0, 3.0, 4.0, 5.0, 6.0), DoubleSeries.buildFrom(1.0, 2.0, 3.0, 4.0, 5.0),
            DoubleSeries.buildFrom(1.0, 2.0, 3.0, 4.0, 5.0));
    TimeSeries result = AnomalyDetectorWrapper.consolidateTimeSeries(ts1, ts2);
    Assert.assertEquals(result.getTime(), LongSeries.buildFrom(1L, 2L, 3L, 4L, 5L, 6L));
    Assert.assertEquals(result.getCurrent(), DoubleSeries.buildFrom(1.0, 2.0, 3.0, 4.0, 5.0, 6.0));
    Assert.assertEquals(result.getPredictedBaseline(), DoubleSeries.buildFrom(1.0, 1.0, 2.0, 3.0, 4.0, 5.0));
    Assert.assertEquals(result.getPredictedUpperBound(), DoubleSeries.buildFrom(1.0, 1.0, 2.0, 3.0, 4.0, 5.0));
    Assert.assertEquals(result.getPredictedLowerBound(), DoubleSeries.buildFrom(1.0, 1.0, 2.0, 3.0, 4.0, 5.0));
  }

  @Test
  public void testConsolidateTimeSeriesWithNull() {
    TimeSeries ts1 = TimeSeries.empty();
    TimeSeries ts2 = new TimeSeries(LongSeries.buildFrom(1L, 2L, 3L, 4L, 5L), DoubleSeries.buildFrom(1.0, 20.0, 30.0, 40.0, 50.0));
    TimeSeries ts3 = new TimeSeries(LongSeries.buildFrom(2L, 3L, 4L, 5L, 6L), DoubleSeries.buildFrom(2.0 ,3.0, 4.0, Double.NaN, 6.0));
    TimeSeries result1 = AnomalyDetectorWrapper.consolidateTimeSeries(ts1, ts2);
    TimeSeries result = AnomalyDetectorWrapper.consolidateTimeSeries(result1, ts3);
    Assert.assertEquals(result.getTime(), LongSeries.buildFrom(1L, 2L, 3L, 4L, 5L, 6L));
    Assert.assertEquals(result.getPredictedBaseline(), DoubleSeries.buildFrom(1.0, 2.0, 3.0, 4.0, Double.NaN, 6.0));
  }

}
