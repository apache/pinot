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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.MockPipeline;
import org.apache.pinot.thirdeye.detection.MockPipelineLoader;
import org.apache.pinot.thirdeye.detection.MockPipelineOutput;
import org.apache.pinot.thirdeye.detection.algorithm.MergeWrapper;
import org.apache.pinot.thirdeye.detection.components.MockBaselineProvider;
import org.apache.pinot.thirdeye.detection.spec.MockBaselineProviderSpec;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.DetectionTestUtils.*;


public class BaselineFillingMergeWrapperTest {
  private static final String PROP_BASELINE_PROVIDER = "baselineValueProvider";

  private DetectionConfigDTO config;
  private MergeWrapper wrapper;
  private Map<String, Object> properties;
  private List<Map<String, Object>> nestedProperties;
  private List<MockPipeline> runs;

  private static final Long PROP_ID_VALUE = 1000L;
  private static final String PROP_NAME_VALUE = "myName";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_PROPERTIES = "properties";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_MAX_GAP = "maxGap";

  @BeforeMethod
  public void beforeMethod() {
    this.runs = new ArrayList<>();

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_PROPERTIES, Collections.singletonMap("key", "value"));

    Map<String, Object> nestedPropertiesOne = new HashMap<>();
    nestedPropertiesOne.put(PROP_CLASS_NAME, "none");
    nestedPropertiesOne.put(PROP_METRIC_URN, "thirdeye:metric:1");

    this.nestedProperties = new ArrayList<>();
    this.nestedProperties.add(nestedPropertiesOne);

    this.properties.put(PROP_NESTED, this.nestedProperties);

    this.config = new DetectionConfigDTO();
    this.config.setId(PROP_ID_VALUE);
    this.config.setName(PROP_NAME_VALUE);
    this.config.setProperties(this.properties);
  }

  @Test
  public void testMergerCurrentAndBaselineLoading() throws Exception {
    // mock anomaly
    MergedAnomalyResultDTO anomaly = makeAnomaly(3000, 3600);
    Map<String, String> anomalyProperties = new HashMap<>();
    anomalyProperties.put("detectorComponentName", "testDetector");
    anomaly.setProperties(anomalyProperties);
    anomaly.setMetricUrn("thirdeye:metric:1");

    // mock time series
    Map<MetricSlice, DataFrame> timeseries = new HashMap<>();
    timeseries.put(MetricSlice.from(1, 3000, 3600),
        DataFrame.builder(DataFrame.COL_TIME + ":LONG", DataFrame.COL_VALUE + ":DOUBLE").append(3000, 100).build());

    // mock metric
    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setId(1L);
    metric.setDefaultAggFunction(MetricAggFunction.SUM);
    DataProvider provider = new MockDataProvider().setLoader(new MockPipelineLoader(this.runs,
        Collections.singletonList(new MockPipelineOutput(Collections.singletonList(anomaly), -1L))))
        .setAnomalies(Collections.emptyList())
        .setMetrics(Collections.singletonList(metric))
        .setTimeseries(timeseries)
        .setAggregates(ImmutableMap.of(MetricSlice.from(1, 3000, 3600),
            DataFrame.builder(DataFrame.COL_TIME + ":LONG", DataFrame.COL_VALUE + ":DOUBLE").append(3000, 100).build()));

    // set up detection config properties
    this.config.getProperties().put(PROP_MAX_GAP, 100);
    this.config.getProperties().put(PROP_BASELINE_PROVIDER, "$baseline");
    this.config.getProperties().put("detector", "$testDetector");

    // initialize the baseline provider
    BaselineProvider baselineProvider = new MockBaselineProvider();
    MockBaselineProviderSpec spec = new MockBaselineProviderSpec();
    spec.setBaselineAggregates(ImmutableMap.of(MetricSlice.from(1, 3000, 3600), 100.0));
    spec.setBaselineTimeseries(ImmutableMap.of(MetricSlice.from(1, 3000, 3600), TimeSeries.fromDataFrame(
        DataFrame.builder(
            DataFrame.COL_TIME + ":LONG", DataFrame.COL_VALUE + ":DOUBLE" , DataFrame.COL_UPPER_BOUND + ":DOUBLE", DataFrame.COL_LOWER_BOUND + ":DOUBLE")
            .append(3000, 100, 200, 50).build())));
    InputDataFetcher dataFetcher = new DefaultInputDataFetcher(provider, this.config.getId());
    baselineProvider.init(spec, dataFetcher);
    this.config.setComponents(ImmutableMap.of("baseline", baselineProvider));

    // run baseline filling merge wrapper
    this.wrapper = new BaselineFillingMergeWrapper(provider, this.config, 2900, 3600);
    DetectionPipelineResult output = this.wrapper.run();

    List<MergedAnomalyResultDTO> anomalyResults = output.getAnomalies();
    Assert.assertEquals(anomalyResults.size(), 1);
    Assert.assertTrue(anomalyResults.contains(anomaly));
    Assert.assertEquals(anomalyResults.get(0).getAvgBaselineVal(), 100.0);
    Assert.assertEquals(anomalyResults.get(0).getAvgBaselineVal(), 100.0);
    Assert.assertEquals(anomalyResults.get(0).getAvgCurrentVal(), 100.0);
    Assert.assertEquals(anomalyResults.get(0).getProperties().get("detectorComponentName"), "testDetector");
    Assert.assertEquals(anomalyResults.get(0).getProperties().get("baselineProviderComponentName"), "baseline");

    TimeSeries ts = baselineProvider.computePredictedTimeSeries(MetricSlice.from(1, 3000, 3600));
    Assert.assertEquals(ts.getPredictedBaseline().get(0), 100.0);
    Assert.assertEquals(ts.getPredictedUpperBound().get(0), 200.0);
    Assert.assertEquals(ts.getPredictedLowerBound().get(0), 50.0);
  }
}
