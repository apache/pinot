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

package com.linkedin.thirdeye.detection.wrapper;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DefaultInputDataFetcher;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.InputDataFetcher;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.MockPipeline;
import com.linkedin.thirdeye.detection.MockPipelineLoader;
import com.linkedin.thirdeye.detection.MockPipelineOutput;
import com.linkedin.thirdeye.detection.algorithm.MergeWrapper;
import com.linkedin.thirdeye.detection.components.MockBaselineProvider;
import com.linkedin.thirdeye.detection.spec.MockBaselineProviderSpec;
import com.linkedin.thirdeye.detection.spi.components.BaselineProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;
import static com.linkedin.thirdeye.detection.DetectionTestUtils.*;


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

    Map<String, Object> nestedPropertiesTwo = new HashMap<>();
    nestedPropertiesTwo.put(PROP_CLASS_NAME, "none");
    nestedPropertiesTwo.put(PROP_METRIC_URN, "thirdeye:metric:2");

    this.nestedProperties = new ArrayList<>();
    this.nestedProperties.add(nestedPropertiesOne);
    this.nestedProperties.add(nestedPropertiesTwo);

    this.properties.put(PROP_NESTED, this.nestedProperties);

    this.config = new DetectionConfigDTO();
    this.config.setId(PROP_ID_VALUE);
    this.config.setName(PROP_NAME_VALUE);
    this.config.setProperties(this.properties);
  }

  @Test
  public void testMergerCurrentAndBaselineLoading() throws Exception {
    MergedAnomalyResultDTO anomaly = makeAnomaly(3000, 3600);
    anomaly.setMetricUrn("thirdeye:metric:1");

    Map<MetricSlice, DataFrame> aggregates = new HashMap<>();
    aggregates.put(MetricSlice.from(1, 3000, 3600), DataFrame.builder(COL_TIME + ":LONG", COL_VALUE + ":DOUBLE").append(-1, 100).build());

    DataProvider
        provider = new MockDataProvider().setLoader(new MockPipelineLoader(this.runs, Collections.<MockPipelineOutput>emptyList())).setAnomalies(Collections.singletonList(anomaly)).setAggregates(aggregates);

    this.config.getProperties().put(PROP_MAX_GAP, 100);
    this.config.getProperties().put(PROP_BASELINE_PROVIDER, "$baseline");
    BaselineProvider baselineProvider = new MockBaselineProvider();
    MockBaselineProviderSpec spec = new MockBaselineProviderSpec();
    spec.setAggregates(ImmutableMap.of(MetricSlice.from(1, 3000, 3600), 100.0));
    InputDataFetcher dataFetcher = new DefaultInputDataFetcher(provider, this.config.getId());
    baselineProvider.init(spec, dataFetcher);
    this.config.setComponents(ImmutableMap.of("baseline", baselineProvider));
    this.wrapper = new BaselineFillingMergeWrapper(provider, this.config, 2900, 3600);
    DetectionPipelineResult output = this.wrapper.run();

    List<MergedAnomalyResultDTO> anomalyResults = output.getAnomalies();
    Assert.assertEquals(anomalyResults.size(), 1);
    Assert.assertTrue(anomalyResults.contains(anomaly));
    Assert.assertEquals(anomalyResults.get(0).getAvgBaselineVal(), 100.0);
    Assert.assertEquals(anomalyResults.get(0).getAvgCurrentVal(), 100.0);
  }

}