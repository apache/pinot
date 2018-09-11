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

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.MockPipeline;
import com.linkedin.thirdeye.detection.MockPipelineLoader;
import com.linkedin.thirdeye.detection.MockPipelineOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class LegacyDimensionWrapperTest {

  // exploration
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_DIMENSIONS = "dimensions";
  private static final String PROP_LOOKBACK = "lookback";
  private static final String PROP_SPECS = "specs";
  private static final String PROP_ANOMALY_FUNCTION_CLASS = "anomalyFunctionClassName";

  // prototyping
  private static final String PROP_NESTED = "nested";
  private static final String PROP_NESTED_METRIC_URNS = "nestedMetricUrns";
  private static final String PROP_NESTED_METRIC_URN_KEY = "nestedMetricUrnKey";
  private static final String PROP_CLASS_NAME = "className";

  // values
  private static final Long PROP_ID_VALUE = 1000L;
  private static final String PROP_NAME_VALUE = "myName";
  private static final String PROP_CLASS_NAME_VALUE = "MyClassName";
  private static final Collection<String> PROP_NESTED_METRIC_URN_VALUES = Collections.singleton("thirdeye:metric:2");
  private static final String PROP_NESTED_METRIC_URN_KEY_VALUE = "myMetricUrn";
  private static final String PROP_ANOMALY_FUNCTION_CLASS_NAME_VALUE =
      "com.linkedin.thirdeye.anomalydetection.function.MinMaxThresholdFunction";
  private Map<String, Object> specs;

  private DataProvider provider;

  private List<MockPipeline> runs;
  private List<MockPipelineOutput> outputs;

  private DetectionConfigDTO config;
  private Map<String, Object> properties;
  private Map<String, Object> nestedProperties;
  private Map<MetricSlice, DataFrame> aggregates;
  private LegacyDimensionWrapper dimensionWrapper;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    this.aggregates = new HashMap<>();
    this.aggregates.put(MetricSlice.from(1, 10, 15),
        new DataFrame().addSeries("a", StringSeries.buildFrom("1", "1", "1", "1", "1", "2", "2", "2", "2", "2"))
            .addSeries("b", StringSeries.buildFrom("1", "2", "1", "2", "3", "1", "2", "1", "2", "3"))
            .addSeries(COL_VALUE, DoubleSeries.buildFrom(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));

    this.runs = new ArrayList<>();

    this.outputs = new ArrayList<>();

    this.provider = new MockDataProvider().setAggregates(this.aggregates)
        .setLoader(new MockPipelineLoader(this.runs, this.outputs));

    this.nestedProperties = new HashMap<>();
    this.nestedProperties.put(PROP_CLASS_NAME, PROP_CLASS_NAME_VALUE);
    this.nestedProperties.put("key", "value");

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_DIMENSIONS, Arrays.asList("b"));
    this.properties.put(PROP_NESTED_METRIC_URN_KEY, PROP_NESTED_METRIC_URN_KEY_VALUE);
    this.properties.put(PROP_NESTED_METRIC_URNS, PROP_NESTED_METRIC_URN_VALUES);
    this.properties.put(PROP_NESTED, Collections.singletonList(this.nestedProperties));
    this.properties.put(PROP_LOOKBACK, 0);
    this.specs = new HashMap<>();
    specs.put("properties", "min=2;max=2");
    specs.put("exploreDimensions", "a");
    this.properties.put(PROP_SPECS, specs);
    this.properties.put(PROP_ANOMALY_FUNCTION_CLASS, PROP_ANOMALY_FUNCTION_CLASS_NAME_VALUE);

    this.config = new DetectionConfigDTO();
    this.config.setId(PROP_ID_VALUE);
    this.config.setName(PROP_NAME_VALUE);
    this.config.setProperties(this.properties);

    this.dimensionWrapper = new LegacyDimensionWrapper(this.provider, this.config, 10, 15);
  }

  @Test
  public void testRun() throws Exception {
    this.dimensionWrapper.run();

    Assert.assertEquals(this.runs.size(), 6);
    Assert.assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:2:a%3D1:b%3D1"));
    Assert.assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:2:a%3D1:b%3D2"));
    Assert.assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:2:a%3D1:b%3D3"));
    Assert.assertEquals(this.runs.get(3), makePipeline("thirdeye:metric:2:a%3D2:b%3D1"));
    Assert.assertEquals(this.runs.get(4), makePipeline("thirdeye:metric:2:a%3D2:b%3D2"));
    Assert.assertEquals(this.runs.get(5), makePipeline("thirdeye:metric:2:a%3D2:b%3D3"));
  }

  private DetectionConfigDTO makeConfig(String metricUrn) {
    Map<String, Object> properties = new HashMap<>(this.nestedProperties);
    properties.put(PROP_NESTED_METRIC_URN_KEY_VALUE, metricUrn);
    properties.put(PROP_SPECS, specs);
    properties.put(PROP_ANOMALY_FUNCTION_CLASS, PROP_ANOMALY_FUNCTION_CLASS_NAME_VALUE);
    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setId(this.config.getId());
    config.setName(this.config.getName());
    config.setProperties(properties);

    return config;
  }

  private MockPipeline makePipeline(String metricUrn) {
    return makePipeline(metricUrn, 10, 15);
  }

  private MockPipeline makePipeline(String metricUrn, long startTime, long endTime) {
    return new MockPipeline(this.provider, makeConfig(metricUrn), startTime, endTime,
        new MockPipelineOutput(Collections.<MergedAnomalyResultDTO>emptyList(), -1));
  }
}
