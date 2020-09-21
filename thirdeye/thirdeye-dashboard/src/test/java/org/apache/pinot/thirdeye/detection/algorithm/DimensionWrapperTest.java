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

package org.apache.pinot.thirdeye.detection.algorithm;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.StringSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.MockPipeline;
import org.apache.pinot.thirdeye.detection.MockPipelineLoader;
import org.apache.pinot.thirdeye.detection.MockPipelineOutput;
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

public class DimensionWrapperTest {
  // exploration
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_DIMENSIONS = "dimensions";
  private static final String PROP_MIN_VALUE = "minValue";
  private static final String PROP_MIN_CONTRIBUTION = "minContribution";
  private static final String PROP_K = "k";
  private static final String PROP_LOOKBACK = "lookback";

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

  private DataProvider provider;
  private DimensionWrapper wrapper;

  private List<MockPipeline> runs;
  private List<MockPipelineOutput> outputs;

  private DetectionConfigDTO config;
  private Map<String, Object> properties;
  private Map<String, Object> nestedProperties;
  private Map<MetricSlice, DataFrame> aggregates;

  private MetricConfigDTO createTestMetricConfig(long id) {
    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setDataset("TEST");
    metric.setId(id);
    metric.setDefaultAggFunction(MetricAggFunction.SUM);
    return metric;
  }

  @BeforeMethod
  public void beforeMethod() {
    this.aggregates = new HashMap<>();
    this.aggregates.put(MetricSlice.from(2, 10, 15),
        new DataFrame()
            .addSeries("a", StringSeries.buildFrom("1", "1", "1", "1", "1", "2", "2", "2", "2", "2"))
            .addSeries("b", StringSeries.buildFrom("1", "2", "1", "2", "3", "1", "2", "1", "2", "3"))
            .addSeries(DataFrame.COL_VALUE, DoubleSeries.buildFrom(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));

    this.runs = new ArrayList<>();

    this.outputs = new ArrayList<>();

    DatasetConfigDTO dataset = new DatasetConfigDTO();
    dataset.setDataset("TEST");
    dataset.setNonAdditiveBucketSize(5);
    dataset.setNonAdditiveBucketUnit(TimeUnit.MILLISECONDS);
    MetricConfigDTO metric1 = createTestMetricConfig(2L);
    MetricConfigDTO metric2 = createTestMetricConfig(10L);
    MetricConfigDTO metric3 = createTestMetricConfig(11L);
    MetricConfigDTO metric4 = createTestMetricConfig(12L);
    this.provider = new MockDataProvider()
        .setAggregates(this.aggregates)
        .setMetrics(Arrays.asList(metric1, metric2, metric3, metric4))
        .setDatasets(Collections.singletonList(dataset))
        .setAnomalies(Collections.emptyList())
        .setLoader(new MockPipelineLoader(this.runs, this.outputs));

    this.nestedProperties = new HashMap<>();
    this.nestedProperties.put(PROP_CLASS_NAME, PROP_CLASS_NAME_VALUE);
    this.nestedProperties.put("key", "value");

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:2");
    this.properties.put(PROP_DIMENSIONS, Arrays.asList("a", "b"));
    this.properties.put(PROP_NESTED_METRIC_URN_KEY, PROP_NESTED_METRIC_URN_KEY_VALUE);
    this.properties.put(PROP_NESTED_METRIC_URNS, PROP_NESTED_METRIC_URN_VALUES);
    this.properties.put(PROP_NESTED, Collections.singletonList(this.nestedProperties));
    this.properties.put(PROP_LOOKBACK, 0);

    this.config = new DetectionConfigDTO();
    this.config.setId(PROP_ID_VALUE);
    this.config.setName(PROP_NAME_VALUE);
    this.config.setProperties(this.properties);
  }

  @Test
  public void testLookBack() throws Exception {
    this.properties.put(PROP_LOOKBACK, "5");
    this.properties.put(PROP_DIMENSIONS, Collections.singleton("b"));

    this.wrapper = new DimensionWrapper(this.provider, this.config, 14, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 3);
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:2:b%3D1", 14, 15));
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:2:b%3D2", 14, 15));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:2:b%3D3", 14, 15));
  }

  @Test
  public void testSingleDimension() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Collections.singleton("b"));

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 3);
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:2:b%3D1"));
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:2:b%3D2"));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:2:b%3D3"));
  }

  @Test
  public void testMultiDimension() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Arrays.asList("a", "b"));

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 6);
    assertEquals(this.runs.get(5), makePipeline("thirdeye:metric:2:a%3D1:b%3D1"));
    assertEquals(this.runs.get(3), makePipeline("thirdeye:metric:2:a%3D1:b%3D2"));
    assertEquals(this.runs.get(4), makePipeline("thirdeye:metric:2:a%3D1:b%3D3"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:2:a%3D2:b%3D1"));
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:2:a%3D2:b%3D2"));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:2:a%3D2:b%3D3"));
  }

  @Test
  public void testMinValue() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Collections.singleton("b"));
    this.properties.put(PROP_MIN_VALUE, 16.0d);

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 2);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:2:b%3D2"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:2:b%3D1"));
  }

  @Test
  public void testMinContribution() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Collections.singleton("b"));
    this.properties.put(PROP_MIN_CONTRIBUTION, 0.40d);

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 1);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:2:b%3D2"));
  }

  @Test
  public void testTopK() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Arrays.asList("a", "b"));
    this.properties.put(PROP_K, 4);

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 4);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:2:a%3D2:b%3D2"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:2:a%3D2:b%3D1"));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:2:a%3D2:b%3D3"));
    assertEquals(this.runs.get(3), makePipeline("thirdeye:metric:2:a%3D1:b%3D2"));
  }

  @Test
  public void testNestedMetricsOnly() throws Exception {
    this.properties.remove(PROP_METRIC_URN);
    this.properties.remove(PROP_DIMENSIONS);
    this.properties.put(PROP_NESTED_METRIC_URNS, Arrays.asList("thirdeye:metric:10", "thirdeye:metric:11", "thirdeye:metric:12"));

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 3);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:10"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:11"));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:12"));
  }

  @Test
  public void testNestedMetricsAndDimensions() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Collections.singleton("b"));
    this.properties.put(PROP_MIN_VALUE, 16.0d);
    this.properties.put(PROP_NESTED_METRIC_URNS, Arrays.asList("thirdeye:metric:10", "thirdeye:metric:11"));

    DatasetConfigDTO dataset = new DatasetConfigDTO();
    dataset.setDataset("TEST");
    dataset.setNonAdditiveBucketSize(5);
    dataset.setNonAdditiveBucketUnit(TimeUnit.MILLISECONDS);
    MetricConfigDTO metric0 = createTestMetricConfig(2L);
    MetricConfigDTO metric1 = createTestMetricConfig(10L);
    MetricConfigDTO metric2 = createTestMetricConfig(11L);

    this.provider = new MockDataProvider()
        .setAggregates(this.aggregates)
        .setMetrics(Arrays.asList(metric0, metric1, metric2))
        .setDatasets(Collections.singletonList(dataset))
        .setAnomalies(Collections.emptyList())
        .setLoader(new MockPipelineLoader(this.runs, this.outputs));

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 4);
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:10:b%3D1"));
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:10:b%3D2"));
    assertEquals(this.runs.get(3), makePipeline("thirdeye:metric:11:b%3D1"));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:11:b%3D2"));
  }

  private DetectionConfigDTO makeConfig(String metricUrn) {
    Map<String, Object> properties = new HashMap<>(this.nestedProperties);
    properties.put(PROP_NESTED_METRIC_URN_KEY_VALUE, metricUrn);

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


  private static void assertEquals(MockPipeline a, MockPipeline b) {
    Assert.assertEquals(a, b);
  }
}
