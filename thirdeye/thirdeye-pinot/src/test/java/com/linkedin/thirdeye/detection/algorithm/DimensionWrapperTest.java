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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;

public class DimensionWrapperTest {
  // exploration
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_DIMENSIONS = "dimensions";
  private static final String PROP_MIN_VALUE = "minValue";
  private static final String PROP_MIN_CONTRIBUTION = "minContribution";
  private static final String PROP_K = "k";

  // prototyping
  private static final String PROP_NESTED = "nested";
  private static final String PROP_NESTED_METRIC_URN = "nestedMetricUrn";
  private static final String PROP_NESTED_METRIC_URN_KEY = "nestedMetricUrnKey";
  private static final String PROP_CLASS_NAME = "className";

  // values
  private static final Long PROP_ID_VALUE = 1000L;
  private static final String PROP_NAME_VALUE = "myName";
  private static final String PROP_CLASS_NAME_VALUE = "MyClassName";
  private static final String PROP_NESTED_METRIC_URN_VALUE = "thirdeye:metric:2";
  private static final String PROP_NESTED_METRIC_URN_KEY_VALUE = "myMetricUrn";

  private DataProvider provider;
  private DimensionWrapper wrapper;

  private List<MockPipeline> runs;
  private List<MockPipelineOutput> outputs;

  private DetectionConfigDTO config;
  private Map<String, Object> properties;
  private Map<String, Object> nestedProperties;

  @BeforeMethod
  public void beforeMethod() {
    Map<MetricSlice, DataFrame> aggregates = new HashMap<>();
    aggregates.put(MetricSlice.from(1, 10, 15),
        new DataFrame()
            .addSeries("a", StringSeries.buildFrom("1", "1", "1", "1", "1", "2", "2", "2", "2", "2"))
            .addSeries("b", StringSeries.buildFrom("1", "2", "1", "2", "3", "1", "2", "1", "2", "3"))
            .addSeries(COL_VALUE, DoubleSeries.buildFrom(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));

    this.runs = new ArrayList<>();

    this.outputs = new ArrayList<>();

    this.provider = new MockDataProvider()
        .setAggregates(aggregates)
        .setLoader(new MockPipelineLoader(this.runs, this.outputs));

    this.nestedProperties = new HashMap<>();
    this.nestedProperties.put(PROP_CLASS_NAME, PROP_CLASS_NAME_VALUE);
    this.nestedProperties.put("key", "value");

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_DIMENSIONS, Arrays.asList("a", "b"));
    this.properties.put(PROP_NESTED_METRIC_URN_KEY, PROP_NESTED_METRIC_URN_KEY_VALUE);
    this.properties.put(PROP_NESTED_METRIC_URN, PROP_NESTED_METRIC_URN_VALUE);
    this.properties.put(PROP_NESTED, Collections.singletonList(this.nestedProperties));

    this.config = new DetectionConfigDTO();
    this.config.setId(PROP_ID_VALUE);
    this.config.setName(PROP_NAME_VALUE);
    this.config.setProperties(this.properties);
  }

  @Test
  public void testSingleDimension() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Collections.singleton("b"));

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 3);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:2:b%3D1"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:2:b%3D2"));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:2:b%3D3"));
  }

  @Test
  public void testMultiDimension() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Arrays.asList("a", "b"));

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 6);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:2:a%3D1:b%3D1"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:2:a%3D1:b%3D2"));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:2:a%3D1:b%3D3"));
    assertEquals(this.runs.get(3), makePipeline("thirdeye:metric:2:a%3D2:b%3D1"));
    assertEquals(this.runs.get(4), makePipeline("thirdeye:metric:2:a%3D2:b%3D2"));
    assertEquals(this.runs.get(5), makePipeline("thirdeye:metric:2:a%3D2:b%3D3"));
  }

  @Test
  public void testMinValue() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Collections.singleton("b"));
    this.properties.put(PROP_MIN_VALUE, 16.0d);

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 2);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:2:b%3D1"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:2:b%3D2"));
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

  private DetectionConfigDTO makeConfig(String metricUrn) {
    Map<String, Object> properties = new HashMap<>(this.nestedProperties);
    properties.put(PROP_NESTED_METRIC_URN_KEY_VALUE, metricUrn);

    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setId(this.config.getId());
    config.setName(this.config.getName());
    config.setProperties(properties);
    config.setClassName(PROP_CLASS_NAME_VALUE);

    return config;
  }

  private MockPipeline makePipeline(String metricUrn) {
    return new MockPipeline(this.provider, makeConfig(metricUrn), 10, 15,
        new MockPipelineOutput(Collections.<MergedAnomalyResultDTO>emptyList(), -1));
  }

  private static void assertEquals(MockPipeline a, MockPipeline b) {
    Assert.assertEquals(a, b);
  }
}
