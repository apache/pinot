package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.MockDetectionPipeline;
import com.linkedin.thirdeye.detection.MockDetectionPipelineLoader;
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
  private static final String PROP_PROPERTIES = "properties";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_TARGET = "target";

  // values
  private static final Long PROP_ID_VALUE = 1000L;
  private static final String PROP_NAME_VALUE = "myName";
  private static final String PROP_CLASS_NAME_VALUE = "MyClassName";
  private static final String PROP_TARGET_VALUE = "myMetricUrn";

  private DataProvider provider;
  private DimensionWrapper wrapper;

  private List<MockDetectionPipeline> runs;

  private DetectionConfigDTO config;
  private Map<String, Object> properties;

  @BeforeMethod
  public void beforeMethod() {
    Map<MetricSlice, DataFrame> breakdowns = new HashMap<>();
    breakdowns.put(MetricSlice.from(1, 10, 15),
        new DataFrame()
            .addSeries("a", StringSeries.buildFrom("1", "1", "1", "1", "1", "2", "2", "2", "2", "2"))
            .addSeries("b", StringSeries.buildFrom("1", "2", "1", "2", "3", "1", "2", "1", "2", "3"))
            .addSeries(COL_VALUE, DoubleSeries.buildFrom(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));

    this.runs = new ArrayList<>();

    this.provider = new MockDataProvider()
        .setBreakdowns(breakdowns)
        .setLoader(new MockDetectionPipelineLoader(this.runs));

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_DIMENSIONS, Arrays.asList("a", "b"));
    this.properties.put(PROP_CLASS_NAME, PROP_CLASS_NAME_VALUE);
    this.properties.put(PROP_TARGET, PROP_TARGET_VALUE);
    this.properties.put(PROP_PROPERTIES, Collections.singletonMap("key", "value"));

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
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:1:b%3D1"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:1:b%3D2"));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:1:b%3D3"));
  }

  @Test
  public void testMultiDimension() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Arrays.asList("a", "b"));

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 6);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:1:a%3D1:b%3D1"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:1:a%3D1:b%3D2"));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:1:a%3D1:b%3D3"));
    assertEquals(this.runs.get(3), makePipeline("thirdeye:metric:1:a%3D2:b%3D1"));
    assertEquals(this.runs.get(4), makePipeline("thirdeye:metric:1:a%3D2:b%3D2"));
    assertEquals(this.runs.get(5), makePipeline("thirdeye:metric:1:a%3D2:b%3D3"));
  }

  @Test
  public void testMinValue() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Collections.singleton("b"));
    this.properties.put(PROP_MIN_VALUE, 16.0d);

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 2);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:1:b%3D1"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:1:b%3D2"));
  }

  @Test
  public void testMinContribution() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Collections.singleton("b"));
    this.properties.put(PROP_MIN_CONTRIBUTION, 0.40d);

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 1);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:1:b%3D2"));
  }

  @Test
  public void testTopK() throws Exception {
    this.properties.put(PROP_DIMENSIONS, Arrays.asList("a", "b"));
    this.properties.put(PROP_K, 4);

    this.wrapper = new DimensionWrapper(this.provider, this.config, 10, 15);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 4);
    assertEquals(this.runs.get(0), makePipeline("thirdeye:metric:1:a%3D2:b%3D2"));
    assertEquals(this.runs.get(1), makePipeline("thirdeye:metric:1:a%3D2:b%3D1"));
    assertEquals(this.runs.get(2), makePipeline("thirdeye:metric:1:a%3D2:b%3D3"));
    assertEquals(this.runs.get(3), makePipeline("thirdeye:metric:1:a%3D1:b%3D2"));
  }

  private DetectionConfigDTO makeConfig(String metricUrn) {
    Map<String, Object> properties = new HashMap<>((Map<String, Object>) this.properties.get(PROP_PROPERTIES));
    properties.put(PROP_TARGET_VALUE, metricUrn);

    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setId(this.config.getId());
    config.setName(this.config.getName());
    config.setProperties(properties);
    config.setClassName(PROP_CLASS_NAME_VALUE);

    return config;
  }

  private MockDetectionPipeline makePipeline(String metricUrn) {
    return new MockDetectionPipeline(this.provider, makeConfig(metricUrn), 10, 15);
  }

  private static void assertEquals(MockDetectionPipeline a, MockDetectionPipeline b) {
    Assert.assertEquals(a, b);
  }
}
