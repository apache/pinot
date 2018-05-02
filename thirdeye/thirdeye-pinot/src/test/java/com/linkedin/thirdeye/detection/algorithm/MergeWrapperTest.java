package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineLoader;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.MockDataProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MergeWrapperTest {
  private DetectionConfigDTO config;
  private MergeWrapper wrapper;
  private Map<String, Object> properties;
  private Map<String, Map<String, Object>> nestedPropertiesMap;
  private DataProvider provider;
  private List<MockMergerPipeline> runs;
  private List<MockMergerPipelineOutput> outputs;

  private static final Long PROP_ID_VALUE = 1000L;
  private static final String PROP_NAME_VALUE = "myName";

  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_PROPERTIES = "properties";
  private static final String PROP_TARGET = "target";
  private static final String PROP_TARGET_VALUE = "myMetricUrn";
  private static final String PROP_PROPERTIES_MAP = "propertiesMap";
  private static final String PROP_MAX_GAP = "maxGap";
  private static final String PROP_MAX_DURATION = "maxDuration";

  @BeforeMethod
  public void beforeMethod() {
    this.runs = new ArrayList<>();

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_TARGET, PROP_TARGET_VALUE);
    this.properties.put(PROP_PROPERTIES, Collections.singletonMap("key", "value"));

    Map<String, Object> nestedPropertiesOne = new HashMap<>();
    nestedPropertiesOne.put(PROP_TARGET, PROP_TARGET_VALUE);
    nestedPropertiesOne.put(PROP_METRIC_URN, "thirdeye:metric:1");

    Map<String, Object> nestedPropertiesTwo = new HashMap<>();
    nestedPropertiesTwo.put(PROP_TARGET, PROP_TARGET_VALUE);
    nestedPropertiesTwo.put(PROP_METRIC_URN, "thirdeye:metric:2");

    this.nestedPropertiesMap = new HashMap<>();
    this.nestedPropertiesMap.put("one", nestedPropertiesOne);
    this.nestedPropertiesMap.put("two", nestedPropertiesTwo);

    this.properties.put(PROP_PROPERTIES_MAP, nestedPropertiesMap);

    this.config = new DetectionConfigDTO();
    this.config.setId(PROP_ID_VALUE);
    this.config.setName(PROP_NAME_VALUE);
    this.config.setProperties(this.properties);

    List<MergedAnomalyResultDTO> existing = new ArrayList<>();
    existing.add(makeAnomaly(PROP_ID_VALUE,    0, 1000, Collections.<String, String>emptyMap()));
    existing.add(makeAnomaly(PROP_ID_VALUE, 1500, 2000, Collections.<String, String>emptyMap()));

    this.outputs = new ArrayList<>();

    this.outputs.add(new MockMergerPipelineOutput(Arrays.asList(
        makeAnomaly(PROP_ID_VALUE, 1100, 1200, Collections.<String, String>emptyMap()),
        makeAnomaly(PROP_ID_VALUE, 2200, 2300, Collections.<String, String>emptyMap())
    ), 2900));

    this.outputs.add(new MockMergerPipelineOutput(Arrays.asList(
        makeAnomaly(PROP_ID_VALUE, 1150, 1250, Collections.<String, String>emptyMap()),
        makeAnomaly(PROP_ID_VALUE, 2400, 2800, Collections.<String, String>emptyMap())
    ), 3000));

    MockMergerLoader mockLoader = new MockMergerLoader(this.runs, this.outputs);

    this.provider = new MockDataProvider()
        .setAnomalies(existing)
        .setLoader(mockLoader);
  }

  @Test
  public void testMergerPassthru() throws Exception {
    this.config.getProperties().put(PROP_MAX_DURATION, 0);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 6);
    Assert.assertEquals(output.getLastTimestamp(), 2900);
  }

  @Test
  public void testMergerMaxGap() throws Exception {
    this.config.getProperties().put(PROP_MAX_GAP, 100);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 8);
    Assert.assertEquals(output.getLastTimestamp(), 2900);
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(PROP_ID_VALUE, 0, 1250, Collections.<String, String>emptyMap())));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(PROP_ID_VALUE, 1500, 2000, Collections.<String, String>emptyMap())));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(PROP_ID_VALUE, 2200, 2800, Collections.<String, String>emptyMap())));
  }

  @Test
  public void testMergerMaxDuration() throws Exception {
    this.config.getProperties().put(PROP_MAX_GAP, 200);
    this.config.getProperties().put(PROP_MAX_DURATION, 1250);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 8);
    Assert.assertEquals(output.getLastTimestamp(), 2900);
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(PROP_ID_VALUE, 0, 1250, Collections.<String, String>emptyMap())));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(PROP_ID_VALUE, 1500, 2300, Collections.<String, String>emptyMap())));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(PROP_ID_VALUE, 2400, 2800, Collections.<String, String>emptyMap())));
  }

  @Test
  public void testMergerExecution() throws Exception {
    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 2);

    Set<String> metrics = new HashSet<>();
    for (MockMergerPipeline run : this.runs) {
      metrics.add(run.getConfig().getProperties().get(PROP_METRIC_URN).toString());
    }

    Assert.assertEquals(metrics, new HashSet<>(Arrays.asList("thirdeye:metric:1", "thirdeye:metric:2")));
  }

  @Test
  public void testMergerDimensions() throws Exception {
    this.config.getProperties().put(PROP_MAX_GAP, 200);
    this.config.getProperties().put(PROP_MAX_DURATION, 1250);

    this.outputs.add(new MockMergerPipelineOutput(Arrays.asList(
        makeAnomaly(PROP_ID_VALUE, 1150, 1250, Collections.singletonMap("key", "value")),
        makeAnomaly(PROP_ID_VALUE, 2400, 2800, Collections.singletonMap("otherKey", "value"))
    ), 3000));

    this.outputs.add(new MockMergerPipelineOutput(Arrays.asList(
        makeAnomaly(PROP_ID_VALUE, 1250, 1300, Collections.singletonMap("key", "value")),
        makeAnomaly(PROP_ID_VALUE, 2700, 2900, Collections.singletonMap("otherKey", "otherValue"))
    ), 3000));

    Map<String, Object> nestedPropertiesThree = new HashMap<>();
    nestedPropertiesThree.put(PROP_TARGET, PROP_TARGET_VALUE);
    nestedPropertiesThree.put(PROP_METRIC_URN, "thirdeye:metric:1");

    Map<String, Object> nestedPropertiesFour = new HashMap<>();
    nestedPropertiesFour.put(PROP_TARGET, PROP_TARGET_VALUE);
    nestedPropertiesFour.put(PROP_METRIC_URN, "thirdeye:metric:1");

    this.nestedPropertiesMap.put("three", nestedPropertiesThree);
    this.nestedPropertiesMap.put("four", nestedPropertiesFour);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 13);
    Assert.assertEquals(output.getLastTimestamp(), 2900);
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(PROP_ID_VALUE, 0, 1250, Collections.<String, String>emptyMap())));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(PROP_ID_VALUE, 1500, 2300, Collections.<String, String>emptyMap())));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(PROP_ID_VALUE, 2400, 2800, Collections.<String, String>emptyMap())));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(PROP_ID_VALUE, 1150, 1300, Collections.singletonMap("key", "value"))));
  }

  static MergedAnomalyResultDTO makeAnomaly(long configId, long start, long end, Map<String, String> dimensions) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setDetectionConfigId(configId);
    anomaly.setStartTime(start);
    anomaly.setEndTime(end);

    DimensionMap dimMap = new DimensionMap();
    dimMap.putAll(dimensions);
    anomaly.setDimensions(dimMap);

    return anomaly;
  }

  static class MockMergerLoader extends DetectionPipelineLoader {
    final List<MockMergerPipeline> runs;
    final List<MockMergerPipelineOutput> outputs;
    int offset = 0;

    public MockMergerLoader(List<MockMergerPipeline> runs, List<MockMergerPipelineOutput> outputs) {
      this.outputs = outputs;
      this.runs = runs;
    }

    @Override
    public DetectionPipeline from(DataProvider provider, DetectionConfigDTO config, long start, long end) throws Exception {
      MockMergerPipeline p = new MockMergerPipeline(provider, config, start, end, outputs.get(this.offset++));
      this.runs.add(p);
      return p;
    }
  }

  static class MockMergerPipeline extends DetectionPipeline {
    final MockMergerPipelineOutput output;

    public MockMergerPipeline(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime, MockMergerPipelineOutput output) {
      super(provider, config, startTime, endTime);
      this.output = output;
    }

    @Override
    public DetectionPipelineResult run() {
      return new DetectionPipelineResult(this.output.anomalies, this.output.lastTimestamp);
    }
  }

  static class MockMergerPipelineOutput {
    final List<MergedAnomalyResultDTO> anomalies;
    final long lastTimestamp;

    public MockMergerPipelineOutput(List<MergedAnomalyResultDTO> anomalies, long lastTimestamp) {
      this.anomalies = anomalies;
      this.lastTimestamp = lastTimestamp;
    }
  }
}
