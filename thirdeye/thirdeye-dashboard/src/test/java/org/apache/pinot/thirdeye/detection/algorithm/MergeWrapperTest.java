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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.thirdeye.anomaly.AnomalyType;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.MockPipeline;
import org.apache.pinot.thirdeye.detection.MockPipelineLoader;
import org.apache.pinot.thirdeye.detection.MockPipelineOutput;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.DetectionTestUtils.*;


public class MergeWrapperTest {
  private DetectionConfigDTO config;
  private MergeWrapper wrapper;
  private Map<String, Object> properties;
  private List<Map<String, Object>> nestedProperties;
  private DataProvider provider;
  private List<MockPipeline> runs;
  private List<MockPipelineOutput> outputs;
  private MockPipelineLoader mockLoader;

  private static final Long PROP_ID_VALUE = 1000L;
  private static final String PROP_NAME_VALUE = "myName";

  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_PROPERTIES = "properties";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_MAX_GAP = "maxGap";
  private static final String PROP_MAX_DURATION = "maxDuration";

  /*
    Here are the anomalies in the test.

    Existing anomalies:
   100                1000             1500       2000
    |-----------------|                |----------|
    New  anomalies:
   50                    1200                     2200  2300
   |-------------------------|                        |-----|
                           1150 1250                            2400          2800
                            |-----|                               |------------|
  */
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

    List<MergedAnomalyResultDTO> existing = new ArrayList<>();
    // For existing anomalies add ids.
    existing.add(setAnomalyId(makeAnomaly(100, 1000), 0));
    existing.add(setAnomalyId(makeAnomaly(1500, 2000), 1));

    this.outputs = new ArrayList<>();

    this.outputs.add(new MockPipelineOutput(Arrays.asList(makeAnomaly(50, 1200), makeAnomaly(2200, 2300)), 2900));

    this.outputs.add(new MockPipelineOutput(Arrays.asList(makeAnomaly(1150, 1250), makeAnomaly(2400, 2800)), 3000));

    this.mockLoader = new MockPipelineLoader(this.runs, this.outputs);

    this.provider = new MockDataProvider().setAnomalies(existing).setLoader(this.mockLoader);
  }

  @Test
  public void testMergerPassthru() throws Exception {
    this.config.getProperties().put(PROP_MAX_GAP, 0);
    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 3);
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(50, 1250), 0L)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(2200, 2300)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(2400, 2800)));
    Assert.assertEquals(output.getLastTimestamp(), 3000);
  }

  @Test
  public void testMergerMaxGap() throws Exception {
    this.config.getProperties().put(PROP_MAX_GAP, 100);
    this.outputs.add(new MockPipelineOutput(Arrays.asList(makeAnomaly(0, 1200)), 2900));

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    DetectionPipelineResult output = this.wrapper.run();

    // anomaly [1500, 2000] is not modified
    Assert.assertEquals(output.getAnomalies().size(), 2);
    Assert.assertEquals(output.getLastTimestamp(), 3000);
    // anomalies [100, 1000] and [1150,1250] are merged into [50, 1200]
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(50, 1250), 0)));
    // anomalies [2200, 2300] and [2400, 2800] are merged
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(2200, 2800)));
  }

  @Test
  public void testMergerMaxDuration() throws Exception {
    this.config.getProperties().put(PROP_MAX_GAP, 200);
    this.config.getProperties().put(PROP_MAX_DURATION, 1250);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 3);
    Assert.assertEquals(output.getLastTimestamp(), 3000);
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(50, 1250), 0)));
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(1500, 2300), 1)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(2400, 2800)));
  }

  @Test
  public void testMergerMaxDurationOverlapping() throws Exception {
    this.config.getProperties().put(PROP_MAX_GAP, 200);
    this.config.getProperties().put(PROP_MAX_DURATION, 1250);

    this.outputs.add(new MockPipelineOutput(Arrays.asList(makeAnomaly(2800, 3700), makeAnomaly(3700, 3800)), 3700));

    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_CLASS_NAME, "none");
    nestedProperties.put(PROP_METRIC_URN, "thirdeye:metric:3");

    this.nestedProperties.add(nestedProperties);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 4000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 4);
    Assert.assertEquals(output.getLastTimestamp(), 3700);
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(50, 1250), 0)));
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(1500, 2300), 1)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(2400, 3650)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(3650, 3800)));
  }

  @Test
  public void testMergerMaxDurationSuperset() throws Exception {
    this.config.getProperties().put(PROP_MAX_GAP, 200);
    this.config.getProperties().put(PROP_MAX_DURATION, 1250);

    this.outputs.add(new MockPipelineOutput(Arrays.asList(makeAnomaly(2800, 3800), makeAnomaly(3500, 3600)), 3700));

    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_CLASS_NAME, "none");
    nestedProperties.put(PROP_METRIC_URN, "thirdeye:metric:3");

    this.nestedProperties.add(nestedProperties);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 4000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 4);
    Assert.assertEquals(output.getLastTimestamp(), 3700);
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(50, 1250), 0)));
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(1500, 2300), 1)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(2400, 3650)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(3650, 3800)));
  }

  @Test
  public void testMergerMaxDurationEnforce() throws Exception {
    this.config.getProperties().put(PROP_MAX_DURATION, 500);

    this.outputs.add(new MockPipelineOutput(Arrays.asList(makeAnomaly(2800, 3800), makeAnomaly(3500, 3600)), 3700));

    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_CLASS_NAME, "none");
    nestedProperties.put(PROP_METRIC_URN, "thirdeye:metric:3");

    this.nestedProperties.add(nestedProperties);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 4000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 6);
    Assert.assertEquals(output.getLastTimestamp(), 3700);
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(50, 1200), 0)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(1200, 1250)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(2200, 2300)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(2400, 2900)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(2900, 3400)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(3400, 3800)));
  }

  @Test
  public void testMergerExecution() throws Exception {
    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 2);

    Set<String> metrics = new HashSet<>();
    for (MockPipeline run : this.runs) {
      metrics.add(run.getConfig().getProperties().get(PROP_METRIC_URN).toString());
    }

    Assert.assertEquals(metrics, new HashSet<>(Arrays.asList("thirdeye:metric:1", "thirdeye:metric:2")));
  }

  @Test
  public void testMergerExecutionNoNested() throws Exception {
    this.config.getProperties().put(PROP_NESTED, Collections.<Map<String, Object>>emptyList());

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    this.wrapper.run();

    Assert.assertEquals(this.runs.size(), 0);
  }

  @Test
  public void testMergerDimensions() throws Exception {
    this.config.getProperties().put(PROP_MAX_GAP, 200);
    this.config.getProperties().put(PROP_MAX_DURATION, 1250);

    this.outputs.add(new MockPipelineOutput(
        Arrays.asList(makeAnomaly(1150, 1250, Collections.singletonMap("key", "value")),
            makeAnomaly(2400, 2800, Collections.singletonMap("otherKey", "value"))), 3000));

    this.outputs.add(new MockPipelineOutput(
        Arrays.asList(makeAnomaly(1250, 1300, Collections.singletonMap("key", "value")),
            makeAnomaly(2700, 2900, Collections.singletonMap("otherKey", "otherValue"))), 3000));

    Map<String, Object> nestedPropertiesThree = new HashMap<>();
    nestedPropertiesThree.put(PROP_CLASS_NAME, "none");
    nestedPropertiesThree.put(PROP_METRIC_URN, "thirdeye:metric:1");

    Map<String, Object> nestedPropertiesFour = new HashMap<>();
    nestedPropertiesFour.put(PROP_CLASS_NAME, "none");
    nestedPropertiesFour.put(PROP_METRIC_URN, "thirdeye:metric:1");

    this.nestedProperties.add(nestedPropertiesThree);
    this.nestedProperties.add(nestedPropertiesFour);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 6);
    Assert.assertEquals(output.getLastTimestamp(), 3000);
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(50, 1250), 0)));
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(1500, 2300), 1)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(2400, 2800)));
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(1150, 1300, Collections.singletonMap("key", "value"))));
    Assert.assertTrue(
        output.getAnomalies().contains(makeAnomaly(2400, 2800, Collections.singletonMap("otherKey", "value"))));
    Assert.assertTrue(
        output.getAnomalies().contains(makeAnomaly(2700, 2900, Collections.singletonMap("otherKey", "otherValue"))));
  }

  @Test
  public void testMergeProperties() throws Exception {

    MergedAnomalyResultDTO anomaly = makeAnomaly(1100, 1250);
    String propertyKey = "trend_day1";
    String propertyValue = "{trend_info}";
    anomaly.setProperties(Collections.singletonMap(propertyKey, propertyValue));

    this.outputs.add(new MockPipelineOutput(Arrays.asList(anomaly), 3700));

    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_CLASS_NAME, "none");
    nestedProperties.put(PROP_METRIC_URN, "thirdeye:metric:1");

    this.nestedProperties.add(nestedProperties);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 4000);
    DetectionPipelineResult output = this.wrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 1);
    Assert.assertEquals(output.getLastTimestamp(), 3700);
    Assert.assertTrue(output.getAnomalies().contains(setAnomalyId(makeAnomaly(50, 2800), 0)));
    Assert.assertTrue(output.getAnomalies().get(0).getProperties().get(propertyKey).equals(propertyValue));
  }

  @Test
  public void testMergerAnomalyType() throws Exception {
    this.config.getProperties().put(PROP_MAX_GAP, 100);

    MergedAnomalyResultDTO trendAnomaly = makeAnomaly(100, 1000);
    trendAnomaly.setType(AnomalyType.TREND_CHANGE);

    MergedAnomalyResultDTO deviationAnomaly = makeAnomaly(50, 1200);
    deviationAnomaly.setType(AnomalyType.DEVIATION);

    this.outputs.add(new MockPipelineOutput(Arrays.asList(trendAnomaly, deviationAnomaly), 3700));

    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_CLASS_NAME, "none");
    nestedProperties.put(PROP_METRIC_URN, "thirdeye:metric:3");

    this.nestedProperties.add(nestedProperties);

    this.wrapper = new MergeWrapper(this.provider, this.config, 1000, 3000);
    DetectionPipelineResult output = this.wrapper.run();

    // trend anomaly was not merged with deviation Anomaly
    Assert.assertTrue(output.getAnomalies().contains(trendAnomaly));
  }
}
