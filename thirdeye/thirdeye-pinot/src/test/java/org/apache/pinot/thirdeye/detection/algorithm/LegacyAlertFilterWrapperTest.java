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

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.MockPipeline;
import com.linkedin.thirdeye.detection.MockPipelineLoader;
import com.linkedin.thirdeye.detection.MockPipelineOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.detection.DetectionTestUtils.*;


public class LegacyAlertFilterWrapperTest {
  private static final String PROP_SPECS = "specs";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_ANOMALY_FUNCTION_CLASS_NAME = "anomalyFunctionClassName";
  private static final String PROP_ANOMALY_FUNCTION_CLASS_NAME_VALUE = "com.linkedin.thirdeye.anomalydetection.function.MinMaxThresholdFunction";
  private static final Long PROP_ID_VALUE = 1000L;
  private static final String PROP_NAME_VALUE = "myName";
  private static final long START_TIME_VALUE = 1000L;
  private static final long END_TIME_VALUE = 2000L;

  private DataProvider provider;
  private DetectionConfigDTO config;
  private Map<String, Object> properties;
  private List<MockPipeline> runs;
  private Map<String, Object> specs;
  private List<Map<String, Object>> nestedProperties;

  @BeforeMethod
  public void beforeMethod() {
    this.runs = new ArrayList<>();

    this.properties = new HashMap<>();

    this.specs = new HashMap<>();
    specs.put("alertFilter", new HashMap<>());
    specs.put("properties", "min=2;max=2");
    specs.put("exploreDimensions", "a");
    specs.put("metricId", 1);
    specs.put("bucketSize", 1);
    specs.put("bucketUnit", "MILLISECONDS");

    this.properties.put(PROP_SPECS, this.specs);
    Map<String, Object> nestedPropertiesOne = new HashMap<>();
    nestedPropertiesOne.put(PROP_CLASS_NAME, "none");
    nestedPropertiesOne.put(PROP_METRIC_URN, "thirdeye:metric:1");
    nestedPropertiesOne.put(PROP_ANOMALY_FUNCTION_CLASS_NAME, PROP_ANOMALY_FUNCTION_CLASS_NAME_VALUE);

    this.nestedProperties = new ArrayList<>();
    this.nestedProperties.add(nestedPropertiesOne);

    this.properties.put(PROP_NESTED, this.nestedProperties);
    this.config = new DetectionConfigDTO();
    this.config.setId(PROP_ID_VALUE);
    this.config.setName(PROP_NAME_VALUE);
    this.config.setProperties(this.properties);

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    anomalies.add(makeAnomaly(0, 1000));
    anomalies.add(makeAnomaly(1500, 2000));
    MockPipelineLoader mockLoader =
        new MockPipelineLoader(this.runs, Collections.singletonList(new MockPipelineOutput(anomalies, -1)));
    this.provider = new MockDataProvider().setLoader(mockLoader);
  }

  @Test
  public void testRun() throws Exception {
    LegacyAlertFilterWrapper legacyAlertFilterWrapper =
        new LegacyAlertFilterWrapper(this.provider, this.config, START_TIME_VALUE, END_TIME_VALUE);
    DetectionPipelineResult result = legacyAlertFilterWrapper.run();
    Assert.assertEquals(this.runs.size(), 1);
    Assert.assertEquals(this.runs.get(0).getStartTime(), START_TIME_VALUE - TimeUnit.DAYS.toMillis(14));
    Assert.assertEquals(this.runs.get(0).getEndTime(), END_TIME_VALUE);
    Assert.assertEquals(MapUtils.getMap(this.runs.get(0).getConfig().getProperties(), "specs"), specs);
    Assert.assertEquals(result.getAnomalies().size(), 2);
  }
}

