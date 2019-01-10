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
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
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
import org.apache.commons.collections.MapUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;
import static com.linkedin.thirdeye.detection.DetectionTestUtils.*;


public class LegacyMergeWrapperTest {
  private static final Long PROP_ID_VALUE = 1000L;
  private static final String PROP_NAME_VALUE = "myName";
  private static final String PROP_SPECS = "specs";

  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_PROPERTIES = "properties";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_ANOMALY_FUNCTION_CLASS_NAME = "anomalyFunctionClassName";
  private static final String PROP_ANOMALY_FUNCTION_CLASS_NAME_VALUE = "com.linkedin.thirdeye.anomalydetection.function.MinMaxThresholdFunction";

  private DetectionConfigDTO config;
  private Map<String, Object> properties;
  private List<Map<String, Object>> nestedProperties;
  private DataProvider provider;
  private List<MockPipeline> runs;
  private List<MockPipelineOutput> outputs;
  private Map<String, Object> specs;
  private Map<MetricSlice, DataFrame> timeseries;

  @BeforeMethod
  public void beforeMethod() {
    this.runs = new ArrayList<>();

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_PROPERTIES, Collections.singletonMap("key", "value"));
    this.properties.put(PROP_ANOMALY_FUNCTION_CLASS_NAME, PROP_ANOMALY_FUNCTION_CLASS_NAME_VALUE);

    this.specs = new HashMap<>();
    specs.put("properties", "min=2;max=2");
    specs.put("exploreDimensions", "a");
    specs.put("metricId", 1);
    specs.put("bucketSize", 1);
    specs.put("bucketUnit", "MILLISECONDS");

    this.properties.put(PROP_SPECS, this.specs);

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

    List<MergedAnomalyResultDTO> existing = new ArrayList<>();
    existing.add(makeAnomaly(0, 1000));
    existing.add(makeAnomaly(1500, 2000));

    this.outputs = new ArrayList<>();

    this.outputs.add(new MockPipelineOutput(Arrays.asList(makeAnomaly(1100, 1200), makeAnomaly(2200, 2300)), 2900));

    MockPipelineLoader mockLoader = new MockPipelineLoader(this.runs, this.outputs);

    this.timeseries = new HashMap<>();
    this.timeseries.put(MetricSlice.from(1, 1000, 3000),
        new DataFrame().addSeries(COL_VALUE, 200, 500, 1000).addSeries(COL_TIME, 1000, 2000, 3000));

    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setId(1L);
    List<MetricConfigDTO> metrics = Collections.singletonList(metric);
    this.provider = new MockDataProvider().setTimeseries(this.timeseries)
        .setAnomalies(existing)
        .setMetrics(metrics)
        .setLoader(mockLoader);
  }

  @Test
  public void testRun() throws Exception {
    LegacyMergeWrapper mergeWrapper = new LegacyMergeWrapper(this.provider, this.config, 1000, 3000);
    DetectionPipelineResult result = mergeWrapper.run();
    Assert.assertEquals(this.runs.size(), 1);
    Assert.assertEquals(this.runs.get(0).getConfig().getName(), PROP_NAME_VALUE);
    Assert.assertEquals(MapUtils.getMap(this.runs.get(0).getConfig().getProperties(), "specs"), specs);
    Assert.assertEquals(MapUtils.getString(this.runs.get(0).getConfig().getProperties(), PROP_ANOMALY_FUNCTION_CLASS_NAME),
        PROP_ANOMALY_FUNCTION_CLASS_NAME_VALUE);
    Assert.assertEquals(result.getAnomalies().size(), 1);
    Assert.assertEquals(result.getAnomalies().get(0).getStartTime(), 0);
    Assert.assertEquals(result.getAnomalies().get(0).getEndTime(), 2300);
    Assert.assertEquals(result.getAnomalies().get(0).getDetectionConfigId(), PROP_ID_VALUE);
    Assert.assertNull(result.getAnomalies().get(0).getFunctionId());
    Assert.assertNull(result.getAnomalies().get(0).getFunction());
  }
}
