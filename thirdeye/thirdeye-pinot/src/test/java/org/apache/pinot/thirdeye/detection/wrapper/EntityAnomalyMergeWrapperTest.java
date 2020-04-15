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

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionTestUtils;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.MockPipeline;
import org.apache.pinot.thirdeye.detection.MockPipelineLoader;
import org.apache.pinot.thirdeye.detection.MockPipelineOutput;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.DetectionTestUtils.*;
import static org.apache.pinot.thirdeye.detection.DetectionUtils.*;


public class EntityAnomalyMergeWrapperTest {
  private DetectionConfigDTO config;
  private EntityAnomalyMergeWrapper mergeWrapper;
  private Map<String, Object> properties;
  private List<Map<String, Object>> nestedProperties;
  private DataProvider provider;
  private List<MockPipeline> runs;
  private List<MockPipelineOutput> outputs;

  private MergedAnomalyResultDTO childAnomaly1;
  private MergedAnomalyResultDTO childAnomaly2;
  private MergedAnomalyResultDTO childAnomaly3;
  private MergedAnomalyResultDTO childAnomaly4;

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

    this.outputs = new ArrayList<>();

    MergedAnomalyResultDTO parentAnomaly1 = DetectionTestUtils.makeAnomaly(1000, 1400);
    this.childAnomaly1 = DetectionTestUtils.makeAnomaly(1000, 1050);
    this.childAnomaly1.setId(2000L);
    this.childAnomaly2 =  DetectionTestUtils.makeAnomaly(1350, 1400);
    this.childAnomaly2.setId(3000L);
    setEntityChildMapping(parentAnomaly1, this.childAnomaly1);
    setEntityChildMapping(parentAnomaly1, this.childAnomaly2);

    MergedAnomalyResultDTO parentAnomaly2 = DetectionTestUtils.makeAnomaly(1500, 2000);
    this.childAnomaly3 = DetectionTestUtils.makeAnomaly(1350, 1550);
    this.childAnomaly3.setId(3000L);
    this.childAnomaly4 = DetectionTestUtils.makeAnomaly(1950, 2000);
    setEntityChildMapping(parentAnomaly2, this.childAnomaly3);
    setEntityChildMapping(parentAnomaly2, this.childAnomaly4);

    this.outputs.add(new MockPipelineOutput(Arrays.asList(parentAnomaly1), 2900));
    this.outputs.add(new MockPipelineOutput(Arrays.asList(parentAnomaly2), 3000));

    MockPipelineLoader mockLoader = new MockPipelineLoader(this.runs, this.outputs);

    this.provider = new MockDataProvider()
        .setLoader(mockLoader)
        .setAnomalies(Collections.emptyList());
  }

  @Test
  public void testEntityMerger() throws Exception {
    // Max gap of 200 ensures that the above two parent anomalies get merged
    this.config.getProperties().put(PROP_MAX_GAP, 200);

    this.mergeWrapper = new EntityAnomalyMergeWrapper(this.provider, this.config, 1000, 4000);
    DetectionPipelineResult output = this.mergeWrapper.run();

    Assert.assertEquals(output.getAnomalies().size(), 1);
    Assert.assertEquals(output.getLastTimestamp(), 3000);
    Assert.assertEquals(output.getAnomalies().get(0).getStartTime(), 1000L);
    Assert.assertEquals(output.getAnomalies().get(0).getEndTime(), 2000L);
    Assert.assertEquals(output.getAnomalies().get(0).getChildren().size(), 3);

    Set<MergedAnomalyResultDTO> expectedChildAnomalies = ImmutableSet.of(childAnomaly1, childAnomaly3, childAnomaly4);
    Assert.assertTrue(output.getAnomalies().contains(makeAnomaly(1000L, 2000L, expectedChildAnomalies)));
  }
}
