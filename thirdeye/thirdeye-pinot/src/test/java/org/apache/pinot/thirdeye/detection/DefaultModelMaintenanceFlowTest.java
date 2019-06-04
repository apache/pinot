/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package org.apache.pinot.thirdeye.detection;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.MockModelEvaluator;
import org.apache.pinot.thirdeye.detection.components.MockTunableDetector;
import org.apache.pinot.thirdeye.detection.spec.MockModelEvaluatorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.ModelEvaluator;
import org.apache.pinot.thirdeye.detection.spi.model.ModelStatus;
import org.joda.time.Instant;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DefaultModelMaintenanceFlowTest {
  private static final String METRIC_NAME = "test_metric";
  private static final String DATASET_NAME = "test_dataset";

  private ModelMaintenanceFlow maintenanceFlow;
  private DataProvider provider;
  private InputDataFetcher dataFetcher;
  private long configId;

  @BeforeMethod
  public void setUp() {
    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setId(101L);
    metric.setName(METRIC_NAME);
    metric.setDataset(DATASET_NAME);
    DatasetConfigDTO dataset = new DatasetConfigDTO();
    dataset.setId(102L);
    dataset.setDataset(DATASET_NAME);
    this.provider = new MockDataProvider().setMetrics(Collections.singletonList(metric)).setDatasets(Collections.singletonList(dataset));
    this.configId = 100L;
    this.dataFetcher = new DefaultInputDataFetcher(this.provider, this.configId);
    this.maintenanceFlow = new ModelRetuneFlow(this.provider, DetectionRegistry.getInstance());
  }

  @Test
  public void testMaintainNotTunable() {
    DetectionConfigDTO configDTO = new DetectionConfigDTO();
    configDTO.setId(this.configId);
    ModelEvaluator evaluator = new MockModelEvaluator();
    MockModelEvaluatorSpec spec = new MockModelEvaluatorSpec();
    spec.setMockModelStatus(ModelStatus.GOOD);
    configDTO.setComponents(ImmutableMap.of("evaluator_1", evaluator));
    DetectionConfigDTO maintainedConfig = this.maintenanceFlow.maintain(configDTO, Instant.now());
    Assert.assertEquals(configDTO, maintainedConfig);
  }

  @Test
  public void testMaintainTunableGood() {
    DetectionConfigDTO configDTO = new DetectionConfigDTO();
    configDTO.setId(this.configId);
    ModelEvaluator evaluator = new MockModelEvaluator();
    MockModelEvaluatorSpec spec = new MockModelEvaluatorSpec();
    spec.setMockModelStatus(ModelStatus.GOOD);
    evaluator.init(spec, this.dataFetcher);
    MockTunableDetector detector = new MockTunableDetector();
    configDTO.setComponents(ImmutableMap.of("evaluator_1", evaluator, "detector", detector));
    DetectionConfigDTO maintainedConfig = this.maintenanceFlow.maintain(configDTO, Instant.now());
    Assert.assertEquals(configDTO, maintainedConfig);
  }

  @Test
  public void testMaintainTunableBad() {
    DetectionRegistry.registerTunableComponent(MockTunableDetector.class.getName(), "MOCK_TUNABLE", "MOCK_TUNABLE");
    DetectionConfigDTO configDTO = new DetectionConfigDTO();
    configDTO.setId(this.configId);
    configDTO.setYaml(String.format("metric: %s\ndataset: %s\n", METRIC_NAME, DATASET_NAME));
    ModelEvaluator evaluator = new MockModelEvaluator();
    MockModelEvaluatorSpec spec = new MockModelEvaluatorSpec();
    spec.setMockModelStatus(ModelStatus.BAD);
    evaluator.init(spec, this.dataFetcher);
    MockTunableDetector tunableDetector = new MockTunableDetector();
    configDTO.setComponents(ImmutableMap.of("evaluator_1", evaluator, "detector", tunableDetector));
    configDTO.setLastTuningTimestamp(1559175301000L);
    configDTO.setComponentSpecs(ImmutableMap.of("detector:MOCK_TUNABLE", ImmutableMap.of("className", MockTunableDetector.class.getName())));
    Instant maintainTimestamp = Instant.now();
    DetectionConfigDTO maintainedConfig = this.maintenanceFlow.maintain(configDTO, maintainTimestamp);
    Assert.assertEquals(maintainedConfig.getLastTuningTimestamp(), maintainTimestamp.getMillis());
  }

}