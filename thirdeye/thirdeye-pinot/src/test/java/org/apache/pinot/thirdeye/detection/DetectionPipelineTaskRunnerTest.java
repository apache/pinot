/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.detection;

import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DetectionPipelineTaskRunnerTest {
  private List<MockPipeline> runs;
  private List<MockPipelineOutput> outputs;

  private DetectionPipelineTaskRunner runner;
  private DetectionPipelineTaskInfo info;
  private TaskContext context;

  private DAOTestBase testDAOProvider;
  private DetectionConfigManager detectionDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private EvaluationManager evaluationDAO;
  private TaskManager taskDAO;
  private DetectionPipelineLoader loader;
  private DataProvider provider;
  private Map<String, Object> properties;

  private long detectorId;

  @BeforeMethod
  public void beforeMethod() {
    this.runs = new ArrayList<>();

    this.outputs = new ArrayList<>();

    this.testDAOProvider = DAOTestBase.getInstance();
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.evaluationDAO = DAORegistry.getInstance().getEvaluationManager();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.loader = new MockPipelineLoader(this.runs, this.outputs);
    this.provider = new MockDataProvider();

    this.properties = new HashMap<>();
    this.properties.put("metricUrn", "thirdeye:metric:1");
    this.properties.put("className", "myClassName");

    DetectionConfigDTO detector = new DetectionConfigDTO();
    detector.setProperties(this.properties);
    detector.setName("myName");
    detector.setDescription("myDescription");
    detector.setCron("myCron");
    this.detectorId = this.detectionDAO.save(detector);

    this.runner = new DetectionPipelineTaskRunner(
        this.detectionDAO,
        this.anomalyDAO,
        this.evaluationDAO,
        this.loader,
        this.provider
    );

    this.info = new DetectionPipelineTaskInfo();
    this.info.setConfigId(this.detectorId);
    this.info.setStart(1250);
    this.info.setEnd(1500);

    this.context = new TaskContext();

  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    this.testDAOProvider.cleanup();
  }

  @Test
  public void testTaskRunnerLoading() throws Exception {
    this.runner.execute(this.info, this.context);

    Assert.assertEquals(this.runs.size(), 1);
    Assert.assertEquals(this.runs.get(0).getStartTime(), 1250);
    Assert.assertEquals(this.runs.get(0).getEndTime(), 1500);
    Assert.assertEquals(this.runs.get(0).getConfig().getName(), "myName");
    Assert.assertEquals(this.runs.get(0).getConfig().getDescription(), "myDescription");
    Assert.assertEquals(this.runs.get(0).getConfig().getProperties().get("className"), "myClassName");
    Assert.assertEquals(this.runs.get(0).getConfig().getCron(), "myCron");
  }

  @Test
  public void testTaskRunnerPersistence() throws Exception {
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(this.detectorId, 1300, 1400, null, null, Collections.singletonMap("myKey", "myValue"));

    this.outputs.add(new MockPipelineOutput(Collections.singletonList(anomaly), 1400));

    this.runner.execute(this.info, this.context);

    Assert.assertNotNull(anomaly.getId());

    MergedAnomalyResultDTO readAnomaly = this.anomalyDAO.findById(anomaly.getId());
    Assert.assertEquals(readAnomaly.getDetectionConfigId(), Long.valueOf(this.detectorId));
    Assert.assertEquals(readAnomaly.getStartTime(), 1300);
    Assert.assertEquals(readAnomaly.getEndTime(), 1400);
    Assert.assertEquals(readAnomaly.getDimensions(), new DimensionMap("{\"myKey\":\"myValue\"}"));
  }

  @Test
  public void testTaskRunnerPersistenceFailTimestamp() throws Exception {
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(this.detectorId, 1300, 1400, null, null,
        Collections.singletonMap("myKey", "myValue"));

    this.outputs.add(new MockPipelineOutput(Collections.singletonList(anomaly), -1));

    this.runner.execute(this.info, this.context);

    Assert.assertNull(anomaly.getId());
  }
}
