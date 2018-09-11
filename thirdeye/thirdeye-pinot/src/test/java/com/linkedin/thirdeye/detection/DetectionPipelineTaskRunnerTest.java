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

package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
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
    this.loader = new MockPipelineLoader(this.runs, this.outputs);
    this.provider = new MockDataProvider();

    this.properties = new HashMap<>();
    this.properties.put("metricUrn", "thirdeye:metric:1");
    this.properties.put("className", "myClassName");

    DetectionConfigDTO detector = new DetectionConfigDTO();
    detector.setProperties(this.properties);
    detector.setName("myName");
    detector.setCron("myCron");
    this.detectorId = this.detectionDAO.save(detector);

    this.runner = new DetectionPipelineTaskRunner(
        this.detectionDAO,
        this.anomalyDAO,
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
