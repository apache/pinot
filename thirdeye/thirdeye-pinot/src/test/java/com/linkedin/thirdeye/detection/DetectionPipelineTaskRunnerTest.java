package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DetectionPipelineTaskRunnerTest {
  private List<MockDetectionPipeline> runs;

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

    this.testDAOProvider = DAOTestBase.getInstance();
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.loader = new MockDetectionPipelineLoader(this.runs);
    this.provider = new MockDataProvider();

    this.properties = new HashMap<>();
    this.properties.put("metricUrn", "thirdeye:metric:1");

    DetectionConfigDTO detector = new DetectionConfigDTO();
    detector.setProperties(this.properties);
    detector.setName("myName");
    detector.setClassName("myClassName");
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

  @AfterMethod
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
    Assert.assertEquals(this.runs.get(0).getConfig().getClassName(), "myClassName");
    Assert.assertEquals(this.runs.get(0).getConfig().getCron(), "myCron");
  }

  @Test
  public void testTaskRunnerPersistence() throws Exception {
    Assert.fail("not implemented yet");
  }
}
