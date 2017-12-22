package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardExecutionContext;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardTaskContext;
import com.linkedin.thirdeye.anomaly.onboard.OnboardingTaskTestUtils;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestOnboardingTasks {
  private DAOTestBase daoTestBase;
  private DetectionOnboardTaskContext context;
  private DatasetConfigManager datasetConfigDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private AlertConfigManager alertConfigDAO;
  private JobManager jobDAO;

  @BeforeClass
  private void beforeClass(){
    daoTestBase = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    alertConfigDAO = daoRegistry.getAlertConfigDAO();
    jobDAO = daoRegistry.getJobDAO();
    context = OnboardingTaskTestUtils.getDetectionTaskContext();

    // Prepare for data
    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    datasetConfig.setDataset("test");
    datasetConfig.setTimeColumn("Date");
    datasetConfig.setTimeUnit(TimeUnit.DAYS);
    datasetConfig.setTimeDuration(1);
    datasetConfig.setTimeFormat("SIMPLE_DATE_FORMAT:yyyyMMdd");
    datasetConfig.setTimezone("US/Pacific");
    datasetConfigDAO.save(datasetConfig);
  }

  @AfterClass
  private void afterClass(){
    daoTestBase.cleanup();
  }

  @Test
  public void testDataPreparationOnboardingTask(){
    DetectionOnboardTask task = new DataPreparationOnboardingTask();
    task.setTaskContext(context);
    task.run();

    DetectionOnboardExecutionContext executionContext = context.getExecutionContext();
    Assert.assertNotNull(executionContext.getExecutionResult(DefaultDetectionOnboardJob.FUNCTION_FACTORY));
    Assert.assertNotNull(executionContext.getExecutionResult(DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY));
    Assert.assertNotNull(executionContext.getExecutionResult(DefaultDetectionOnboardJob.ALERT_FILTER_AUTOTUNE_FACTORY));
  }

  @Test(dependsOnMethods = {"testDataPreparationOnboardingTask"})
  public void testFunctionCreationOnboardingTask(){
    DetectionOnboardTask task = new FunctionCreationOnboardingTask();
    task.setTaskContext(context);
    task.run();

    Assert.assertEquals(1, anomalyFunctionDAO.findAll().size());
    Assert.assertEquals(1, alertConfigDAO.findAll().size());
  }

  @Test(dependsOnMethods = {"testFunctionCreationOnboardingTask"})
  public void testFunctionReplayOnboardingTask() throws Exception{
    FunctionReplayOnboardingTask task = new FunctionReplayOnboardingTask();
    task.setTaskContext(context);
    task.initDetectionJob();

    Assert.assertEquals(1, jobDAO.findAll().size());
  }

  @Test(dependsOnMethods = {"testFunctionReplayOnboardingTask"})
  public void testAlertFilterAutoTuneOnboardingTask() {
    final DetectionOnboardTask task = new AlertFilterAutoTuneOnboardingTask();
    task.setTaskContext(context);
    task.run();
  }
}
