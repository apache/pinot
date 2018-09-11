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

package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.linkedin.thirdeye.anomaly.onboard.OnboardingTaskTestUtils;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardExecutionContext;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardTaskContext;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
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
  private MetricConfigManager metricConfigDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private AlertConfigManager alertConfigDAO;
  private JobManager jobDAO;

  @BeforeClass
  public void beforeClass(){
    daoTestBase = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
    metricConfigDAO = daoRegistry.getMetricConfigDAO();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    alertConfigDAO = daoRegistry.getAlertConfigDAO();
    jobDAO = daoRegistry.getJobDAO();
    context = OnboardingTaskTestUtils.getDetectionTaskContext();
    initDataset();
    initMetric();
  }

  public void initDataset(){
    // Prepare for data
    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    datasetConfig.setDataset(OnboardingTaskTestUtils.TEST_COLLECTION);
    datasetConfig.setTimeColumn("Date");
    datasetConfig.setTimeUnit(TimeUnit.DAYS);
    datasetConfig.setTimeDuration(1);
    datasetConfig.setTimeFormat("SIMPLE_DATE_FORMAT:yyyyMMdd");
    datasetConfig.setTimezone("US/Pacific");
    datasetConfigDAO.save(datasetConfig);
    Assert.assertNotNull(datasetConfigDAO.findByDataset(OnboardingTaskTestUtils.TEST_COLLECTION));
  }

  public void initMetric(){
    // Prepare for data
    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setDataset(OnboardingTaskTestUtils.TEST_COLLECTION);
    metricConfigDTO.setName(OnboardingTaskTestUtils.TEST_METRIC);
    metricConfigDTO.setAlias(OnboardingTaskTestUtils.TEST_COLLECTION + "::" + OnboardingTaskTestUtils.TEST_METRIC);
    metricConfigDTO.setActive(true);
    metricConfigDTO.setDefaultAggFunction(MetricAggFunction.SUM);
    metricConfigDAO.save(metricConfigDTO);
    Assert.assertNotNull(metricConfigDAO.findByMetricName(OnboardingTaskTestUtils.TEST_METRIC));
  }

  @AfterClass(alwaysRun = true)
  public void afterClass(){
    daoTestBase.cleanup();
  }

  @Test
  public void testOnboardingTasks() throws Exception{
    AnomalyFunctionDTO dummyFunction = new AnomalyFunctionDTO();
    dummyFunction.setFunctionName(context.getConfiguration().getString(DefaultDetectionOnboardJob.FUNCTION_NAME));
    dummyFunction.setMetricId(-1);
    dummyFunction.setIsActive(false);
    anomalyFunctionDAO.save(dummyFunction);

    DetectionOnboardTask task = new DataPreparationOnboardingTask();
    task.setTaskContext(context);
    task.run();

    DetectionOnboardExecutionContext executionContext = context.getExecutionContext();
    Assert.assertNotNull(executionContext.getExecutionResult(DefaultDetectionOnboardJob.FUNCTION_FACTORY));
    Assert.assertNotNull(executionContext.getExecutionResult(DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY));
    Assert.assertNotNull(executionContext.getExecutionResult(DefaultDetectionOnboardJob.ALERT_FILTER_AUTOTUNE_FACTORY));

    task = new FunctionCreationOnboardingTask();
    task.setTaskContext(context);
    task.run();

    Assert.assertEquals(1, anomalyFunctionDAO.findAll().size());
    Assert.assertEquals(1, alertConfigDAO.findAll().size());

    FunctionReplayOnboardingTask DetectionOnboardTask = new FunctionReplayOnboardingTask();
    DetectionOnboardTask.setTaskContext(context);
    DetectionOnboardTask.initDetectionJob();

    Assert.assertEquals(1, jobDAO.findAll().size());

    task = new AlertFilterAutoTuneOnboardingTask();
    task.setTaskContext(context);
    task.run();
  }
}
