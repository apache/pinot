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

package com.linkedin.thirdeye.integration;

import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.v2.AlertJobSchedulerV2;
import com.linkedin.thirdeye.anomaly.classification.classifier.AnomalyClassifierFactory;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomaly.classification.ClassificationJobScheduler;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConfiguration;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConstants;
import com.linkedin.thirdeye.anomaly.monitor.MonitorJobScheduler;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.anomaly.task.TaskDriver;
import com.linkedin.thirdeye.anomaly.task.TaskDriverConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskInfoFactory;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessType;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessScheduler;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessTaskInfo;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.cache.MetricDataset;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeResponse;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.datalayer.DaoTestUtils.*;


public class AnomalyApplicationEndToEndTest {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyApplicationEndToEndTest.class);
  private DetectionJobScheduler detectionJobScheduler = null;
  private TaskDriver taskDriver = null;
  private MonitorJobScheduler monitorJobScheduler = null;
  private AlertJobSchedulerV2 alertJobScheduler = null;
  private DataCompletenessScheduler dataCompletenessScheduler = null;
  private ClassificationJobScheduler classificationJobScheduler = null;
  private AnomalyFunctionFactory anomalyFunctionFactory = null;
  private AlertFilterFactory alertFilterFactory = null;
  private AnomalyClassifierFactory anomalyClassifierFactory = null;
  private ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();
  private ThirdEyeAnomalyConfiguration thirdeyeAnomalyConfig;
  private List<TaskDTO> tasks;
  private List<JobDTO> jobs;
  private long functionId;
  private long classificationConfigId;

  private int id = 0;
  private String dashboardHost = "http://localhost:8080/dashboard";
  private String functionPropertiesFile = "/sample-functions.properties";
  private String alertFilterPropertiesFile = "/sample-alertfilter.properties";
  private String classifierPropertiesFile = "/sample-classifier.properties";
  private String metric = "cost";
  private String collection = "test-collection";
  private DAOTestBase testDAOProvider = null;
  private DAORegistry daoRegistry = null;

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    daoRegistry = DAORegistry.getInstance();
    Assert.assertNotNull(daoRegistry.getJobDAO());
  }

  @AfterClass(alwaysRun = true)
  void afterClass() throws Exception {
    cleanup_schedulers();
    testDAOProvider.cleanup();
  }

  private void cleanup_schedulers() throws SchedulerException {
    if (detectionJobScheduler != null) {
      detectionJobScheduler.shutdown();
    }
    if (alertJobScheduler != null) {
      alertJobScheduler.shutdown();
    }
    if (monitorJobScheduler != null) {
      monitorJobScheduler.shutdown();
    }
    if (taskDriver != null) {
      taskDriver.shutdown();
    }
    if (dataCompletenessScheduler != null) {
      dataCompletenessScheduler.shutdown();
    }
    if (classificationJobScheduler != null) {
      classificationJobScheduler.shutdown();
    }
  }

  private void setup() throws Exception {

    // Mock query cache
    ThirdEyeDataSource mockThirdeyeDataSource = Mockito.mock(ThirdEyeDataSource.class);
    Mockito.when(mockThirdeyeDataSource.execute(Matchers.any(ThirdEyeRequest.class)))
    .thenAnswer(new Answer<ThirdEyeResponse>() {

      @Override
      public ThirdEyeResponse answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        ThirdEyeRequest request = (ThirdEyeRequest) args[0];
        ThirdEyeResponse response = getMockResponse(request);
        return response;
      }
    });
    Map<String, ThirdEyeDataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put(PinotThirdEyeDataSource.class.getSimpleName(), mockThirdeyeDataSource);

    QueryCache mockQueryCache = new QueryCache(dataSourceMap, Executors.newFixedThreadPool(10));
    cacheRegistry.registerQueryCache(mockQueryCache);

    MetricConfigDTO metricConfig = getTestMetricConfig(collection, metric, 1L);
    // create metric config in cache
    LoadingCache<MetricDataset, MetricConfigDTO> mockMetricConfigCache = Mockito.mock(LoadingCache.class);
    Mockito.when(mockMetricConfigCache.get(new MetricDataset(metric, collection))).thenReturn(metricConfig);
    cacheRegistry.registerMetricConfigCache(mockMetricConfigCache);
    // create dataset config in cache
    LoadingCache<String, DatasetConfigDTO> mockDatasetConfigCache = Mockito.mock(LoadingCache.class);
    Mockito.when(mockDatasetConfigCache.get(collection)).thenReturn(getTestDatasetConfig(collection));
    cacheRegistry.registerDatasetConfigCache(mockDatasetConfigCache);

    // Application config
    thirdeyeAnomalyConfig = new ThirdEyeAnomalyConfiguration();
    thirdeyeAnomalyConfig.setId(id);
    thirdeyeAnomalyConfig.setDashboardHost(dashboardHost);
    // Monitor config
    MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
    monitorConfiguration.setDefaultRetentionDays(MonitorConstants.DEFAULT_RETENTION_DAYS);
    monitorConfiguration.setCompletedJobRetentionDays(MonitorConstants.DEFAULT_COMPLETED_JOB_RETENTION_DAYS);
    monitorConfiguration.setDetectionStatusRetentionDays(MonitorConstants.DEFAULT_DETECTION_STATUS_RETENTION_DAYS);
    monitorConfiguration.setRawAnomalyRetentionDays(MonitorConstants.DEFAULT_RAW_ANOMALY_RETENTION_DAYS);
    monitorConfiguration.setMonitorFrequency(new TimeGranularity(3, TimeUnit.SECONDS));
    thirdeyeAnomalyConfig.setMonitorConfiguration(monitorConfiguration);
    // Task config
    TaskDriverConfiguration taskDriverConfiguration = new TaskDriverConfiguration();
    taskDriverConfiguration.setNoTaskDelayInMillis(1000);
    taskDriverConfiguration.setRandomDelayCapInMillis(200);
    taskDriverConfiguration.setTaskFailureDelayInMillis(500);
    taskDriverConfiguration.setMaxParallelTasks(2);
    thirdeyeAnomalyConfig.setTaskDriverConfiguration(taskDriverConfiguration);
    thirdeyeAnomalyConfig.setRootDir(System.getProperty("dw.rootDir", "NOT_SET(dw.rootDir)"));


    // create test anomaly function
    functionId = daoRegistry.getAnomalyFunctionDAO().save(DaoTestUtils.getTestFunctionSpec(metric, collection));

    // create test alert configuration
    daoRegistry.getAlertConfigDAO().save(getTestAlertConfiguration("test alert v2"));

    // create test dataset config
    daoRegistry.getDatasetConfigDAO().save(getTestDatasetConfig(collection));

    // create test grouping config
    classificationConfigId =
        daoRegistry.getClassificationConfigDAO().save(getTestGroupingConfiguration(Collections.singletonList(functionId)));

    // setup function factory for worker and merger
    InputStream factoryStream = AnomalyApplicationEndToEndTest.class.getResourceAsStream(functionPropertiesFile);
    anomalyFunctionFactory = new AnomalyFunctionFactory(factoryStream);

    // setup alert filter factory for worker
    InputStream alertFilterStream = AnomalyApplicationEndToEndTest.class.getResourceAsStream(alertFilterPropertiesFile);
    alertFilterFactory = new AlertFilterFactory(alertFilterStream);

    // setup classifier factory for worker
    InputStream classifierStream = AnomalyApplicationEndToEndTest.class.getResourceAsStream(classifierPropertiesFile);
    anomalyClassifierFactory = new AnomalyClassifierFactory(classifierStream);
  }


  private ThirdEyeResponse getMockResponse(ThirdEyeRequest request) {
    ThirdEyeResponse response = null;
    Random rand = new Random();
    DatasetConfigDTO datasetConfig = daoRegistry.getDatasetConfigDAO().findByDataset(collection);
    TimeSpec dataTimeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    List<String[]> rows = new ArrayList<>();
    DateTime start = request.getStartTimeInclusive();
    DateTime end = request.getEndTimeExclusive();
    List<String> metrics = request.getMetricNames();
    int bucket = 0;
    while (start.isBefore(end)) {
      String[] row = new String[metrics.size() + 1];
      row[0] = String.valueOf(bucket);
      bucket++;
      for (int i = 0; i < metrics.size(); i++) {
        row[i+1] = String.valueOf(rand.nextInt(1000));
      }
      rows.add(row);
      start = start.plusHours(1);
    }
    response = new PinotThirdEyeResponse(request, rows, dataTimeSpec);
    return response;
  }

  @Test(enabled=true)
  public void testThirdeyeAnomalyApplication() throws Exception {

    Assert.assertNotNull(daoRegistry.getJobDAO());

    // setup caches and config
    setup();

    Assert.assertNotNull(daoRegistry.getJobDAO());

    TaskManager taskDAO = daoRegistry.getTaskDAO();

    // startDataCompletenessChecker
    startDataCompletenessScheduler();
    Thread.sleep(10000);
    int jobSizeDataCompleteness = daoRegistry.getJobDAO().findAll().size();
    int taskSizeDataCompleteness = taskDAO.findAll().size();
    Assert.assertTrue(jobSizeDataCompleteness == 1);
    Assert.assertTrue(taskSizeDataCompleteness == 2);
    JobDTO jobDTO = daoRegistry.getJobDAO().findAll().get(0);
    Assert.assertTrue(jobDTO.getJobName().startsWith(TaskType.DATA_COMPLETENESS.toString()));
    List<TaskDTO> taskDTOs = taskDAO.findAll();
    for (TaskDTO taskDTO : taskDTOs) {
      Assert.assertEquals(taskDTO.getTaskType(), TaskType.DATA_COMPLETENESS);
      Assert.assertEquals(taskDTO.getStatus(), TaskStatus.WAITING);
      DataCompletenessTaskInfo taskInfo = (DataCompletenessTaskInfo) TaskInfoFactory.
          getTaskInfoFromTaskType(taskDTO.getTaskType(), taskDTO.getTaskInfo());
      Assert.assertTrue((taskInfo.getDataCompletenessType() == DataCompletenessType.CHECKER)
          || (taskInfo.getDataCompletenessType() == DataCompletenessType.CLEANUP));
    }

    // start detection scheduler
    startDetectionScheduler();

    // start alert scheduler
    startAlertScheduler();

    // check for number of entries in tasks and jobs
    Thread.sleep(10000);
    int jobSize1 = daoRegistry.getJobDAO().findAll().size();
    int taskSize1 = taskDAO.findAll().size();
    Assert.assertTrue(jobSize1 > 0);
    Assert.assertTrue(taskSize1 > 0);
    Thread.sleep(10000);
    int jobSize2 = daoRegistry.getJobDAO().findAll().size();
    int taskSize2 = taskDAO.findAll().size();
    Assert.assertTrue(jobSize2 > jobSize1);
    Assert.assertTrue(taskSize2 > taskSize1);

    tasks = taskDAO.findAll();

    // check for task type
    int detectionCount = 0;
    int alertCount = 0;
    for (TaskDTO task : tasks) {
      if (task.getTaskType().equals(TaskType.ANOMALY_DETECTION)) {
        detectionCount ++;
      } else if (task.getTaskType().equals(TaskType.ALERT2)) {
        alertCount ++;
      }
    }
    Assert.assertTrue(detectionCount > 0);
    Assert.assertTrue(alertCount > 0);

    // check for task status
    tasks = taskDAO.findAll();
    for (TaskDTO task : tasks) {
      Assert.assertEquals(task.getStatus(), TaskStatus.WAITING);
    }

    // start monitor
    startMonitor();

    // check for monitor tasks
    Thread.sleep(5000);
    tasks = taskDAO.findAll();
    int monitorCount = 0;
    for (TaskDTO task : tasks) {
      if (task.getTaskType().equals(TaskType.MONITOR)) {
        monitorCount++;
      }
    }
    Assert.assertTrue(monitorCount > 0);

    // check for job status
    jobs = daoRegistry.getJobDAO().findAll();
    for (JobDTO job : jobs) {
      Assert.assertEquals(job.getStatus(), JobStatus.SCHEDULED);
    }

    // start task drivers
    startWorker();

    // check for change in task status to COMPLETED
    Thread.sleep(30000);
    tasks = taskDAO.findAll();
    int completedCount = 0;
    for (TaskDTO task : tasks) {
      if (task.getStatus().equals(TaskStatus.COMPLETED)) {
        completedCount++;
      }
    }
    Assert.assertTrue(completedCount > 0);

    // check merged anomalies
    List<MergedAnomalyResultDTO> mergedAnomalies = daoRegistry.getMergedAnomalyResultDAO().findByFunctionId(functionId);
    Assert.assertTrue(mergedAnomalies.size() > 0);
    AnomalyFunctionDTO functionSpec = daoRegistry.getAnomalyFunctionDAO().findById(functionId);
    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      Assert.assertEquals(mergedAnomaly.getFunction(), functionSpec);
      Assert.assertEquals(mergedAnomaly.getCollection(), functionSpec.getCollection());
      Assert.assertEquals(mergedAnomaly.getMetric(), functionSpec.getTopicMetric());
      Assert.assertNotNull(mergedAnomaly.getAnomalyResultSource());
      Assert.assertNotNull(mergedAnomaly.getDimensions());
      Assert.assertNotNull(mergedAnomaly.getProperties());
      Assert.assertNull(mergedAnomaly.getFeedback());
    }

    // THE FOLLOWING TEST MAY FAIL OCCASIONALLY DUE TO MACHINE COMPUTATION POWER
    // check for job status COMPLETED
    jobs = daoRegistry.getJobDAO().findAll();
    int completedJobCount = 0;
    for (JobDTO job : jobs) {
      int attempt = 0;
      while (attempt < 3 && !job.getStatus().equals(JobStatus.COMPLETED)) {
        LOG.info("Checking job status with attempt : {}", attempt + 1);
        Thread.sleep(5_000);
        attempt++;
      }
      if (job.getStatus().equals(JobStatus.COMPLETED)) {
        completedJobCount++;
        break;
      }
    }
    Assert.assertTrue(completedJobCount > 0);

    // start classifier
    startClassifier();
    List<JobDTO> latestCompletedDetectionJobDTO =
        daoRegistry.getJobDAO().findRecentScheduledJobByTypeAndConfigId(TaskType.ANOMALY_DETECTION, functionId, 0L);
    Assert.assertNotNull(latestCompletedDetectionJobDTO);
    Assert.assertEquals(latestCompletedDetectionJobDTO.get(0).getStatus(), JobStatus.COMPLETED);
    Thread.sleep(5000);
    jobs = daoRegistry.getJobDAO().findAll();
    List<JobDTO> latestCompletedClassificationJobDTO =
        daoRegistry.getJobDAO().findRecentScheduledJobByTypeAndConfigId(TaskType.CLASSIFICATION, classificationConfigId, 0L);
    Assert.assertNotNull(latestCompletedClassificationJobDTO);
    Assert.assertEquals(latestCompletedClassificationJobDTO.get(0).getStatus(), JobStatus.COMPLETED);
  }

  private void startDataCompletenessScheduler() throws Exception {
    dataCompletenessScheduler = new DataCompletenessScheduler();
    dataCompletenessScheduler.start();
  }

  private void startClassifier() {
    classificationJobScheduler = new ClassificationJobScheduler();
    classificationJobScheduler.start();
  }

  private void startMonitor() {
    monitorJobScheduler = new MonitorJobScheduler(thirdeyeAnomalyConfig.getMonitorConfiguration());
    monitorJobScheduler.start();
  }


  private void startWorker() throws Exception {
    taskDriver =
        new TaskDriver(thirdeyeAnomalyConfig, anomalyFunctionFactory, alertFilterFactory, anomalyClassifierFactory);
    taskDriver.start();
  }

  private void startAlertScheduler() throws SchedulerException {
    alertJobScheduler = new AlertJobSchedulerV2();
    alertJobScheduler.start();
  }


  private void startDetectionScheduler() throws SchedulerException {
    detectionJobScheduler = new DetectionJobScheduler();
    detectionJobScheduler.start();
  }
}
