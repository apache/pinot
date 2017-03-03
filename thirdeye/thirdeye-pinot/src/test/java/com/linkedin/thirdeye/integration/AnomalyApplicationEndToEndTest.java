package com.linkedin.thirdeye.integration;

import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.AlertJobScheduler;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeExecutor;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConfiguration;
import com.linkedin.thirdeye.anomaly.monitor.MonitorJobScheduler;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.anomaly.task.TaskDriver;
import com.linkedin.thirdeye.anomaly.task.TaskInfoFactory;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.cache.MetricDataset;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.pinot.PinotQuery;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeResponse;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessType;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessScheduler;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessTaskInfo;
import com.linkedin.thirdeye.datalayer.bao.AbstractManagerTestBase;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AnomalyApplicationEndToEndTest extends AbstractManagerTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyApplicationEndToEndTest.class);
  private DetectionJobScheduler detectionJobScheduler = null;
  private TaskDriver taskDriver = null;
  private MonitorJobScheduler monitorJobScheduler = null;
  private AlertJobScheduler alertJobScheduler = null;
  private AnomalyMergeExecutor anomalyMergeExecutor = null;
  private DataCompletenessScheduler dataCompletenessScheduler = null;
  private AnomalyFunctionFactory anomalyFunctionFactory = null;
  private AlertFilterFactory alertFilterFactory = null;
  private ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();
  private ThirdEyeAnomalyConfiguration thirdeyeAnomalyConfig;
  private List<TaskDTO> tasks;
  private List<JobDTO> jobs;
  private long functionId;

  private int id = 0;
  private String dashboardHost = "http://localhost:8080/dashboard";
  private String functionPropertiesFile = "/sample-functions.properties";
  private String alertFilterPropertiesFile = "/sample-alertfilter.properties";
  private String metric = "cost";
  private String collection = "test-collection";

  @BeforeClass
  void beforeClass() {
    super.init();
    Assert.assertNotNull(daoRegistry.getJobDAO());

  }

  @AfterClass(alwaysRun = true)
  void afterClass() throws Exception {
    cleanup_schedulers();
    super.cleanup();
  }

  /**
   * This test class needs to use a class that accesses the global singleton DAO registry. Therefore,
   * we need to copy the local DAO registry, which is created for this test, to the global one.
   * Otherwise, the global singleton one may reference to an arbitrary DB that is created for this
   * test.
   */
  @BeforeMethod
  void beforeMethod(){
    DAORegistry.overrideSingletonDAORegistryForTesting(testDBResources.getTestDaoRegistry());
  }

  void cleanup_schedulers() throws SchedulerException {
    if (detectionJobScheduler != null) {
      detectionJobScheduler.shutdown();
    }
    if (alertJobScheduler != null) {
      alertJobScheduler.shutdown();
    }
    if (monitorJobScheduler != null) {
      monitorJobScheduler.stop();
    }
    if (anomalyMergeExecutor != null) {
      anomalyMergeExecutor.stop();
    }
    if (taskDriver != null) {
      taskDriver.stop();
    }
    if (dataCompletenessScheduler != null) {
      dataCompletenessScheduler.shutdown();
    }
  }

  private void setup() throws Exception {

    // Mock query cache
    ThirdEyeClient mockThirdeyeClient = Mockito.mock(ThirdEyeClient.class);
    Mockito.when(mockThirdeyeClient.execute(Matchers.any(ThirdEyeRequest.class)))
    .thenAnswer(new Answer<ThirdEyeResponse>() {

      @Override
      public ThirdEyeResponse answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        ThirdEyeRequest request = (ThirdEyeRequest) args[0];
        ThirdEyeResponse response = getMockResponse(request);
        return response;
      }
    });

    QueryCache mockQueryCache = new QueryCache(mockThirdeyeClient, Executors.newFixedThreadPool(10));
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

    ResultSet mockResultSet = Mockito.mock(ResultSet.class);
    Mockito.when(mockResultSet.getRowCount()).thenReturn(0);
    ResultSetGroup mockResultSetGroup = Mockito.mock(ResultSetGroup.class);
    Mockito.when(mockResultSetGroup.getResultSet(0)).thenReturn(mockResultSet);
    LoadingCache<PinotQuery, ResultSetGroup> mockResultSetGroupCache = Mockito.mock(LoadingCache.class);
    Mockito.when(mockResultSetGroupCache.get(Matchers.any(PinotQuery.class)))
    .thenAnswer(new Answer<ResultSetGroup>() {

      @Override
      public ResultSetGroup answer(InvocationOnMock invocation) throws Throwable {
        return mockResultSetGroup;
      }
    });
    cacheRegistry.registerResultSetGroupCache(mockResultSetGroupCache);

    // Application config
    thirdeyeAnomalyConfig = new ThirdEyeAnomalyConfiguration();
    thirdeyeAnomalyConfig.setId(id);
    thirdeyeAnomalyConfig.setDashboardHost(dashboardHost);
    MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
    monitorConfiguration.setMonitorFrequency(new TimeGranularity(30, TimeUnit.SECONDS));
    thirdeyeAnomalyConfig.setMonitorConfiguration(monitorConfiguration);
    thirdeyeAnomalyConfig.setRootDir(System.getProperty("dw.rootDir", "NOT_SET(dw.rootDir)"));


    // create test anomaly function
    functionId = anomalyFunctionDAO.save(getTestFunctionSpec(metric, collection));

    // create test email configuration
    emailConfigurationDAO.save(getTestEmailConfiguration(metric, collection));

    // create test alert configuration
    alertConfigDAO.save(getTestAlertConfiguration("test alert v2"));

    // create test dataset config
    datasetConfigDAO.save(getTestDatasetConfig(collection));

    // setup function factory for worker and merger
    InputStream factoryStream = AnomalyApplicationEndToEndTest.class.getResourceAsStream(functionPropertiesFile);
    anomalyFunctionFactory = new AnomalyFunctionFactory(factoryStream);

    // setup alertfilter factory for worker
    InputStream alertFilterStream = AnomalyApplicationEndToEndTest.class.getResourceAsStream(alertFilterPropertiesFile);
    alertFilterFactory = new AlertFilterFactory(alertFilterStream);
  }


  private ThirdEyeResponse getMockResponse(ThirdEyeRequest request) {
    ThirdEyeResponse response = null;
    Random rand = new Random();
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(collection);
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

    // startDataCompletenessChecker
    startDataCompletenessScheduler();
    Thread.sleep(10000);
    int jobSizeDataCompleteness = jobDAO.findAll().size();
    int taskSizeDataCompleteness = taskDAO.findAll().size();
    Assert.assertTrue(jobSizeDataCompleteness == 1);
    Assert.assertTrue(taskSizeDataCompleteness == 2);
    JobDTO jobDTO = jobDAO.findAll().get(0);
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
    int jobSize1 = jobDAO.findAll().size();
    int taskSize1 = taskDAO.findAll().size();
    Assert.assertTrue(jobSize1 > 0);
    Assert.assertTrue(taskSize1 > 0);
    Thread.sleep(10000);
    int jobSize2 = jobDAO.findAll().size();
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
      } else if (task.getTaskType().equals(TaskType.ALERT)) {
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
    Assert.assertEquals(monitorCount, 2);

    // check for job status
    jobs = jobDAO.findAll();
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

    // Raw anomalies of the same function and dimensions should have been merged by the worker, so we
    // check if any raw anomalies present, whose existence means the worker fails the synchronous merge.
    List<RawAnomalyResultDTO> rawAnomalies = rawAnomalyResultDAO.findUnmergedByFunctionId(functionId);
    Assert.assertTrue(rawAnomalies.size() == 0);

    // start merge
    // startMerger();

    // check merged anomalies
    // Thread.sleep(4000);
    List<MergedAnomalyResultDTO> mergedAnomalies = mergedAnomalyResultDAO.findByFunctionId(functionId);
    Assert.assertTrue(mergedAnomalies.size() > 0);

    // check for job status COMPLETED
    jobs = jobDAO.findAll();
    int completedJobCount = 0;
    for (JobDTO job : jobs) {
      int attempt = 0;
      while (attempt < 3 && !job.getStatus().equals(JobStatus.COMPLETED)) {
        LOG.info("Checking job status with attempt : {}", attempt + 1);
        Thread.sleep(5_000);
        attempt++;
      }
      if (job.getStatus().equals(JobStatus.COMPLETED)) {
        completedJobCount ++;
      }
    }
    Assert.assertTrue(completedJobCount > 0);
    // stop schedulers
    cleanup();
  }

  private void startDataCompletenessScheduler() throws Exception {
    dataCompletenessScheduler = new DataCompletenessScheduler();
    dataCompletenessScheduler.start();
  }

  private void startMerger() throws Exception {
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    anomalyMergeExecutor = new AnomalyMergeExecutor(executorService, anomalyFunctionFactory);
    executorService.scheduleWithFixedDelay(anomalyMergeExecutor, 0, 3, TimeUnit.SECONDS);
  }

  private void startMonitor() {
    monitorJobScheduler = new MonitorJobScheduler(thirdeyeAnomalyConfig.getMonitorConfiguration());
    monitorJobScheduler.start();
  }


  private void startWorker() throws Exception {
    taskDriver = new TaskDriver(thirdeyeAnomalyConfig, anomalyFunctionFactory, alertFilterFactory);
    taskDriver.start();
  }


  private void startAlertScheduler() throws SchedulerException {
    alertJobScheduler = new AlertJobScheduler();
    alertJobScheduler.start();
  }


  private void startDetectionScheduler() throws SchedulerException {
    detectionJobScheduler = new DetectionJobScheduler();
    detectionJobScheduler.start();
  }

}
