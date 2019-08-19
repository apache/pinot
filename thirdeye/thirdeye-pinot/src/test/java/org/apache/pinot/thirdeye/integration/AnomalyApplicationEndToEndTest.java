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

package org.apache.pinot.thirdeye.integration;

import com.google.common.cache.LoadingCache;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.classification.ClassificationJobScheduler;
import org.apache.pinot.thirdeye.anomaly.classification.classifier.AnomalyClassifierFactory;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorConfiguration;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorConstants;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorJobScheduler;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import org.apache.pinot.thirdeye.anomaly.task.TaskDriver;
import org.apache.pinot.thirdeye.anomaly.task.TaskDriverConfiguration;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.completeness.checker.DataCompletenessScheduler;
import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.RelationalThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.DetectionPipelineScheduler;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertScheduler;
import org.apache.pinot.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter;
import org.apache.pinot.thirdeye.detection.alert.scheme.DetectionEmailAlerter;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleDetector;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.datalayer.DaoTestUtils.*;


/**
 * End-to-end integration test for ThirdEye application.
 *
 */
public class AnomalyApplicationEndToEndTest {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyApplicationEndToEndTest.class);

  private DetectionPipelineScheduler detectionJobScheduler = null;
  private TaskDriver taskDriver = null;
  private MonitorJobScheduler monitorJobScheduler = null;
  private DetectionAlertScheduler alertJobScheduler = null;
  private DataCompletenessScheduler dataCompletenessScheduler = null;
  private ClassificationJobScheduler classificationJobScheduler = null;
  private AnomalyFunctionFactory anomalyFunctionFactory = null;
  private AlertFilterFactory alertFilterFactory = null;
  private AnomalyClassifierFactory anomalyClassifierFactory = null;
  private ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();
  private ThirdEyeAnomalyConfiguration thirdeyeAnomalyConfig;

  private int id = 0;
  private String dashboardHost = "http://localhost:8080/dashboard";
  private String detectionConfigFile = "/sample-detection-config.yml";
  private String alertConfigFile = "/sample-alert-config.yml";
  private String metric = "cost";
  private String collection = "test-collection";
  private DAOTestBase testDAOProvider = null;
  private DAORegistry daoRegistry = null;
  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private EventManager eventDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private TaskManager taskDAO;
  private EvaluationManager evaluationDAO;
  private DetectionPipelineLoader detectionPipelineLoader;

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    daoRegistry = DAORegistry.getInstance();
    Assert.assertNotNull(daoRegistry.getJobDAO());
    initDao();
    initRegistries();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() throws Exception {
    cleanup_schedulers();
    testDAOProvider.cleanup();
  }

  void initRegistries() {
    DetectionRegistry.registerComponent(ThresholdRuleDetector.class.getName(), "THRESHOLD");
    DetectionAlertRegistry.getInstance().registerAlertScheme("EMAIL", DetectionEmailAlerter.class.getName());
    DetectionAlertRegistry.getInstance().registerAlertFilter("DEFAULT_ALERTER_PIPELINE",
        ToAllRecipientsDetectionAlertFilter.class.getName() );
  }

  void initDao() {
    daoRegistry = DAORegistry.getInstance();
    metricDAO = daoRegistry.getMetricConfigDAO();
    datasetDAO = daoRegistry.getDatasetConfigDAO();
    eventDAO = daoRegistry.getEventDAO();
    taskDAO = daoRegistry.getTaskDAO();
    anomalyDAO = daoRegistry.getMergedAnomalyResultDAO();
    evaluationDAO = daoRegistry.getEvaluationManager();
    detectionPipelineLoader = new DetectionPipelineLoader();
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
  }

  private void setup() throws Exception {

    // Mock query cache
    ThirdEyeDataSource mockThirdeyeDataSource = Mockito.mock(ThirdEyeDataSource.class);
    Mockito.when(mockThirdeyeDataSource.execute(Matchers.any(ThirdEyeRequest.class)))
    .thenAnswer((Answer<ThirdEyeResponse>) invocation -> {
      Object[] args = invocation.getArguments();
      ThirdEyeRequest request = (ThirdEyeRequest) args[0];
      ThirdEyeResponse response = getMockResponse(request);
      return response;
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

    // create test dataset config
    datasetDAO.save(getTestDatasetConfig(collection));
    metricDAO.save(getTestMetricConfig(collection, metric, null));

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(daoRegistry.getMetricConfigDAO(), datasetDAO, mockQueryCache);
    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    DataProvider provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, evaluationDAO,
        timeseriesLoader, aggregationLoader, detectionPipelineLoader);

    daoRegistry.getDetectionConfigManager().save(DaoTestUtils.getTestDetectionConfig(provider, detectionConfigFile));
    // create test alert configuration
    daoRegistry.getDetectionAlertConfigManager()
        .save(DaoTestUtils.getTestDetectionAlertConfig(daoRegistry.getDetectionConfigManager(), alertConfigFile));
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
    response = new RelationalThirdEyeResponse(request, rows, dataTimeSpec);
    return response;
  }

  @Test
  public void testThirdeyeAnomalyApplication() throws Exception {
    // setup caches and config
    setup();

    // start detection scheduler
    startDetectionScheduler();

    // start alert scheduler
    startAlertScheduler();

    // start task drivers
    startWorker();

    // check for number of entries in tasks and jobs
    Thread.sleep(10000);
    List<TaskDTO> tasks = taskDAO.findAll();
    Assert.assertTrue(tasks.size() > 0);
    Assert.assertTrue(tasks.stream().anyMatch(x -> x.getTaskType() == TaskConstants.TaskType.DETECTION));
    Assert.assertTrue(tasks.stream().anyMatch(x -> x.getTaskType() == TaskConstants.TaskType.DETECTION_ALERT));

    // check for change in task status to COMPLETED
    Thread.sleep(30000);
    tasks = taskDAO.findAll();
    Assert.assertTrue(tasks.stream().anyMatch(x -> x.getStatus() == TaskStatus.COMPLETED));

    // check merged anomalies
    List<MergedAnomalyResultDTO> mergedAnomalies = daoRegistry.getMergedAnomalyResultDAO().findAll();
    Assert.assertTrue(mergedAnomalies.size() > 0);

    // start monitor
    startMonitor();

    // check for monitor tasks
    Thread.sleep(5000);
    tasks = taskDAO.findAll();
    Assert.assertTrue(tasks.stream().anyMatch(x -> x.getTaskType() == TaskConstants.TaskType.MONITOR));
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
    alertJobScheduler = new DetectionAlertScheduler();
    alertJobScheduler.start();
  }


  private void startDetectionScheduler() throws Exception {
    detectionJobScheduler = new DetectionPipelineScheduler(DAORegistry.getInstance().getDetectionConfigManager());
    detectionJobScheduler.start();
  }
}
