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

import java.util.List;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.scheduler.DetectionCronScheduler;
import org.apache.pinot.thirdeye.scheduler.SubscriptionCronScheduler;
import org.apache.pinot.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter;
import org.apache.pinot.thirdeye.detection.alert.scheme.DetectionEmailAlerter;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleDetector;
import org.quartz.SchedulerException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.datalayer.DaoTestUtils.*;


/**
 * This tests the notification task scheduler.
 * The notification task scheduler should not schedule notification task if there is no anomaly generated.
 *
 */
public class NotificationTaskSchedulerTest {

  private DetectionCronScheduler detectionJobScheduler = null;
  private SubscriptionCronScheduler alertJobScheduler = null;
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
  private ApplicationManager appDAO;
  private DetectionPipelineLoader detectionPipelineLoader;
  private long detectionId;

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
    appDAO = daoRegistry.getApplicationDAO();
    detectionPipelineLoader = new DetectionPipelineLoader();
  }

  private void cleanup_schedulers() throws SchedulerException {
    if (detectionJobScheduler != null) {
      detectionJobScheduler.shutdown();
    }
    if (alertJobScheduler != null) {
      alertJobScheduler.shutdown();
    }
  }

  private void setup() throws Exception {
    // create test dataset config
    datasetDAO.save(getTestDatasetConfig(collection));
    metricDAO.save(getTestMetricConfig(collection, metric, null));

    ApplicationDTO app = new ApplicationDTO();
    app.setApplication("thirdeye");
    app.setRecipients("test@test");
    this.appDAO.save(app);

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(daoRegistry.getMetricConfigDAO(), datasetDAO, null, null);
    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    DataProvider provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, evaluationDAO,
        timeseriesLoader, aggregationLoader, detectionPipelineLoader, TimeSeriesCacheBuilder.getInstance(),
        AnomaliesCacheBuilder.getInstance());

    detectionId = daoRegistry.getDetectionConfigManager().save(DaoTestUtils.getTestDetectionConfig(provider, detectionConfigFile));
    // create test alert configuration
    daoRegistry.getDetectionAlertConfigManager().save(DaoTestUtils.getTestDetectionAlertConfig(alertConfigFile));
  }

  @Test
  public void testNotificationJobCreation() throws Exception {
    // setup test environment
    setup();

    // start detection scheduler
    startDetectionScheduler();

    // start alert scheduler
    startAlertScheduler();

    // check only detection task is created, but detection alert task is not created
    Thread.sleep(10000);
    List<TaskDTO> tasks = taskDAO.findAll();
    Assert.assertTrue(tasks.size() > 0);
    //Assert.assertTrue(tasks.stream().anyMatch(x -> x.getTaskType() == TaskConstants.TaskType.DETECTION));
    Assert.assertTrue(tasks.stream().noneMatch(x -> x.getTaskType() == TaskConstants.TaskType.DETECTION_ALERT));

    // generate an anomaly
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setDetectionConfigId(detectionId);
    anomaly.setStartTime(System.currentTimeMillis() - 1000);
    anomaly.setEndTime(System.currentTimeMillis());
    anomalyDAO.save(anomaly);

    // check the detection alert task is created
    Thread.sleep(10000);
    tasks = taskDAO.findAll();
    Assert.assertTrue(tasks.size() > 0);
    Assert.assertTrue(tasks.stream().anyMatch(x -> x.getTaskType() == TaskConstants.TaskType.DETECTION_ALERT));
  }

  private void startAlertScheduler() throws SchedulerException {
    alertJobScheduler = new SubscriptionCronScheduler();
    alertJobScheduler.start();
  }

  private void startDetectionScheduler() throws Exception {
    detectionJobScheduler = new DetectionCronScheduler(DAORegistry.getInstance().getDetectionConfigManager());
    detectionJobScheduler.start();
  }
}
