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

package org.apache.pinot.thirdeye.notification.content.templates;

import java.util.Properties;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleDetector;
import org.apache.pinot.thirdeye.notification.commons.EmailEntity;
import org.apache.pinot.thirdeye.notification.formatter.channels.EmailContentFormatter;
import org.apache.pinot.thirdeye.notification.ContentFormatterUtils;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorConfiguration;
import org.apache.pinot.thirdeye.anomaly.task.TaskDriverConfiguration;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.datalayer.DaoTestUtils.*;
import static org.apache.pinot.thirdeye.notification.commons.SmtpConfiguration.*;


public class TestMetricAnomaliesContent {
  private static final String TEST = "test";
  private int id = 0;
  private String dashboardHost = "http://localhost:8080/dashboard";
  private String detectionConfigFile = "/sample-detection-config.yml";
  private DAOTestBase testDAOProvider;
  private DetectionConfigManager detectionConfigDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private EventManager eventDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private TaskManager taskDAO;
  private EvaluationManager evaluationDAO;
  private DetectionPipelineLoader detectionPipelineLoader;
  private DataProvider provider;

  @BeforeClass
  public void beforeClass(){
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    detectionConfigDAO = daoRegistry.getDetectionConfigManager();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
    metricDAO = daoRegistry.getMetricConfigDAO();
    datasetDAO = daoRegistry.getDatasetConfigDAO();
    eventDAO = daoRegistry.getEventDAO();
    taskDAO = daoRegistry.getTaskDAO();
    anomalyDAO = daoRegistry.getMergedAnomalyResultDAO();
    evaluationDAO = daoRegistry.getEvaluationManager();
    detectionPipelineLoader = new DetectionPipelineLoader();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(daoRegistry.getMetricConfigDAO(), datasetDAO, null, null);
    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, evaluationDAO,
        timeseriesLoader, aggregationLoader, detectionPipelineLoader);
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testGetEmailEntity() throws Exception {
    DetectionRegistry.registerComponent(ThresholdRuleDetector.class.getName(), "THRESHOLD");

    DateTimeZone dateTimeZone = DateTimeZone.forID("America/Los_Angeles");
    ThirdEyeAnomalyConfiguration thirdeyeAnomalyConfig = new ThirdEyeAnomalyConfiguration();
    thirdeyeAnomalyConfig.setId(id);
    thirdeyeAnomalyConfig.setDashboardHost(dashboardHost);
    MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
    monitorConfiguration.setMonitorFrequency(new TimeGranularity(3, TimeUnit.SECONDS));
    thirdeyeAnomalyConfig.setMonitorConfiguration(monitorConfiguration);
    TaskDriverConfiguration taskDriverConfiguration = new TaskDriverConfiguration();
    taskDriverConfiguration.setNoTaskDelayInMillis(1000);
    taskDriverConfiguration.setRandomDelayCapInMillis(200);
    taskDriverConfiguration.setTaskFailureDelayInMillis(500);
    taskDriverConfiguration.setMaxParallelTasks(2);
    thirdeyeAnomalyConfig.setTaskDriverConfiguration(taskDriverConfiguration);
    thirdeyeAnomalyConfig.setRootDir(System.getProperty("dw.rootDir", "NOT_SET(dw.rootDir)"));
    Map<String, Map<String, Object>> alerters = new HashMap<>();
    Map<String, Object> smtpProps = new HashMap<>();
    smtpProps.put(SMTP_HOST_KEY, "host");
    smtpProps.put(SMTP_PORT_KEY, "9000");
    alerters.put("smtpConfiguration", smtpProps);
    thirdeyeAnomalyConfig.setAlerterConfiguration(alerters);

    // create test dataset config
    datasetDAO.save(getTestDatasetConfig("test-collection"));
    metricDAO.save(getTestMetricConfig("test-collection", "cost", null));

    List<AnomalyResult> anomalies = new ArrayList<>();
    DetectionConfigDTO detectionConfigDTO = DaoTestUtils.getTestDetectionConfig(provider, detectionConfigFile);
    detectionConfigDAO.save(detectionConfigDTO);
    MergedAnomalyResultDTO anomaly = DaoTestUtils.getTestMergedAnomalyResult(
        new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 6, 13, 0, dateTimeZone).getMillis(),
        TEST, TEST, 0.1, 1l, new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis());
    anomaly.setDetectionConfigId(detectionConfigDTO.getId());
    anomaly.setAvgCurrentVal(1.1);
    anomaly.setAvgBaselineVal(1.0);
    anomaly.setMetricUrn("thirdeye:metric:1");
    mergedAnomalyResultDAO.save(anomaly);
    anomalies.add(anomaly);
    anomaly = DaoTestUtils.getTestMergedAnomalyResult(
        new DateTime(2017, 11, 7, 10, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 7, 17, 0, dateTimeZone).getMillis(),
        TEST, TEST, 0.1, 1l, new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis());
    anomaly.setDetectionConfigId(detectionConfigDTO.getId());
    anomaly.setAvgCurrentVal(0.9);
    anomaly.setAvgBaselineVal(1.0);
    anomaly.setMetricUrn("thirdeye:metric:2");
    mergedAnomalyResultDAO.save(anomaly);
    anomalies.add(anomaly);

    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setName(TEST);
    metric.setDataset(TEST);
    metric.setAlias(TEST + "::" + TEST);
    metricDAO.save(metric);

    EmailContentFormatter
        contentFormatter = new EmailContentFormatter(new Properties(), new MetricAnomaliesContent(),
        thirdeyeAnomalyConfig, DaoTestUtils.getTestNotificationConfig("Test Config"));
    EmailEntity emailEntity = contentFormatter.getEmailEntity(anomalies);

    String htmlPath = ClassLoader.getSystemResource("test-metric-anomalies-template.html").getPath();
    Assert.assertEquals(
        ContentFormatterUtils.getEmailHtml(emailEntity).replaceAll("\\s", ""),
        ContentFormatterUtils.getHtmlContent(htmlPath).replaceAll("\\s", ""));
  }
}
