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

import org.apache.pinot.thirdeye.notification.commons.EmailEntity;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;
import org.apache.pinot.thirdeye.notification.formatter.channels.EmailContentFormatter;
import org.apache.pinot.thirdeye.notification.ContentFormatterUtils;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorConfiguration;
import org.apache.pinot.thirdeye.anomaly.task.TaskDriverConfiguration;
import org.apache.pinot.thirdeye.anomaly.utils.EmailUtils;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
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

import static org.apache.pinot.thirdeye.anomaly.SmtpConfiguration.*;


public class TestMetricAnomaliesContent {
  private static final String TEST = "test";
  private int id = 0;
  private String dashboardHost = "http://localhost:8080/dashboard";
  private DAOTestBase testDAOProvider;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private MetricConfigManager metricDAO;

  @BeforeClass
  public void beforeClass(){
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
    metricDAO = daoRegistry.getMetricConfigDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testGetEmailEntity() throws Exception {
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


    List<AnomalyResult> anomalies = new ArrayList<>();
    AnomalyFunctionDTO anomalyFunction = DaoTestUtils.getTestFunctionSpec(TEST, TEST);
    anomalyFunctionDAO.save(anomalyFunction);
    MergedAnomalyResultDTO anomaly = DaoTestUtils.getTestMergedAnomalyResult(
        new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 6, 13, 0, dateTimeZone).getMillis(),
        TEST, TEST, 0.1, 1l, new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis());
    anomaly.setFunction(anomalyFunction);
    anomaly.setAvgCurrentVal(1.1);
    anomaly.setAvgBaselineVal(1.0);
    mergedAnomalyResultDAO.save(anomaly);
    anomalies.add(anomaly);
    anomaly = DaoTestUtils.getTestMergedAnomalyResult(
        new DateTime(2017, 11, 7, 10, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 7, 17, 0, dateTimeZone).getMillis(),
        TEST, TEST, 0.1, 1l, new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis());
    anomaly.setFunction(anomalyFunction);
    anomaly.setAvgCurrentVal(0.9);
    anomaly.setAvgBaselineVal(1.0);
    mergedAnomalyResultDAO.save(anomaly);
    anomalies.add(anomaly);

    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setName(TEST);
    metric.setDataset(TEST);
    metric.setAlias(TEST + "::" + TEST);
    metricDAO.save(metric);

    AlertConfigDTO alertConfigDTO = DaoTestUtils.getTestAlertConfiguration("Test Config");

    EmailContentFormatter
        contentFormatter = new EmailContentFormatter(new MetricAnomaliesContent(), thirdeyeAnomalyConfig);
    ADContentFormatterContext context = new ADContentFormatterContext();
    context.setAlertConfig(alertConfigDTO);
    DetectionAlertFilterRecipients recipients = new DetectionAlertFilterRecipients(
        EmailUtils.getValidEmailAddresses("a@b.com"));
    EmailEntity emailEntity = contentFormatter.getEmailEntity(recipients, TEST, null, "", anomalies, context);

    String htmlPath = ClassLoader.getSystemResource("test-metric-anomalies-template.html").getPath();
    Assert.assertEquals(
        ContentFormatterUtils.getEmailHtml(emailEntity).replaceAll("\\s", ""),
        ContentFormatterUtils.getHtmlContent(htmlPath).replaceAll("\\s", ""));
  }
}
