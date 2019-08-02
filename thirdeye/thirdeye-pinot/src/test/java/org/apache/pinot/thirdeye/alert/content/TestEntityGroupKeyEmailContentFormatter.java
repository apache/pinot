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

package org.apache.pinot.thirdeye.alert.content;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.alert.commons.EmailEntity;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorConfiguration;
import org.apache.pinot.thirdeye.anomaly.task.TaskDriverConfiguration;
import org.apache.pinot.thirdeye.anomaly.utils.EmailUtils;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.alert.content.EntityGroupKeyContentFormatter.*;
import static org.apache.pinot.thirdeye.anomaly.SmtpConfiguration.*;
import static org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator.*;


public class TestEntityGroupKeyEmailContentFormatter {
  private static final String TEST = "test";
  private int id = 0;
  private String dashboardHost = "http://localhost:8080/dashboard";
  private DAOTestBase testDAOProvider;
  private DetectionConfigManager detectionDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private MetricConfigManager metricDAO;

  @BeforeClass
  public void beforeClass(){
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    detectionDAO = daoRegistry.getDetectionConfigManager();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
    metricDAO = daoRegistry.getMetricConfigDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  /**
   * Entity Alert Trigger Condition: Sub-Entity-A OR Sub-Entity-B
   * Where Sub-Entity-A and Sub-Entity-B anomalies have a groupKey and a groupScore
   */
  @Test
  public void testGetEmailEntity() throws Exception {
    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setName(TEST);
    metric.setDataset(TEST);
    metric.setAlias(TEST + "::" + TEST);
    metricDAO.save(metric);

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

    DetectionConfigDTO detection = new DetectionConfigDTO();
    detection.setName("test_report");
    detection.setDescription("test_description");
    long id = detectionDAO.save(detection);

    DateTimeZone dateTimeZone = DateTimeZone.forID("America/Los_Angeles");

    MergedAnomalyResultDTO subGroupedAnomaly1 = DaoTestUtils.getTestGroupedAnomalyResult(
        new DateTime(2017, 11, 7, 10, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 7, 17, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis(),
        id);
    Map<String, String> properties = new HashMap<>();
    properties.put(PROP_GROUP_KEY, "group-1");
    properties.put(PROP_SUB_ENTITY_NAME, "sub-entity-A");
    properties.put(PROP_ANOMALY_SCORE, "10");
    subGroupedAnomaly1.setProperties(properties);
    subGroupedAnomaly1.setChildIds(Collections.singleton(1l));
    mergedAnomalyResultDAO.save(subGroupedAnomaly1);

    MergedAnomalyResultDTO subGroupedAnomaly2 = DaoTestUtils.getTestGroupedAnomalyResult(
        new DateTime(2017, 11, 7, 10, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 7, 17, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis(),
        id);
    Map<String, String> properties2 = new HashMap<>();
    properties2.put(PROP_GROUP_KEY, "group-2");
    properties2.put(PROP_SUB_ENTITY_NAME, "sub-entity-B");
    properties2.put(PROP_ANOMALY_SCORE, "20");
    subGroupedAnomaly2.setProperties(properties2);
    subGroupedAnomaly2.setChildIds(Collections.singleton(2l));
    mergedAnomalyResultDAO.save(subGroupedAnomaly2);

    MergedAnomalyResultDTO parentGroupedAnomaly = DaoTestUtils.getTestGroupedAnomalyResult(
        new DateTime(2017, 11, 7, 10, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 7, 17, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis(),
        id);
    Set<MergedAnomalyResultDTO> childrenAnomalies = new HashSet<>();
    childrenAnomalies.add(subGroupedAnomaly1);
    childrenAnomalies.add(subGroupedAnomaly2);
    parentGroupedAnomaly.setChildren(childrenAnomalies);
    mergedAnomalyResultDAO.save(parentGroupedAnomaly);
    List<AnomalyResult> anomalies = new ArrayList<>();
    anomalies.add(parentGroupedAnomaly);

    EmailContentFormatter contentFormatter = new EntityGroupKeyContentFormatter();
    contentFormatter.init(new Properties(), EmailContentFormatterConfiguration.fromThirdEyeAnomalyConfiguration(thirdeyeAnomalyConfig));
    EmailEntity emailEntity = contentFormatter.getEmailEntity(
        DaoTestUtils.getTestAlertConfiguration("Test Config"),
        new DetectionAlertFilterRecipients(EmailUtils.getValidEmailAddresses("a@b.com")),
        TEST, null, "", anomalies, null);
    String htmlPath = ClassLoader.getSystemResource("test-entity-groupby-email-content-formatter.html").getPath();

    Assert.assertEquals(
        ContentFormatterUtils.getEmailHtml(emailEntity).replaceAll("\\s", ""),
        ContentFormatterUtils.getHtmlContent(htmlPath).replaceAll("\\s", ""));
  }
}
