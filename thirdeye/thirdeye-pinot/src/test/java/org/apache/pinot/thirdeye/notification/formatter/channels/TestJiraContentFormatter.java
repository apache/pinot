/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.notification.formatter.channels;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.thirdeye.anomaly.AnomalyType;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.common.restclient.MockThirdEyeRcaRestClient;
import org.apache.pinot.thirdeye.common.restclient.ThirdEyeRcaRestClient;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.alert.scheme.DetectionAlertScheme;
import org.apache.pinot.thirdeye.notification.commons.JiraConfiguration;
import org.apache.pinot.thirdeye.notification.commons.JiraEntity;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.notification.content.templates.MetricAnomaliesContent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.alert.filter.DimensionsRecipientAlertFilter.*;
import static org.apache.pinot.thirdeye.notification.commons.JiraConfiguration.*;
import static org.apache.pinot.thirdeye.notification.commons.ThirdEyeJiraClient.*;
import static org.apache.pinot.thirdeye.notification.formatter.channels.JiraContentFormatter.*;


public class TestJiraContentFormatter {
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String FROM_ADDRESS_VALUE = "test3@test.test";
  private static final String ALERT_NAME_VALUE = "alert_name";
  private static final String COLLECTION_VALUE = "test_dataset";
  private static final String DETECTION_NAME_VALUE = "test detection";
  private static final String METRIC_VALUE = "test_metric";

  private static final List<Map<String, Object>> PROP_DIMENSION_RECIPIENTS_VALUE = new ArrayList<>();

  private DetectionAlertConfigManager alertConfigDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private DetectionConfigManager detectionDAO;
  private MetricConfigManager metricDAO;
  private DatasetConfigManager dataSetDAO;
  private DetectionAlertConfigDTO alertConfigDTO;
  private DetectionAlertConfigDTO alertConfigDimAlerter;
  private Long detectionConfigId;
  private Long subsId1;
  private Long subsId2;
  private DAOTestBase testDAOProvider;
  private List<AnomalyResult> anomalies;
  private MetricAnomaliesContent metricAnomaliesContent;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.alertConfigDAO = daoRegistry.getDetectionAlertConfigManager();
    this.anomalyDAO = daoRegistry.getMergedAnomalyResultDAO();
    this.detectionDAO = daoRegistry.getDetectionConfigManager();
    this.metricDAO = daoRegistry.getMetricConfigDAO();
    this.dataSetDAO = daoRegistry.getDatasetConfigDAO();

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setName(METRIC_VALUE);
    metricConfigDTO.setDataset(COLLECTION_VALUE);
    metricConfigDTO.setAlias("test");
    long metricId = this.metricDAO.save(metricConfigDTO);

    DetectionConfigDTO detectionConfig = new DetectionConfigDTO();
    detectionConfig.setName(DETECTION_NAME_VALUE);
    this.detectionConfigId = this.detectionDAO.save(detectionConfig);

    this.alertConfigDTO = createDetectionAlertConfig(this.detectionConfigId);
    subsId1 = this.alertConfigDAO.save(this.alertConfigDTO);
    this.alertConfigDTO.setId(subsId1);

    anomalies = new ArrayList<>();
    DateTimeZone dateTimeZone = DateTimeZone.forID("America/Los_Angeles");
    MergedAnomalyResultDTO anomalyResultDTO = new MergedAnomalyResultDTO();
    anomalyResultDTO.setStartTime(new DateTime(2020, 1, 1, 10, 5, dateTimeZone).getMillis());
    anomalyResultDTO.setEndTime(new DateTime(2020, 1, 2, 10, 5, dateTimeZone).getMillis());
    anomalyResultDTO.setDetectionConfigId(this.detectionConfigId);
    anomalyResultDTO.setCollection(COLLECTION_VALUE);
    anomalyResultDTO.setMetric(METRIC_VALUE);
    anomalyResultDTO.setType(AnomalyType.DEVIATION);
    anomalyResultDTO.setMetricUrn("thirdeye:metric:123:key%3Dvalue");
    this.anomalyDAO.save(anomalyResultDTO);
    anomalies.add(anomalyResultDTO);

    MergedAnomalyResultDTO anomalyResultDTO2 = new MergedAnomalyResultDTO();
    anomalyResultDTO2.setStartTime(new DateTime(2020, 1, 1, 10, 5, dateTimeZone).getMillis());
    anomalyResultDTO2.setEndTime(new DateTime(2020, 1, 1, 22, 50, dateTimeZone).getMillis());
    anomalyResultDTO2.setDetectionConfigId(this.detectionConfigId);
    anomalyResultDTO2.setCollection(COLLECTION_VALUE);
    anomalyResultDTO2.setMetric(METRIC_VALUE);
    anomalyResultDTO2.setType(AnomalyType.DATA_SLA);
    anomalyResultDTO2.setAnomalyResultSource(AnomalyResultSource.DATA_QUALITY_DETECTION);
    anomalyResultDTO2.setMetricUrn("thirdeye:metric:124");
    Map<String, String> props = new HashMap<>();
    props.put("sla", "2_HOURS");
    props.put("datasetLastRefreshTime", "" + new DateTime(2020, 1, 1, 10, 5, dateTimeZone).getMillis());
    anomalyResultDTO2.setProperties(props);
    this.anomalyDAO.save(anomalyResultDTO2);
    anomalies.add(anomalyResultDTO2);

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset(COLLECTION_VALUE);
    datasetConfigDTO.setDataSource("myDataSource");
    this.dataSetDAO.save(datasetConfigDTO);

    this.alertConfigDimAlerter = createDimRecipientsDetectionAlertConfig(this.detectionConfigId);
    subsId2 = this.alertConfigDAO.save(this.alertConfigDimAlerter);
    alertConfigDimAlerter.setId(subsId2);

    Map<String, Object> expectedResponse = new HashMap<>();
    expectedResponse.put("cubeResults", "{}");
    ThirdEyeRcaRestClient rcaClient = MockThirdEyeRcaRestClient.setupMockClient(expectedResponse);
    this.metricAnomaliesContent = new MetricAnomaliesContent(rcaClient);
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  public static DetectionAlertConfigDTO createDetectionAlertConfig(Long detectionConfigId) {
    DetectionAlertConfigDTO alertConfig = new DetectionAlertConfigDTO();

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, "org.apache.pinot.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    properties.put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(detectionConfigId));

    Map<String, Object> jiraScheme = new HashMap<>();
    jiraScheme.put("className", "org.apache.pinot.thirdeye.detection.alert.scheme.RandomAlerter");
    alertConfig.setAlertSchemes(Collections.singletonMap("jiraScheme", jiraScheme));
    alertConfig.setProperties(properties);
    alertConfig.setFrom(FROM_ADDRESS_VALUE);
    alertConfig.setName(ALERT_NAME_VALUE + 1);
    alertConfig.setVectorClocks(new HashMap<>());
    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("test_ref_key", "test_ref_value");
    alertConfig.setReferenceLinks(refLinks);

    return alertConfig;
  }

  public static DetectionAlertConfigDTO createDimRecipientsDetectionAlertConfig(Long detectionConfigId) {
    DetectionAlertConfigDTO alertConfig = new DetectionAlertConfigDTO();

    Map<String, Object> dimensionRecipient1 = new HashMap<>();
    Map<String, String> dimensionKeys1 = new HashMap<>();
    dimensionKeys1.put("key", "value");
    dimensionRecipient1.put(PROP_DIMENSION, dimensionKeys1);
    Map<String, Object> notificationValues1 = new HashMap<>();
    Map<String, Object> notificationJiraParams1 = new HashMap<>();
    notificationJiraParams1.put("project", "THIRDEYE1");
    notificationJiraParams1.put("assignee", "thirdeye1");
    notificationValues1.put("jiraScheme", notificationJiraParams1);
    dimensionRecipient1.put(PROP_NOTIFY, notificationValues1);

    Map<String, Object> dimensionRecipient2 = new HashMap<>();
    Map<String, String> dimensionKeys2 = new HashMap<>();
    dimensionKeys2.put("key1", "anotherValue1");
    dimensionKeys2.put("key2", "anotherValue2");
    dimensionRecipient2.put(PROP_DIMENSION, dimensionKeys2);
    Map<String, Object> notificationValues2 = new HashMap<>();
    Map<String, Object> notificationJiraParams2 = new HashMap<>();
    notificationJiraParams2.put("project", "THIRDEYE2");
    notificationJiraParams2.put("assignee", "thirdeye2");
    notificationValues2.put("jiraScheme", notificationJiraParams2);
    dimensionRecipient2.put(PROP_NOTIFY, notificationValues2);

    PROP_DIMENSION_RECIPIENTS_VALUE.add(dimensionRecipient1);
    PROP_DIMENSION_RECIPIENTS_VALUE.add(dimensionRecipient2);

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, "org.apache.pinot.thirdeye.detection.alert.filter.DimensionsRecipientAlertFilter");
    properties.put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(detectionConfigId));
    properties.put(PROP_DIMENSION_RECIPIENTS, PROP_DIMENSION_RECIPIENTS_VALUE);

    Map<String, Object> jiraScheme = new HashMap<>();
    jiraScheme.put("className", "org.apache.pinot.thirdeye.detection.alert.scheme.RandomAlerter");
    alertConfig.setAlertSchemes(Collections.singletonMap("jiraScheme", jiraScheme));
    alertConfig.setProperties(properties);
    alertConfig.setFrom(FROM_ADDRESS_VALUE);
    alertConfig.setName(ALERT_NAME_VALUE + 2);
    alertConfig.setVectorClocks(new HashMap<>());
    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("test_ref_key", "test_ref_value");
    alertConfig.setReferenceLinks(refLinks);

    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(detectionConfigId, 0L);
    alertConfig.setVectorClocks(vectorClocks);

    return alertConfig;
  }

  @Test
  public void testJiraContent() throws Exception {
    Map<String,Object> jiraConfiguration = new HashMap<>();
    jiraConfiguration.put(JIRA_URL_KEY, "jira-test-url");
    jiraConfiguration.put(JIRA_USER_KEY, "test");
    jiraConfiguration.put(JIRA_PASSWD_KEY, "test");
    jiraConfiguration.put(JIRA_DEFAULT_PROJECT_KEY, "thirdeye");
    jiraConfiguration.put(JIRA_ISSUE_TYPE_KEY, 16);

    Map<String, Map<String, Object>> alerterConfigurations = new HashMap<>();
    alerterConfigurations.put(JIRA_CONFIG_KEY, jiraConfiguration);
    ThirdEyeAnomalyConfiguration teConfig = new ThirdEyeAnomalyConfiguration();
    teConfig.setAlerterConfiguration(alerterConfigurations);
    teConfig.setDashboardHost("test");

    Properties jiraClientConfig = new Properties();
    jiraClientConfig.put(PROP_ASSIGNEE, "test");
    jiraClientConfig.put(PROP_LABELS, Arrays.asList("test-label-1", "test-label-2"));
    jiraClientConfig.put(PROP_SUBJECT_STYLE, AlertConfigBean.SubjectType.METRICS);

    JiraContentFormatter jiraContent = new JiraContentFormatter(
        JiraConfiguration.createFromProperties(jiraConfiguration), jiraClientConfig, this.metricAnomaliesContent, teConfig,
        this.alertConfigDTO);

    JiraEntity jiraEntity = jiraContent.getJiraEntity(ArrayListMultimap.create(), this.anomalies);

    // Assert Jira fields
    Assert.assertEquals(jiraEntity.getLabels(), Arrays.asList("test-label-1", "test-label-2", "thirdeye", "subsId=" + subsId1));
    Assert.assertEquals(jiraEntity.getJiraIssueTypeId().longValue(), 16L);
    Assert.assertEquals(jiraEntity.getAssignee(), "test");
    Assert.assertEquals(jiraEntity.getJiraProject(), "thirdeye");

    // Assert summary and description
    Assert.assertEquals(jiraEntity.getSummary(), "Thirdeye Alert : alert_name1 - test_metric");
    Assert.assertEquals(
        jiraEntity.getDescription().replaceAll(" ", ""),
        IOUtils.toString(Thread.currentThread().getContextClassLoader()
            .getResourceAsStream("test-jira-anomalies-template.ftl")).replaceAll(" ", ""));
  }

  @Test
  public void testJiraContentWithDimRecipientsAlerter() throws Exception {
    Map<String,Object> jiraConfiguration = new HashMap<>();
    jiraConfiguration.put(JIRA_URL_KEY, "jira-test-url");
    jiraConfiguration.put(JIRA_USER_KEY, "test");
    jiraConfiguration.put(JIRA_PASSWD_KEY, "test");
    jiraConfiguration.put(JIRA_DEFAULT_PROJECT_KEY, "thirdeye");
    jiraConfiguration.put(JIRA_ISSUE_TYPE_KEY, 16);

    Map<String, Map<String, Object>> alerterConfigurations = new HashMap<>();
    alerterConfigurations.put(JIRA_CONFIG_KEY, jiraConfiguration);
    ThirdEyeAnomalyConfiguration teConfig = new ThirdEyeAnomalyConfiguration();
    teConfig.setAlerterConfiguration(alerterConfigurations);
    teConfig.setDashboardHost("test");

    Properties jiraClientConfig = new Properties();
    jiraClientConfig.put(PROP_ASSIGNEE, "test");
    jiraClientConfig.put(PROP_LABELS, Arrays.asList("test-label-1", "test-label-2"));
    jiraClientConfig.put(PROP_SUBJECT_STYLE, AlertConfigBean.SubjectType.METRICS);

    JiraContentFormatter jiraContent = new JiraContentFormatter(
        JiraConfiguration.createFromProperties(jiraConfiguration), jiraClientConfig, this.metricAnomaliesContent,
        teConfig, this.alertConfigDimAlerter);

    Multimap<String, String> dimensionKeys1 = ArrayListMultimap.create();
    dimensionKeys1.put("key", "value");
    JiraEntity jiraEntity = jiraContent.getJiraEntity(dimensionKeys1, this.anomalies);

    // Assert Jira fields
    Assert.assertEquals(jiraEntity.getLabels(), Arrays.asList("test-label-1", "test-label-2", "thirdeye", "subsId=" + subsId2, "key=value"));
    Assert.assertEquals(jiraEntity.getJiraIssueTypeId().longValue(), 16L);
    Assert.assertEquals(jiraEntity.getAssignee(), "test");
    Assert.assertEquals(jiraEntity.getJiraProject(), "thirdeye");

    // Assert summary and description
    Assert.assertEquals(jiraEntity.getSummary(), "Thirdeye Alert : alert_name2 - test_metric, key=value");
    Assert.assertEquals(
        jiraEntity.getDescription().replaceAll(" ", ""),
        IOUtils.toString(Thread.currentThread().getContextClassLoader()
            .getResourceAsStream("test-jira-anomalies-template.ftl")).replaceAll(" ", "").replaceAll("alert_name1", "alert_name2"));
  }
}
