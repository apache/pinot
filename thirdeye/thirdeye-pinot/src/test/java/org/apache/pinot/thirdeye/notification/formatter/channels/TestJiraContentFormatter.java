package org.apache.pinot.thirdeye.notification.formatter.channels;

import com.atlassian.jira.rest.client.api.domain.input.ComplexIssueInputFieldValue;
import com.atlassian.jira.rest.client.api.domain.input.IssueInput;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
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
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.notification.commons.JiraConfiguration.*;
import static org.apache.pinot.thirdeye.notification.formatter.channels.JiraContentFormatter.*;


public class TestJiraContentFormatter {
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String FROM_ADDRESS_VALUE = "test3@test.test";
  private static final String ALERT_NAME_VALUE = "alert_name";
  private static final String COLLECTION_VALUE = "test_dataset";
  private static final String DETECTION_NAME_VALUE = "test detection";
  private static final String METRIC_VALUE = "test_metric";

  private DAOTestBase testDAOProvider;
  private DetectionAlertConfigManager alertConfigDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private DetectionConfigManager detectionDAO;
  private MetricConfigManager metricDAO;
  private DatasetConfigManager dataSetDAO;
  private DetectionAlertConfigDTO alertConfigDTO;
  private ADContentFormatterContext adContext;
  private Long alertConfigId;
  private Long detectionConfigId;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    this.testDAOProvider = DAOTestBase.getInstance();
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

    this.alertConfigDTO = new DetectionAlertConfigDTO();
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, "org.apache.pinot.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    properties.put(PROP_DETECTION_CONFIG_IDS, Collections.singletonList(this.detectionConfigId));

    Map<String, Object> jiraScheme = new HashMap<>();
    jiraScheme.put("className", "org.apache.pinot.thirdeye.detection.alert.scheme.RandomAlerter");
    this.alertConfigDTO.setAlertSchemes(Collections.singletonMap("jiraScheme", jiraScheme));
    this.alertConfigDTO.setProperties(properties);
    this.alertConfigDTO.setFrom(FROM_ADDRESS_VALUE);
    this.alertConfigDTO.setName(ALERT_NAME_VALUE);
    this.alertConfigDTO.setVectorClocks(new HashMap<>());
    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("test_ref_key", "test_ref_value");
    this.alertConfigDTO.setReferenceLinks(refLinks);

    this.alertConfigId = this.alertConfigDAO.save(this.alertConfigDTO);

    this.adContext = new ADContentFormatterContext();
    adContext.setNotificationConfig(this.alertConfigDTO);

    MergedAnomalyResultDTO anomalyResultDTO = new MergedAnomalyResultDTO();
    anomalyResultDTO.setStartTime(1000L);
    anomalyResultDTO.setEndTime(2000L);
    anomalyResultDTO.setDetectionConfigId(this.detectionConfigId);
    anomalyResultDTO.setCollection(COLLECTION_VALUE);
    anomalyResultDTO.setMetric(METRIC_VALUE);
    this.anomalyDAO.save(anomalyResultDTO);

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset(COLLECTION_VALUE);
    datasetConfigDTO.setDataSource("myDataSource");
    this.dataSetDAO.save(datasetConfigDTO);
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

    BaseNotificationContent content = DetectionAlertScheme.buildNotificationContent(jiraClientConfig);
    JiraContentFormatter jiraContent = new JiraContentFormatter(
        JiraConfiguration.createFromProperties(jiraConfiguration), jiraClientConfig, content, teConfig, adContext);

    IssueInput issueInput = jiraContent.getJiraEntity(new HashSet<>(this.anomalyDAO.findAll()));

    // Assert Jira fields
    Assert.assertEquals(issueInput.getField(PROP_LABELS).getValue(), Arrays.asList("test-label-1", "test-label-2", "thirdeye"));
    Assert.assertEquals(((ComplexIssueInputFieldValue) issueInput.getField(PROP_ISSUE_TYPE).getValue()).getValuesMap().get("id"), "16");
    Assert.assertEquals(((ComplexIssueInputFieldValue) issueInput.getField(PROP_ASSIGNEE).getValue()).getValuesMap().get("name"), "test");
    Assert.assertEquals(((ComplexIssueInputFieldValue) issueInput.getField(PROP_PROJECT).getValue()).getValuesMap().get("key"), "thirdeye");

    // Assert summary and description
    Assert.assertEquals(issueInput.getField(PROP_SUMMARY).getValue(), "Thirdeye Alert : alert_name - test_metric");
    Assert.assertEquals(
        issueInput.getField("description").getValue().toString().replaceAll(" ", ""),
        IOUtils.toString(Thread.currentThread().getContextClassLoader()
            .getResourceAsStream("test-jira-anomalies-template.ftl")).replaceAll(" ", ""));
  }
}
