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

package com.linkedin.thirdeye.anomaly.alert.v2;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.linkedin.thirdeye.alert.commons.AnomalyFeedConfig;
import com.linkedin.thirdeye.alert.commons.AnomalyFeedFactory;
import com.linkedin.thirdeye.alert.commons.EmailContentFormatterFactory;
import com.linkedin.thirdeye.alert.commons.EmailEntity;
import com.linkedin.thirdeye.alert.content.EmailContentFormatter;
import com.linkedin.thirdeye.alert.content.EmailContentFormatterConfiguration;
import com.linkedin.thirdeye.alert.content.EmailContentFormatterContext;
import com.linkedin.thirdeye.alert.feed.AnomalyFeed;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskInfo;
import com.linkedin.thirdeye.anomaly.alert.grouping.AlertGrouper;
import com.linkedin.thirdeye.anomaly.alert.grouping.AlertGrouperFactory;
import com.linkedin.thirdeye.anomaly.alert.grouping.DummyAlertGrouper;
import com.linkedin.thirdeye.anomaly.alert.grouping.SimpleGroupedAnomalyMerger;
import com.linkedin.thirdeye.anomaly.alert.grouping.auxiliary_info_provider.AlertGroupAuxiliaryInfoProvider;
import com.linkedin.thirdeye.anomaly.alert.grouping.auxiliary_info_provider.AlertGroupRecipientProviderFactory;
import com.linkedin.thirdeye.anomaly.alert.grouping.auxiliary_info_provider.AuxiliaryAlertGroupInfo;
import com.linkedin.thirdeye.anomaly.alert.grouping.filter.AlertGroupFilter;
import com.linkedin.thirdeye.anomaly.alert.grouping.filter.AlertGroupFilterFactory;
import com.linkedin.thirdeye.anomaly.alert.template.pojo.MetricDimensionReport;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.alert.util.DataReportHelper;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.anomaly.utils.EmailUtils;
import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AlertSnapshotManager;
import com.linkedin.thirdeye.datalayer.bao.GroupedAnomalyResultsManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AlertSnapshotDTO;
import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean.AlertGroupConfig;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean.EmailConfig;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean.EmailFormatterConfig;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean.ReportConfigCollection;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean.ReportMetricConfig;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTimeZone;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.anomaly.SmtpConfiguration.SMTP_CONFIG_KEY;


public class AlertTaskRunnerV2 implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AlertTaskRunnerV2.class);

  public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone("America/Los_Angeles");
  public static final String DEFAULT_EMAIL_FORMATTER_TYPE = "MultipleAnomaliesEmailContentFormatter";
  public static final String CHARSET = "UTF-8";
  public static final String EMAIL_WHITELIST_KEY = "emailWhitelist";

  private final MergedAnomalyResultManager anomalyMergedResultDAO;
  private final AlertConfigManager alertConfigDAO;
  private final MetricConfigManager metricConfigManager;
  private final GroupedAnomalyResultsManager groupedAnomalyResultsDAO;
  private final AlertSnapshotManager alertSnapshotDAO;

  private AlertConfigDTO alertConfig;
  private ThirdEyeAnomalyConfiguration thirdeyeConfig;
  private AlertFilterFactory alertFilterFactory;
  private AlertGroupRecipientProviderFactory alertGroupAuxiliaryInfoProviderFactory;
  private final String MAX_ALLOWED_MERGE_GAP_KEY = "maxAllowedMergeGap";
  private final long DEFAULT_MAX_ALLOWED_MERGE_GAP = 14400000L;

  public AlertTaskRunnerV2() {
    anomalyMergedResultDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    alertConfigDAO = DAORegistry.getInstance().getAlertConfigDAO();
    alertSnapshotDAO = DAORegistry.getInstance().getAlertSnapshotDAO();
    metricConfigManager = DAORegistry.getInstance().getMetricConfigDAO();
    groupedAnomalyResultsDAO = DAORegistry.getInstance().getGroupedAnomalyResultsDAO();
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext)
      throws Exception {
    List<TaskResult> taskResult = new ArrayList<>();
    AlertTaskInfo alertTaskInfo = (AlertTaskInfo) taskInfo;
    // Fetch the latest alert config instead of the one provided by task context, which could be out-of-dated.
    alertConfig = alertConfigDAO.findById(alertTaskInfo.getAlertConfigDTO().getId());
    thirdeyeConfig = taskContext.getThirdEyeAnomalyConfiguration();
    alertFilterFactory = new AlertFilterFactory(thirdeyeConfig.getAlertFilterConfigPath());
    alertGroupAuxiliaryInfoProviderFactory =
        new AlertGroupRecipientProviderFactory(thirdeyeConfig.getAlertGroupRecipientProviderConfigPath());

    try {
      LOG.info("Begin executing task {}", taskInfo);
      runTask();
    } catch (Exception t) {
      LOG.error("Task failed with exception:", t);
      sendFailureEmail(t);
      // Let task driver mark this task failed
      throw t;
    }
    return taskResult;
  }

  // TODO : separate code path for new vs old alert config !
  private void runTask() throws Exception {
    ThirdeyeMetricsUtil.alertTaskCounter.inc();
    try {
      LOG.info("Starting email report for id : {}, name : {} ", alertConfig.getId(),
          alertConfig.getName());
      sendAnomalyReport();
      sendScheduledDataReport();

    } finally {
      ThirdeyeMetricsUtil.alertTaskSuccessCounter.inc();
    }
  }

  private void sendAnomalyReport() throws Exception {
    EmailConfig emailConfig = alertConfig.getEmailConfig();
    List<MergedAnomalyResultDTO> results = new ArrayList<>();
    List<MergedAnomalyResultDTO> mergedAnomaliesAllResults = new ArrayList<>();
    AnomalyFeed anomalyFeed = null;
    if (alertConfig.getAnomalyFeedConfig() != null &&
        StringUtils.isNotBlank(alertConfig.getAnomalyFeedConfig().getAnomalyFeedType())) {
      LOG.info("Use Anomaly Feed for alert {}", alertConfig.getId());
      AnomalyFeedConfig anomalyFeedConfig = alertConfig.getAnomalyFeedConfig();
      AlertSnapshotDTO alertSnapshot = alertSnapshotDAO.findById(anomalyFeedConfig.getAlertSnapshotId());
      if (alertSnapshot == null) { // if alert snapshot doesn't exist, create a new one
        alertSnapshot = new AlertSnapshotDTO();
        alertSnapshot.setLastNotifyTime(0);
        long snapshotId = alertSnapshotDAO.save(alertSnapshot);
        anomalyFeedConfig.setAlertSnapshotId(snapshotId);
        alertConfigDAO.update(alertConfig);
      }
      anomalyFeed = AnomalyFeedFactory.fromClassName(anomalyFeedConfig.getAnomalyFeedType());
      anomalyFeed.init(alertFilterFactory, anomalyFeedConfig);
      Collection<MergedAnomalyResultDTO> anaomalyAlertCandidates = anomalyFeed.getAnomalyFeed();
      mergedAnomaliesAllResults.addAll(anaomalyAlertCandidates);
    } else if (emailConfig != null && emailConfig.getFunctionIds() != null) {
      LOG.info("Use Email Config for alert {}", alertConfig.getId());
      List<Long> functionIds = alertConfig.getEmailConfig().getFunctionIds();
      long lastNotifiedAnomaly = emailConfig.getAnomalyWatermark();
      for (Long functionId : functionIds) {
        List<MergedAnomalyResultDTO> resultsForFunction =
            anomalyMergedResultDAO.findByFunctionIdAndIdGreaterThan(functionId, lastNotifiedAnomaly);
        if (CollectionUtils.isNotEmpty(resultsForFunction)) {
          mergedAnomaliesAllResults.addAll(resultsForFunction);
        }
        // fetch anomalies having id lesser than the watermark for the same function with notified = false & endTime > last one day
        // these anomalies are the ones that did not qualify filtration rule and got modified.
        // We should add these again so that these could be included in email if qualified through filtration rule
        List<MergedAnomalyResultDTO> filteredAnomalies = anomalyMergedResultDAO.findUnNotifiedByFunctionIdAndIdLesserThanAndEndTimeGreaterThanLastOneDay(functionId,
                lastNotifiedAnomaly);
        if (CollectionUtils.isNotEmpty(filteredAnomalies)) {
          mergedAnomaliesAllResults.addAll(filteredAnomalies);
        }
      }
    }
    // apply filtration rule
    results = AlertFilterHelper.applyFiltrationRule(mergedAnomaliesAllResults, alertFilterFactory);

    // only anomalies detected by default scheduler are sent; the anomalies detected during replay or given by users will not be sent
    Iterator<MergedAnomalyResultDTO> mergedAnomalyIterator = results.iterator();
    while (mergedAnomalyIterator.hasNext()) {
      MergedAnomalyResultDTO anomaly = mergedAnomalyIterator.next();
      if (anomaly.getAnomalyResultSource() == null) {
        continue;
      }
      if (!AnomalyResultSource.DEFAULT_ANOMALY_DETECTION.equals(anomaly.getAnomalyResultSource())) {
        mergedAnomalyIterator.remove();
      }
    }

    if (results.isEmpty()) {
      LOG.info("Zero anomalies found, skipping sending email");
    } else {
      // TODO: Add dimensional alert grouping before the stage of task runner?
      // There are two approaches to solve the problem of alert grouping:
      // 1. Anomaly results from detection --> Grouped anomalies from grouper --> Alerter sends emails on grouped anomalies
      // 2. Anomaly results from detection --> Alerter performs simple grouping and sends alerts in one go

      // Current implementation uses the second approach for experimental purpose. We might need to move to
      //     approach 1 in order to consider multi-metric grouping.
      // Input: a list of anomalies.
      // Output: lists of anomalies; each list contains the anomalies of the same group.
      AlertGroupConfig alertGroupConfig = alertConfig.getAlertGroupConfig();
      if (alertGroupConfig == null) {
        alertGroupConfig = new AlertGroupConfig();
      }
      AlertGrouper alertGrouper = AlertGrouperFactory.fromSpec(alertGroupConfig.getGroupByConfig());
      Map<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomalyResultsMap = alertGrouper.group(results);

      Map<DimensionMap, GroupedAnomalyResultsDTO> filteredGroupedAnomalyResultsMap;
      // DummyAlertGroupFilter does not generate any GroupedAnomaly. Thus, we don't apply any additional processes.
      if (alertGrouper instanceof DummyAlertGrouper) {
        filteredGroupedAnomalyResultsMap = groupedAnomalyResultsMap;
      } else {
        filteredGroupedAnomalyResultsMap = timeBasedMergeAndFilterGroupedAnomalies(groupedAnomalyResultsMap, alertGroupConfig);
      }

      for (Map.Entry<DimensionMap, GroupedAnomalyResultsDTO> entry : filteredGroupedAnomalyResultsMap.entrySet()) {
        // Anomaly results for this group
        DimensionMap dimensions = entry.getKey();
        GroupedAnomalyResultsDTO groupedAnomalyDTO = entry.getValue();
        List<AnomalyResult> anomalyResultListOfGroup = new ArrayList<>();
        for (MergedAnomalyResultDTO anomaly : groupedAnomalyDTO.getAnomalyResults()) {
          anomalyResultListOfGroup.add(anomaly);
        }
        // Append auxiliary recipients for this group
        DetectionAlertFilterRecipients recipientsForThisGroup = alertConfig.getReceiverAddresses();
        //   Get auxiliary email recipient from provider
        AlertGroupAuxiliaryInfoProvider auxiliaryInfoProvider =
            alertGroupAuxiliaryInfoProviderFactory.fromSpec(alertGroupConfig.getGroupAuxiliaryEmailProvider());
        AuxiliaryAlertGroupInfo auxiliaryInfo =
            auxiliaryInfoProvider.getAlertGroupAuxiliaryInfo(dimensions, groupedAnomalyDTO.getAnomalyResults());
        // Check if this group should be skipped
        if (auxiliaryInfo.isSkipGroupAlert()) {
          continue;
        }
        // Construct email subject
        StringBuilder emailSubjectBuilder = new StringBuilder("Thirdeye Alert : ");
        emailSubjectBuilder.append(alertConfig.getName());
        // Append group tag to email subject
        if (StringUtils.isNotBlank(auxiliaryInfo.getGroupTag())) {
          emailSubjectBuilder.append(" (").append(auxiliaryInfo.getGroupTag()).append(") ");
        }
        // Append auxiliary recipients
        recipientsForThisGroup.getTo().addAll(EmailUtils.getValidEmailAddresses(auxiliaryInfo.getAuxiliaryRecipients()));

        // Construct group name if dimensions of this group is not empty
        String groupName = null;
        if (dimensions.size() != 0) {
          StringBuilder sb = new StringBuilder();
          String separator = "";
          for (Map.Entry<String, String> dimension : dimensions.entrySet()) {
            sb.append(separator).append(dimension.getKey()).append(":").append(dimension.getValue());
            separator = ", ";
          }
          groupName = sb.toString();
        }
        // Generate and send out an anomaly report for this group
        EmailContentFormatter emailContentFormatter;
        EmailFormatterConfig emailFormatterConfig = alertConfig.getEmailFormatterConfig();
        try {
          emailContentFormatter = EmailContentFormatterFactory.fromClassName(emailFormatterConfig.getType());
        } catch (Exception e) {
          LOG.error("User-assigned Email Formatter Config {} is not available. Use default instead.", alertConfig.getEmailFormatterConfig());
          emailContentFormatter = EmailContentFormatterFactory.fromClassName(DEFAULT_EMAIL_FORMATTER_TYPE);
        }
        EmailContentFormatterConfiguration emailContemtFormatterConfig = EmailContentFormatterConfiguration
            .fromThirdEyeAnomalyConfiguration(thirdeyeConfig);
        if (emailFormatterConfig != null && StringUtils.isNotBlank(emailFormatterConfig.getProperties())) {
          emailContentFormatter.init(com.linkedin.thirdeye.datalayer.util.
              StringUtils.decodeCompactedProperties(emailFormatterConfig.getProperties()), emailContemtFormatterConfig
              );
        } else {
          emailContentFormatter.init(new Properties(), emailContemtFormatterConfig);
        }

        // whitelisted recipients only
        List<String> emailWhitelist = ConfigUtils.getList(
            this.thirdeyeConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY).get(EMAIL_WHITELIST_KEY));
        if (!emailWhitelist.isEmpty()) {
          recipientsForThisGroup = retainWhitelisted(recipientsForThisGroup, emailWhitelist);
        }

        EmailEntity emailEntity = emailContentFormatter
            .getEmailEntity(alertConfig, recipientsForThisGroup, emailSubjectBuilder.toString(),
                groupedAnomalyDTO.getId(), groupName, anomalyResultListOfGroup, new EmailContentFormatterContext());
        EmailHelper.sendEmailWithEmailEntity(emailEntity,
            SmtpConfiguration.createFromProperties(thirdeyeConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY)));
        // Update notified flag
        if (alertGrouper instanceof DummyAlertGrouper) {
          // DummyAlertGroupFilter does not generate real GroupedAnomaly, so the flag has to be set on merged anomalies.
          updateNotifiedStatus(groupedAnomalyDTO.getAnomalyResults());
        } else {
          // For other alert groupers, the notified flag is set on the grouped anomalies.
          groupedAnomalyDTO.setNotified(true);
          groupedAnomalyResultsDAO.update(groupedAnomalyDTO);
        }
      }

      // update anomaly watermark in alertConfig
      long lastNotifiedAlertId = emailConfig.getAnomalyWatermark();
      for (MergedAnomalyResultDTO anomalyResult : results) {
        if (anomalyResult.getId() > lastNotifiedAlertId) {
          lastNotifiedAlertId = anomalyResult.getId();
        }
      }
      if (lastNotifiedAlertId != emailConfig.getAnomalyWatermark()) {
        alertConfig.getEmailConfig().setAnomalyWatermark(lastNotifiedAlertId);
        alertConfigDAO.update(alertConfig);
      }
      if (anomalyFeed != null && results.size() > 0) {
        anomalyFeed.updateSnapshot(results);
      }
    }
  }

  private void sendScheduledDataReport() throws Exception {
    ReportConfigCollection reportConfigCollection =
        alertConfig.getReportConfigCollection();

    if (reportConfigCollection != null && reportConfigCollection.isEnabled()) {
      if (reportConfigCollection.getReportMetricConfigs() != null
          && reportConfigCollection.getReportMetricConfigs().size() > 0) {

        List<MetricDimensionReport> metricDimensionValueReports;
        // Used later to provide collection for a metric to help build the url link in report

        Map<String, MetricConfigDTO> metricMap = new HashMap<>();

        List<ContributorViewResponse> reports = new ArrayList<>();
        for (int i = 0; i < reportConfigCollection.getReportMetricConfigs().size(); i++) {
          ReportMetricConfig reportMetricConfig =
              reportConfigCollection.getReportMetricConfigs().get(i);
          MetricConfigDTO metricConfig =
              metricConfigManager.findById(reportMetricConfig.getMetricId());

          List<String> dimensions = reportMetricConfig.getDimensions();
          if (dimensions != null && dimensions.size() > 0) {
            for (String dimension : dimensions) {
              ContributorViewResponse report = EmailHelper
                  .getContributorDataForDataReport(metricConfig.getDataset(),
                      metricConfig.getName(), Arrays.asList(dimension),
                      makeFilters(reportMetricConfig.getFilters()),
                      reportMetricConfig.getCompareMode(),
                      alertConfig.getReportConfigCollection().getDelayOffsetMillis(),
                      alertConfig.getReportConfigCollection().isIntraDay());
              if (report != null) {
                metricMap.put(metricConfig.getName(), metricConfig);
                reports.add(report);
              }
            }
          }
        }
        if (reports.size() == 0) {
          LOG.warn("Could not fetch report data for " + alertConfig.getName());
          return;
        }
        long reportStartTs = reports.get(0).getTimeBuckets().get(0).getCurrentStart();
        metricDimensionValueReports =
            DataReportHelper.getInstance().getDimensionReportList(reports);
        for (int i = 0; i < metricDimensionValueReports.size(); i++) {
          MetricDimensionReport report = metricDimensionValueReports.get(i);
          report.setDataset(metricMap.get(report.getMetricName()).getDataset());
          long metricId = metricMap.get(report.getMetricName()).getId();
          report.setMetricId(metricId);
          for (ReportMetricConfig reportMetricConfig : reportConfigCollection
              .getReportMetricConfigs()) {
            if (reportMetricConfig.getMetricId() == metricId) {
              metricDimensionValueReports.get(i)
                  .setCompareMode(reportMetricConfig.getCompareMode().name());
            }
          }
        }
        Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
        freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector/");
        freemarkerConfig.setDefaultEncoding(CHARSET);
        freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        Map<String, Object> templateData = new HashMap<>();
        DateTimeZone timeZone = DateTimeZone.forTimeZone(DEFAULT_TIME_ZONE);
        DataReportHelper.DateFormatMethod dateFormatMethod =
            new DataReportHelper.DateFormatMethod(timeZone);
        templateData.put("timeZone", timeZone);
        templateData.put("dateFormat", dateFormatMethod);
        templateData.put("dashboardHost", thirdeyeConfig.getDashboardHost());
        templateData.put("fromEmail", alertConfig.getFromAddress());
        templateData.put("contactEmail", alertConfig.getReportConfigCollection().getContactEmail());
        templateData.put("reportStartDateTime", reportStartTs);
        templateData.put("metricDimensionValueReports", metricDimensionValueReports);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
          Template template = freemarkerConfig.getTemplate("data-report-by-metric-dimension.ftl");
          template.process(templateData, out);

          DetectionAlertFilterRecipients recipients = alertConfig.getReceiverAddresses();
          List<String> emailWhitelist = ConfigUtils.getList(
              this.thirdeyeConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY).get(EMAIL_WHITELIST_KEY));
          if (!emailWhitelist.isEmpty()) {
            recipients = retainWhitelisted(recipients, emailWhitelist);
          }

          // Send email
          HtmlEmail email = new HtmlEmail();
          String alertEmailSubject =
              String.format("Thirdeye data report : %s", alertConfig.getName());
          String alertEmailHtml = new String(baos.toByteArray(), CHARSET);
          EmailHelper.sendEmailWithHtml(email,
              SmtpConfiguration.createFromProperties(thirdeyeConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY)),
              alertEmailSubject,
              alertEmailHtml, alertConfig.getFromAddress(), recipients);

        } catch (Exception e) {
          throw new JobExecutionException(e);
        }
      }
    }
  }

  private void updateNotifiedStatus(List<MergedAnomalyResultDTO> mergedResults) {
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      mergedResult.setNotified(true);
      anomalyMergedResultDAO.update(mergedResult);
    }
  }

  private void sendFailureEmail(Throwable t) throws JobExecutionException {
    HtmlEmail email = new HtmlEmail();
    String subject = String
        .format("[ThirdEye Anomaly Detector] FAILED ALERT ID=%d for config %s", alertConfig.getId(),
            alertConfig.getName());
    String textBody = String
        .format("%s%n%nException:%s", alertConfig.toString(), ExceptionUtils.getStackTrace(t));
    try {
      EmailHelper.sendEmailWithTextBody(email,
          SmtpConfiguration.createFromProperties(thirdeyeConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY)),
          subject, textBody, thirdeyeConfig.getFailureFromAddress(),
          new DetectionAlertFilterRecipients(EmailUtils.getValidEmailAddresses(thirdeyeConfig.getFailureToAddress())));
    } catch (EmailException e) {
      throw new JobExecutionException(e);
    }
  }

  /**
   * Given a map, which maps from a dimension map to a grouped anomaly, of new GroupedAnomalies, this method performs
   * a time based merged with existing grouped anomalies, which are stored in a DB. Afterwards, if a merged grouped
   * anomaly passes through the filter, it is returned in a map.
   *
   * @param groupedAnomalyResultsMap a map of new GroupedAnomaly.
   * @param alertGroupConfig the configuration for alert group.
   *
   * @return a map of merged GroupedAnomaly that pass through the filter.
   */
  private Map<DimensionMap, GroupedAnomalyResultsDTO> timeBasedMergeAndFilterGroupedAnomalies(
      Map<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomalyResultsMap,
      AlertGroupConfig alertGroupConfig) {
    // Populate basic fields of the new grouped anomaly
    for (Map.Entry<DimensionMap, GroupedAnomalyResultsDTO> entry : groupedAnomalyResultsMap.entrySet()) {
      GroupedAnomalyResultsDTO groupedAnomaly = entry.getValue();
      DimensionMap dimensions = entry.getKey();
      groupedAnomaly.setAlertConfigId(alertConfig.getId());
      groupedAnomaly.setDimensions(dimensions);
    }

    Map<DimensionMap, GroupedAnomalyResultsDTO> mergedGroupedAnomalyResultsMap =
        this.timeBasedMergeGroupedAnomalyResults(groupedAnomalyResultsMap,
            alertGroupConfig.getGroupTimeBasedMergeConfig());
    // Read and update grouped anomalies in DB
    for (Map.Entry<DimensionMap, GroupedAnomalyResultsDTO> entry : mergedGroupedAnomalyResultsMap.entrySet()) {
      GroupedAnomalyResultsDTO groupedAnomaly = entry.getValue();
      Long groupId = groupedAnomalyResultsDAO.save(groupedAnomaly);
      groupedAnomaly.setId(groupId);
      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : groupedAnomaly.getAnomalyResults()) {
        if (!mergedAnomalyResultDTO.isNotified()) {
          mergedAnomalyResultDTO.setNotified(true);
          anomalyMergedResultDAO.update(mergedAnomalyResultDTO);
        }
      }
    }

    // Filter out the grouped anomalies that trigger an alert
    return this
        .filterMergedGroupedAnomalyResults(mergedGroupedAnomalyResultsMap, alertGroupConfig.getGroupFilterConfig());
  }

  /**
   * Given a map, which maps from a dimension map to a grouped anomaly, of new GroupedAnomalies, this method performs
   * a time based merged with existing grouped anomalies, which are stored in a DB.
   *
   * @param newGroupedAnomalies a map of new GroupedAnomaly.
   * @param mergeConfig the configuration for time-based merge strategy.
   *
   * @return a map of merged GroupedAnomaly in time dimensions.
   */
  private Map<DimensionMap, GroupedAnomalyResultsDTO> timeBasedMergeGroupedAnomalyResults(
      Map<DimensionMap, GroupedAnomalyResultsDTO> newGroupedAnomalies, Map<String, String> mergeConfig) {

    // Parse max allowed merge gap from config
    long maxAllowedMergeGap = DEFAULT_MAX_ALLOWED_MERGE_GAP;
    if (MapUtils.isNotEmpty(mergeConfig)) {
      if (mergeConfig.containsKey(MAX_ALLOWED_MERGE_GAP_KEY)) {
        try {
          Long value = Long.parseLong(mergeConfig.get(MAX_ALLOWED_MERGE_GAP_KEY));
          maxAllowedMergeGap = value;
        } catch (Exception e) {
          LOG.warn("Failed to parse {} as 'MAX_ALLOWED_MERGE_GAP_KEY'; Use default value {}",
              mergeConfig.get(MAX_ALLOWED_MERGE_GAP_KEY), DEFAULT_MAX_ALLOWED_MERGE_GAP);
        }
      }
    }

    // Retrieve the most recent grouped anomalies from DB
    // TODO: Get update time from merged anomaly after the field "updateTime" is updated in DB correctly
    Map<DimensionMap, GroupedAnomalyResultsDTO> recentGroupedAnomalies = new HashMap<>();
    for (Map.Entry<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomalyEntry : newGroupedAnomalies.entrySet()) {
      DimensionMap dimensions = groupedAnomalyEntry.getKey();
      GroupedAnomalyResultsDTO newGroupedAnomaly = groupedAnomalyEntry.getValue();

      long approximateUpdateTime = newGroupedAnomaly.getEndTime();
      GroupedAnomalyResultsDTO recentGroupedAnomaly = groupedAnomalyResultsDAO
          .findMostRecentInTimeWindow(alertConfig.getId(), dimensions.toString(),
              approximateUpdateTime - maxAllowedMergeGap, approximateUpdateTime);
      recentGroupedAnomalies.put(dimensions, recentGroupedAnomaly);
    }
    // Merge grouped anomalies
    return SimpleGroupedAnomalyMerger.timeBasedMergeGroupedAnomalyResults(recentGroupedAnomalies, newGroupedAnomalies);
  }

  /**
   * Given a map, which maps from a dimension map to a grouped anomaly, of new GroupedAnomalies, this method returns
   * the GroupedAnomalies that pass through the filter.
   *
   * @param mergedGroupedAnomalies a map of GroupedAnomaly.
   * @param filterConfig the configuration for group filter
   *
   * @return a map of GroupedAnomaly that pass through the filter.
   */
  private Map<DimensionMap, GroupedAnomalyResultsDTO> filterMergedGroupedAnomalyResults(
      Map<DimensionMap, GroupedAnomalyResultsDTO> mergedGroupedAnomalies, Map<String, String> filterConfig) {
    Map<DimensionMap, GroupedAnomalyResultsDTO> filteredGroupedAnomalies = new HashMap<>();
    AlertGroupFilter filter = AlertGroupFilterFactory.fromSpec(filterConfig);
    for (Map.Entry<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomalyEntry : mergedGroupedAnomalies.entrySet()) {
      GroupedAnomalyResultsDTO groupedAnomaly = groupedAnomalyEntry.getValue();
      if (!groupedAnomaly.isNotified()) {
        assert (groupedAnomalyEntry.getKey().equals(groupedAnomaly.getDimensions()));
        if (filter.isQualified(groupedAnomaly)) {
          filteredGroupedAnomalies.put(groupedAnomalyEntry.getKey(), groupedAnomaly);
        }
      }
    }
    return filteredGroupedAnomalies;
  }

  /**
   * Retain whitelisted email addresses for email recipient string only.
   *
   * @param recipients email recipient
   * @param whitelist whitelisted recipients
   * @return whitelisted recipients
   */
  private static DetectionAlertFilterRecipients retainWhitelisted(DetectionAlertFilterRecipients recipients, Collection<String> whitelist) {
    if (recipients == null) {
      return null;
    }

    recipients.setTo(retainWhitelisted(recipients.getTo(), whitelist));
    recipients.setCc(retainWhitelisted(recipients.getCc(), whitelist));
    recipients.setBcc(retainWhitelisted(recipients.getBcc(), whitelist));

    return recipients;
  }

  private static Set<String> retainWhitelisted(Set<String> recipients, Collection<String> whitelist) {
    if (recipients != null) {
      recipients.retainAll(whitelist);
    }
    return recipients;
  }

  /**
   * Returns a filter multimap from a map of collections.
   *
   * @param filterMap map of collections (dimension name to dimension values)
   * @return filter multimap
   */
  private static SetMultimap<String, String> makeFilters(Map<String, Collection<String>> filterMap) {
    SetMultimap<String, String> filters = HashMultimap.create();

    if (filterMap == null) {
      return filters;
    }

    for (Map.Entry<String, Collection<String>> entry : filterMap.entrySet()) {
      filters.putAll(entry.getKey(), entry.getValue());
    }

    return filters;
  }
}
