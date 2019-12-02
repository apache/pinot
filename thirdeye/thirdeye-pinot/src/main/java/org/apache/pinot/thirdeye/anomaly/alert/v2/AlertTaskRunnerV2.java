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

package org.apache.pinot.thirdeye.anomaly.alert.v2;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.pinot.thirdeye.notification.commons.SmtpConfiguration;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.alert.AlertTaskInfo;
import org.apache.pinot.thirdeye.anomaly.alert.template.pojo.MetricDimensionReport;
import org.apache.pinot.thirdeye.anomaly.alert.util.DataReportHelper;
import org.apache.pinot.thirdeye.anomaly.alert.util.EmailHelper;
import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.anomaly.task.TaskInfo;
import org.apache.pinot.thirdeye.anomaly.task.TaskResult;
import org.apache.pinot.thirdeye.anomaly.task.TaskRunner;
import org.apache.pinot.thirdeye.anomaly.utils.EmailUtils;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean.ReportConfigCollection;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean.ReportMetricConfig;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTimeZone;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.notification.commons.SmtpConfiguration.SMTP_CONFIG_KEY;

/**
 * This method of sending emails is deprecated. This class will be removed once
 * we migrate/deprecate the daily data report.
 *
 * @deprecated use {@link org.apache.pinot.thirdeye.detection.alert.scheme.DetectionEmailAlerter} instead.
 */
@Deprecated
public class AlertTaskRunnerV2 implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AlertTaskRunnerV2.class);

  public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone("America/Los_Angeles");
  public static final String CHARSET = "UTF-8";
  public static final String EMAIL_WHITELIST_KEY = "emailWhitelist";

  private final AlertConfigManager alertConfigDAO;
  private final MetricConfigManager metricConfigManager;

  private AlertConfigDTO alertConfig;
  private ThirdEyeAnomalyConfiguration thirdeyeConfig;


  public AlertTaskRunnerV2() {
    alertConfigDAO = DAORegistry.getInstance().getAlertConfigDAO();
    metricConfigManager = DAORegistry.getInstance().getMetricConfigDAO();
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext)
      throws Exception {
    List<TaskResult> taskResult = new ArrayList<>();
    AlertTaskInfo alertTaskInfo = (AlertTaskInfo) taskInfo;
    // Fetch the latest alert config instead of the one provided by task context, which could be out-of-dated.
    alertConfig = alertConfigDAO.findById(alertTaskInfo.getAlertConfigDTO().getId());
    thirdeyeConfig = taskContext.getThirdEyeAnomalyConfiguration();

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
      sendScheduledDataReport();
    } finally {
      ThirdeyeMetricsUtil.alertTaskSuccessCounter.inc();
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
        freemarkerConfig.setClassForTemplateLoading(getClass(), "/org/apache/pinot/thirdeye/detector/");
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
