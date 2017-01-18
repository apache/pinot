package com.linkedin.thirdeye.anomaly.alert;

import com.linkedin.thirdeye.anomaly.alert.template.pojo.MetricDimensionReport;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.alert.util.DataReportHelper;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

public class AlertTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AlertTaskRunner.class);

  private static final DAORegistry daoRegistry = DAORegistry.getInstance();

  private final MergedAnomalyResultManager anomalyMergedResultDAO;
  private final EmailConfigurationManager emailConfigurationDAO;

  private EmailConfigurationDTO alertConfig;
  private DateTime windowStart;
  private DateTime windowEnd;
  private ThirdEyeAnomalyConfiguration thirdeyeConfig;

  public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone("America/Los_Angeles");
  public static final String CHARSET = "UTF-8";

  public AlertTaskRunner() {
    anomalyMergedResultDAO = daoRegistry.getMergedAnomalyResultDAO();
    emailConfigurationDAO = daoRegistry.getEmailConfigurationDAO();
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    AlertTaskInfo alertTaskInfo = (AlertTaskInfo) taskInfo;
    List<TaskResult> taskResult = new ArrayList<>();
    alertConfig = alertTaskInfo.getAlertConfig();
    windowStart = alertTaskInfo.getWindowStartTime();
    windowEnd = alertTaskInfo.getWindowEndTime();
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

  private void runTask() throws Exception {
    LOG.info("Starting email report {}", alertConfig.getId());

    final String collection = alertConfig.getCollection();

    // Get the anomalies in that range
    final List<MergedAnomalyResultDTO> allResults = anomalyMergedResultDAO
        .getAllByTimeEmailIdAndNotifiedFalse(windowStart.getMillis(), windowEnd.getMillis(),
            alertConfig.getId());

    // apply filtration rule
    List<MergedAnomalyResultDTO> results = AlertFilterHelper.applyFiltrationRule(allResults);

    if (results.isEmpty() && !alertConfig.isSendZeroAnomalyEmail()) {
      LOG.info("Zero anomalies found, skipping sending email");
      return;
    }

    // Group by dimension key, then sort according to anomaly result compareTo method.
    Map<DimensionMap, List<MergedAnomalyResultDTO>> groupedResults = new TreeMap<>();
    for (MergedAnomalyResultDTO result : results) {
      DimensionMap dimensions = result.getDimensions();
      if (!groupedResults.containsKey(dimensions)) {
        groupedResults.put(dimensions, new ArrayList<>());
      }
      groupedResults.get(dimensions).add(result);
    }
    // sort each list of anomaly results afterwards
    for (List<MergedAnomalyResultDTO> resultsByExploredDimensions : groupedResults.values()) {
      Collections.sort(resultsByExploredDimensions);
    }
    sendAlertForAnomalies(collection, results, groupedResults);
    updateNotifiedStatus(results);
  }

  private void sendAlertForAnomalies(String collectionAlias, List<MergedAnomalyResultDTO> results,
      Map<DimensionMap, List<MergedAnomalyResultDTO>> groupedResults)
      throws JobExecutionException {

    long anomalyStartMillis = 0;
    long anomalyEndMillis = 0;
    int anomalyResultSize = 0;
    if (CollectionUtils.isNotEmpty(results)) {
      anomalyResultSize = results.size();
      anomalyStartMillis = results.get(0).getStartTime();
      anomalyEndMillis = results.get(0).getEndTime();
      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : results) {
        if (mergedAnomalyResultDTO.getStartTime() < anomalyStartMillis) {
          anomalyStartMillis = mergedAnomalyResultDTO.getStartTime();
        }
        if (mergedAnomalyResultDTO.getEndTime() > anomalyEndMillis) {
          anomalyEndMillis = mergedAnomalyResultDTO.getEndTime();
        }
      }
    }

    DateTimeZone timeZone = DateTimeZone.forTimeZone(DEFAULT_TIME_ZONE);
    DataReportHelper.DateFormatMethod dateFormatMethod = new DataReportHelper.DateFormatMethod(timeZone);

    HtmlEmail email = new HtmlEmail();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector/");
      freemarkerConfig.setDefaultEncoding(CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Map<String, Object> templateData = new HashMap<>();

      String metric = alertConfig.getMetric();
      String windowUnit = alertConfig.getWindowUnit().toString();
      templateData.put("groupedAnomalyResults", DataReportHelper.convertToStringKeyBasedMap(groupedResults));
      templateData.put("anomalyCount", anomalyResultSize);
      templateData.put("startTime", anomalyStartMillis);
      templateData.put("endTime", anomalyEndMillis);
      templateData.put("reportGenerationTimeMillis", System.currentTimeMillis());
      templateData.put("dateFormat", dateFormatMethod);
      templateData.put("timeZone", timeZone);
      templateData.put("collection", collectionAlias);
      templateData.put("metric", metric);
      templateData.put("windowUnit", windowUnit);
      templateData.put("dashboardHost", thirdeyeConfig.getDashboardHost());

      if (alertConfig.isReportEnabled() & alertConfig.getDimensions() != null) {
        long reportStartTs = 0;
        List<MetricDimensionReport> metricDimensionValueReports;
        List<ContributorViewResponse> reports = new ArrayList<>();
        for (String dimension : alertConfig.getDimensions()) {
          ContributorViewResponse report = EmailHelper
              .getContributorData(collectionAlias, alertConfig.getMetric(), Arrays.asList(dimension));
          if(report != null) {
            reports.add(report);
          }
        }
        reportStartTs = reports.get(0).getTimeBuckets().get(0).getCurrentStart();
        metricDimensionValueReports = DataReportHelper.getDimensionReportList(reports);
        templateData.put("metricDimensionValueReports", metricDimensionValueReports);
        templateData.put("reportStartDateTime", reportStartTs);
      }

      Template template = freemarkerConfig.getTemplate("anomaly-report.ftl");
      template.process(templateData, out);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    // Send email
    try {
      String alertEmailSubject;
      if (results.size() > 0) {
      String anomalyString = (results.size() == 1) ? "anomaly" : "anomalies";
        alertEmailSubject = String
            .format("Thirdeye: %s: %s - %d %s detected", alertConfig.getMetric(), collectionAlias,
                results.size(), anomalyString);
      } else {
        alertEmailSubject = String
            .format("Thirdeye data report : %s: %s", alertConfig.getMetric(), collectionAlias);
      }

      String alertEmailHtml = new String(baos.toByteArray(), CHARSET);
      EmailHelper.sendEmailWithHtml(email, thirdeyeConfig.getSmtpConfiguration(), alertEmailSubject,
          alertEmailHtml, alertConfig.getFromAddress(), alertConfig.getToAddresses());
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    // once email is sent, update the last merged anomaly id as watermark in email config
    long anomalyId = 0;
    for (MergedAnomalyResultDTO anomalyResultDTO : results) {
      if (anomalyResultDTO.getId() > anomalyId) {
        anomalyId = anomalyResultDTO.getId();
      }
    }
    alertConfig.setLastNotifiedAnomalyId(anomalyId);
    emailConfigurationDAO.update(alertConfig);

    LOG.info("Sent email with {} anomalies! {}", results.size(), alertConfig);
  }

  // TODO : deprecate this, move last notified alert id in the alertConfig
  private void updateNotifiedStatus(List<MergedAnomalyResultDTO> mergedResults) {
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      mergedResult.setNotified(true);
      anomalyMergedResultDAO.update(mergedResult);
    }
  }

  private void sendFailureEmail(Throwable t) throws JobExecutionException {
    HtmlEmail email = new HtmlEmail();
    String collection = alertConfig.getCollection();
    String metric = alertConfig.getMetric();

    String subject = String
        .format("[ThirdEye Anomaly Detector] FAILED ALERT ID=%d (%s:%s)", alertConfig.getId(),
            collection, metric);
    String textBody = String
        .format("%s%n%nException:%s", alertConfig.toString(), ExceptionUtils.getStackTrace(t));
    try {
      EmailHelper
          .sendEmailWithTextBody(email, thirdeyeConfig.getSmtpConfiguration(), subject, textBody,
              thirdeyeConfig.getFailureFromAddress(), thirdeyeConfig.getFailureToAddress());
    } catch (EmailException e) {
      throw new JobExecutionException(e);
    }
  }
}
