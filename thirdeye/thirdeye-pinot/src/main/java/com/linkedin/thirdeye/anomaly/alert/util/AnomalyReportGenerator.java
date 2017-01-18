package com.linkedin.thirdeye.anomaly.alert.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskRunner;
import com.linkedin.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnomalyReportGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(AnomalyReportGenerator.class);

  private static final AnomalyReportGenerator INSTANCE = new AnomalyReportGenerator();

  public static AnomalyReportGenerator getInstance() {
    return INSTANCE;
  }

  MergedAnomalyResultManager anomalyResultManager =
      DAORegistry.getInstance().getMergedAnomalyResultDAO();
  MetricConfigManager metricConfigManager = DAORegistry.getInstance().getMetricConfigDAO();

  public List<MergedAnomalyResultDTO> getAnomaliesForDatasets(List<String> collections,
      long startTime, long endTime) {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    for (String collection : collections) {
      anomalies
          .addAll(anomalyResultManager.findByCollectionTime(collection, startTime, endTime, false));
    }
    return anomalies;
  }

  public List<MergedAnomalyResultDTO> getAnomaliesForMetrics(List<String> metrics, long startTime,
      long endTime) {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    LOG.info("fetching anomalies for metrics : " + metrics);
    for (String metric : metrics) {
      List<MetricConfigDTO> metricConfigDTOList = metricConfigManager.findByMetricName(metric);
      for (MetricConfigDTO metricConfigDTO : metricConfigDTOList) {
        List<MergedAnomalyResultDTO> results = anomalyResultManager
            .findByCollectionMetricTime(metricConfigDTO.getDataset(), metric, startTime, endTime,
                false);
        LOG.info("Found {} result for metric {}", results.size(), metric);
        anomalies.addAll(results);
      }
    }
    return anomalies;
  }

  public void buildReport(List<MergedAnomalyResultDTO> anomalies,
      ThirdEyeAnomalyConfiguration configuration, AlertConfigDTO alertConfig) {
    String subject = "Thirdeye report : " + alertConfig.getName();
    long startTime = System.currentTimeMillis();
    long endTime = 0;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomaly.getStartTime() < startTime) {
        startTime = anomaly.getStartTime();
      }
      if (anomaly.getEndTime() > endTime) {
        endTime = anomaly.getEndTime();
      }
    }
    buildReport(startTime, endTime, anomalies, subject, configuration, false,
        alertConfig.getRecipients(), alertConfig.getFromAddress(), alertConfig.getName());
  }

  public void buildReport(long startTime, long endTime, List<MergedAnomalyResultDTO> anomalies,
      String subject, ThirdEyeAnomalyConfiguration configuration, boolean includeSentAnomaliesOnly,
      String emailRecipients, String fromEmail, String alertConfigName) {
    if (anomalies == null || anomalies.size() == 0) {
      LOG.info("No anomalies found to send email, please check the parameters.. exiting");
    } else {
      Set<String> metrics = new HashSet<>();
      int alertedAnomalies = 0;
      int feedbackCollected = 0;
      int trueAlert = 0;
      int falseAlert = 0;
      int nonActionable = 0;

      List<AnomalyReportDTO> anomalyReportDTOList = new ArrayList<>();

      for (MergedAnomalyResultDTO anomaly : anomalies) {
        metrics.add(anomaly.getMetric());
        if (anomaly.getFeedback() != null) {
          feedbackCollected++;
          if (anomaly.getFeedback().getFeedbackType().equals(AnomalyFeedbackType.ANOMALY)) {
            trueAlert++;
          } else if (anomaly.getFeedback().getFeedbackType()
              .equals(AnomalyFeedbackType.NOT_ANOMALY)) {
            falseAlert++;
          } else {
            nonActionable++;
          }
        }
        String feedbackVal = getFeedback(
            anomaly.getFeedback() == null ? "NA" : anomaly.getFeedback().getFeedbackType().name());

        AnomalyReportDTO anomalyReportDTO =
            new AnomalyReportDTO(String.valueOf(anomaly.getId()), feedbackVal,
                String.format("%+.2f", anomaly.getWeight()), anomaly.getMetric(),
                new Date(anomaly.getStartTime()).toString(), String
                .format("%.2f", getTimeDiffInHours(anomaly.getStartTime(), anomaly.getEndTime())),
                getAnomalyURL(anomaly, configuration.getDashboardHost()));

        if (anomaly.isNotified()) {
          alertedAnomalies++;
        }
        // include notified alerts only in the email
        if (includeSentAnomaliesOnly) {
          if (anomaly.isNotified()) {
            anomalyReportDTOList.add(anomalyReportDTO);
          }
        } else {
          anomalyReportDTOList.add(anomalyReportDTO);
        }
      }

      Map<String, Object> templateData = new HashMap<>();
      DateTimeZone timeZone = DateTimeZone.forTimeZone(AlertTaskRunnerV2.DEFAULT_TIME_ZONE);
      DataReportHelper.DateFormatMethod dateFormatMethod = new DataReportHelper.DateFormatMethod(timeZone);
      templateData.put("timeZone", timeZone);
      templateData.put("dateFormat", dateFormatMethod);
      templateData.put("startTime", new Date(startTime));
      templateData.put("endTime", new Date(endTime));
      templateData.put("anomalyCount", anomalies.size());
      templateData.put("metricsCount", metrics.size());
      templateData.put("notifiedCount", alertedAnomalies);
      templateData.put("feedbackCount", feedbackCollected);
      templateData.put("trueAlertCount", trueAlert);
      templateData.put("falseAlertCount", falseAlert);
      templateData.put("nonActionableCount", nonActionable);
      templateData.put("anomalyDetails", anomalyReportDTOList);
      templateData.put("alertConfigName", alertConfigName);
      templateData.put("reportGenerationTimeMillis", System.currentTimeMillis());
      buildEmailTemplateAndSendAlert(templateData, configuration.getSmtpConfiguration(), subject,
          emailRecipients, fromEmail);
    }
  }

  void buildEmailTemplateAndSendAlert(Map<String, Object> paramMap,
      SmtpConfiguration smtpConfiguration, String subject, String emailRecipients,
      String fromEmail) {
    if (Strings.isNullOrEmpty(fromEmail)) {
      throw new IllegalArgumentException("Invalid sender's email");
    }
    HtmlEmail email = new HtmlEmail();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (Writer out = new OutputStreamWriter(baos, AlertTaskRunner.CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector");
      freemarkerConfig.setDefaultEncoding(AlertTaskRunner.CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Template template = freemarkerConfig.getTemplate("custom-anomaly-report.ftl");
      template.process(paramMap, out);

      String alertEmailHtml = new String(baos.toByteArray(), AlertTaskRunner.CHARSET);
      EmailHelper.sendEmailWithHtml(email, smtpConfiguration, subject, alertEmailHtml, fromEmail,
          emailRecipients);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  double getTimeDiffInHours(long start, long end) {
    return Double.valueOf((end - start) / 1000) / 3600;
  }

  String getFeedback(String feedbackType) {
    switch (feedbackType) {
    case "ANOMALY":
      return "Confirmed Anomaly";
    case "NOT_ANOMALY":
      return "False Alarm";
    case "ANOMALY_NO_ACTION":
      return "Not Actionable";
    }
    return "NA";
  }

  String getAnomalyURL(MergedAnomalyResultDTO anomalyResultDTO, String dashboardUrl) {
    String urlPart =
        "/dashboard#view=anomalies&dataset=%s&metrics=%s&currentStart=%s&currentEnd=%s";
    return dashboardUrl + String
        .format(urlPart, anomalyResultDTO.getCollection(), anomalyResultDTO.getMetric(),
            anomalyResultDTO.getStartTime(), anomalyResultDTO.getEndTime());
  }

  @JsonIgnoreProperties(ignoreUnknown = true) public static class AnomalyReportDTO {
    String metric;
    String startDateTime;
    String windowSize;
    String lift;
    String feedback;
    String anomalyId;
    String anomalyURL;

    public AnomalyReportDTO(String anomalyId, String feedback, String lift, String metric,
        String startDateTime, String windowSize, String anomalyURL) {
      this.anomalyId = anomalyId;
      this.feedback = feedback;
      this.lift = lift;
      this.metric = metric;
      this.startDateTime = startDateTime;
      this.windowSize = windowSize;
      this.anomalyURL = anomalyURL;
    }

    public String getAnomalyId() {
      return anomalyId;
    }

    public void setAnomalyId(String anomalyId) {
      this.anomalyId = anomalyId;
    }

    public String getFeedback() {
      return feedback;
    }

    public void setFeedback(String feedback) {
      this.feedback = feedback;
    }

    public String getLift() {
      return lift;
    }

    public void setLift(String lift) {
      this.lift = lift;
    }

    public String getMetric() {
      return metric;
    }

    public void setMetric(String metric) {
      this.metric = metric;
    }

    public String getStartDateTime() {
      return startDateTime;
    }

    public void setStartDateTime(String startDateTime) {
      this.startDateTime = startDateTime;
    }

    public String getWindowSize() {
      return windowSize;
    }

    public void setWindowSize(String windowSize) {
      this.windowSize = windowSize;
    }

    public String getAnomalyURL() {
      return anomalyURL;
    }

    public void setAnomalyURL(String anomalyURL) {
      this.anomalyURL = anomalyURL;
    }
  }
}
