package com.linkedin.thirdeye.anomaly.alert.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskRunner;
import com.linkedin.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.dashboard.Utils;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class AnomalyReportGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(AnomalyReportGenerator.class);

  private static final AnomalyReportGenerator INSTANCE = new AnomalyReportGenerator();
  private static final String DATE_PATTERN = "MMM dd, HH:mm";
  private static final String MULTIPLE_ANOMALIES_EMAIL_TEMPLATE = "multiple-anomalies-email-template.ftl";

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
    String subject = "Thirdeye anomaly report : " + alertConfig.getName();
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
        alertConfig.getRecipients(), alertConfig.getFromAddress(), alertConfig.getName(), false);
  }




  public void buildReport(long startTime, long endTime, List<MergedAnomalyResultDTO> anomalies,
      String subject, ThirdEyeAnomalyConfiguration configuration, boolean includeSentAnomaliesOnly,
      String emailRecipients, String fromEmail, String alertConfigName, boolean includeSummary) {
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
      List<String> anomalyIds = new ArrayList<>();
      Set<String> datasets = new HashSet<>();
      for (MergedAnomalyResultDTO anomaly : anomalies) {
        metrics.add(anomaly.getMetric());
        datasets.add(anomaly.getCollection());

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
            anomaly.getFeedback() == null ? "Not Resolved" : "Resolved(" + anomaly.getFeedback().getFeedbackType().name() + ")");

        DateTimeZone dateTimeZone = Utils.getDataTimeZone(anomaly.getCollection());
        AnomalyReportDTO anomalyReportDTO = new AnomalyReportDTO(String.valueOf(anomaly.getId()),
            getAnomalyURL(anomaly, configuration.getDashboardHost()),
            String.format("%.2f", anomaly.getAvgBaselineVal()),
            String.format("%.2f", anomaly.getAvgCurrentVal()),
            getDimensionsList(anomaly.getDimensions()),
            String.format("%.2f hours (%s to %s) %s",
                getTimeDiffInHours(anomaly.getStartTime(), anomaly.getEndTime()),
                getDateString(anomaly.getStartTime(), dateTimeZone),
                getDateString(anomaly.getEndTime(), dateTimeZone),
                getTimezoneString(dateTimeZone)), // duration
            feedbackVal,
            anomaly.getFunction().getFunctionName(),
            String.format("%+.2f%%", anomaly.getWeight() * 100), // lift
            getLiftDirection(anomaly.getWeight()),
            anomaly.getMetric(),
            getDateString(anomaly.getStartTime(), dateTimeZone),
            getDateString(anomaly.getEndTime(), dateTimeZone),
            getTimezoneString(dateTimeZone)
        );


        if (anomaly.isNotified()) {
          alertedAnomalies++;
        }
        // include notified alerts only in the email
        if (includeSentAnomaliesOnly) {
          if (anomaly.isNotified()) {
            anomalyReportDTOList.add(anomalyReportDTO);
            anomalyIds.add(anomalyReportDTO.getAnomalyId());
          }
        } else {
          anomalyReportDTOList.add(anomalyReportDTO);
          anomalyIds.add(anomalyReportDTO.getAnomalyId());
        }
      }

      HtmlEmail email = new HtmlEmail();

      DateTimeZone timeZone = DateTimeZone.forTimeZone(AlertTaskRunnerV2.DEFAULT_TIME_ZONE);
      DataReportHelper.DateFormatMethod dateFormatMethod = new DataReportHelper.DateFormatMethod(timeZone);
      Map<String, Object> templateData = new HashMap<>();
      templateData.put("datasets", Joiner.on(", ").join(datasets));
      templateData.put("timeZone", getTimezoneString(timeZone));
      templateData.put("dateFormat", dateFormatMethod);
      templateData.put("startTime", getDateString(startTime, timeZone));
      templateData.put("endTime", getDateString(endTime, timeZone));
      templateData.put("anomalyCount", anomalies.size());
      templateData.put("metricsCount", metrics.size());
      templateData.put("notifiedCount", alertedAnomalies);
      templateData.put("feedbackCount", feedbackCollected);
      templateData.put("trueAlertCount", trueAlert);
      templateData.put("falseAlertCount", falseAlert);
      templateData.put("nonActionableCount", nonActionable);
      templateData.put("anomalyDetails", anomalyReportDTOList);
      templateData.put("alertConfigName", alertConfigName);
      templateData.put("includeSummary", includeSummary);
      templateData.put("reportGenerationTimeMillis", System.currentTimeMillis());
      templateData.put("dashboardHost", configuration.getDashboardHost());
      templateData.put("anomalyIds", Joiner.on(",").join(anomalyIds));

      String imgPath = null;
      String cid = "";
      if (anomalyReportDTOList.size() == 1) {
        AnomalyReportDTO singleAnomaly = anomalyReportDTOList.get(0);
        subject = subject + " - " + singleAnomaly.getMetric();
        imgPath = EmailScreenshotHelper.takeGraphScreenShot(singleAnomaly.getAnomalyId(), configuration);
        try {
          cid = email.embed(new File(imgPath));
        } catch (Exception e) {
          LOG.error("Exception while embedding screenshot for anomaly {}", singleAnomaly.getAnomalyId(), e);
        }
      }
      templateData.put("cid", cid);

      buildEmailTemplateAndSendAlert(templateData, configuration.getSmtpConfiguration(), subject,
          emailRecipients, fromEmail, email);

      if (StringUtils.isNotBlank(imgPath)) {
        try {
          Files.deleteIfExists(new File(imgPath).toPath());
        } catch (IOException e) {
          LOG.error("Exception in deleting screenshot {}", imgPath, e);
        }
      }
    }
  }

  void buildEmailTemplateAndSendAlert(Map<String, Object> paramMap,
      SmtpConfiguration smtpConfiguration, String subject, String emailRecipients,
      String fromEmail, HtmlEmail email) {
    if (Strings.isNullOrEmpty(fromEmail)) {
      throw new IllegalArgumentException("Invalid sender's email");
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (Writer out = new OutputStreamWriter(baos, AlertTaskRunner.CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector");
      freemarkerConfig.setDefaultEncoding(AlertTaskRunner.CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Template template = freemarkerConfig.getTemplate(MULTIPLE_ANOMALIES_EMAIL_TEMPLATE);

      template.process(paramMap, out);

      String alertEmailHtml = new String(baos.toByteArray(), AlertTaskRunner.CHARSET);
      EmailHelper.sendEmailWithHtml(email, smtpConfiguration, subject, alertEmailHtml, fromEmail,
          emailRecipients);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  private String getDateString(Long millis, DateTimeZone dateTimeZone) {
    String dateString = new DateTime(millis, dateTimeZone).toString(DATE_PATTERN);
    return dateString;
  }

  private double getTimeDiffInHours(long start, long end) {
    return Double.valueOf((end - start) / 1000) / 3600;
  }

  private List<String> getDimensionsList(DimensionMap dimensionMap) {
    List<String> dimensionsList = new ArrayList<>();
    if (dimensionMap != null && !dimensionMap.isEmpty()) {
      for (Entry<String, String> entry : dimensionMap.entrySet()) {
        dimensionsList.add(entry.getKey() + " : " + entry.getValue());
      }
    }
    return dimensionsList;
  }

  private boolean getLiftDirection(double lift) {
    return lift < 0 ? false : true;
  }

  private String getTimezoneString(DateTimeZone dateTimeZone) {
    TimeZone tz = TimeZone.getTimeZone(dateTimeZone.getID());
    return tz.getDisplayName(true, 0);
  }

  private String getFeedback(String feedbackType) {
    switch (feedbackType) {
    case "ANOMALY":
      return "Resolved (Confirmed Anomaly)";
    case "NOT_ANOMALY":
      return "Resolved (False Alarm)";
    case "ANOMALY_NO_ACTION":
      return "Not Actionable";
    }
    return "Not Resolved";
  }

  private String getAnomalyURL(MergedAnomalyResultDTO anomalyResultDTO, String dashboardUrl) {
    String urlPart = "/thirdeye#investigate?anomalyId=";
    return dashboardUrl + urlPart;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AnomalyReportDTO {
    String metric;
    String startDateTime;
    String lift;
    boolean positiveLift;
    String feedback;
    String anomalyId;
    String anomalyURL;
    String currentVal;
    String baselineVal;
    List<String> dimensions;
    String function;
    String duration;
    String startTime;
    String endTime;
    String timezone;


    public AnomalyReportDTO(String anomalyId, String anomalyURL, String baselineVal,
        String currentVal, List<String> dimensions, String duration, String feedback, String function,
        String lift, boolean positiveLift, String metric, String startTime, String endTime, String timezone) {
      this.anomalyId = anomalyId;
      this.anomalyURL = anomalyURL;
      this.baselineVal = baselineVal;
      this.currentVal = currentVal;
      this.dimensions = dimensions;
      this.duration = duration;
      this.feedback = feedback;
      this.function = function;
      this.lift = lift;
      this.positiveLift = positiveLift;
      this.metric = metric;
      this.startDateTime = startTime;
      this.endTime = endTime;
      this.timezone = timezone;
    }

    public String getBaselineVal() {
      return baselineVal;
    }

    public void setBaselineVal(String baselineVal) {
      this.baselineVal = baselineVal;
    }

    public String getCurrentVal() {
      return currentVal;
    }

    public void setCurrentVal(String currentVal) {
      this.currentVal = currentVal;
    }

    public List<String> getDimensions() {
      return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
      this.dimensions = dimensions;
    }

    public String getDuration() {
      return duration;
    }

    public void setDuration(String duration) {
      this.duration = duration;
    }

    public String getFunction() {
      return function;
    }

    public void setFunction(String function) {
      this.function = function;
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


    public boolean isPositiveLift() {
      return positiveLift;
    }

    public void setPositiveLift(boolean positiveLift) {
      this.positiveLift = positiveLift;
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

    public String getAnomalyURL() {
      return anomalyURL;
    }

    public void setAnomalyURL(String anomalyURL) {
      this.anomalyURL = anomalyURL;
    }

    public String getStartTime() {
      return startTime;
    }

    public void setStartTime(String startTime) {
      this.startTime = startTime;
    }

    public String getEndTime() {
      return endTime;
    }

    public void setEndTime(String endTime) {
      this.endTime = endTime;
    }

    public String getTimezone() {
      return timezone;
    }

    public void setTimezone(String timezone) {
      this.timezone = timezone;
    }


  }

}

