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

package com.linkedin.thirdeye.tools.anomaly.report;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.Validation;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.mail.HtmlEmail;

import static com.linkedin.thirdeye.anomaly.SmtpConfiguration.SMTP_CONFIG_KEY;


public class GenerateAnomalyReport {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

  MergedAnomalyResultManager anomalyResultManager;
  MetricConfigManager metricConfigManager;

  Date startTime;
  Date endTime;
  List<String> collections;
  String dashboardUrl;
  SmtpConfiguration smtpConfiguration;
  DetectionAlertFilterRecipients emailRecipients;

  public GenerateAnomalyReport(Date startTime, Date endTime, File persistenceConfig,
      List<String> datasets, String dashboardUrl, SmtpConfiguration smtpConfiguration,
      DetectionAlertFilterRecipients emailRecipients) {
    DaoProviderUtil.init(persistenceConfig);
    anomalyResultManager = DaoProviderUtil.getInstance(MergedAnomalyResultManagerImpl.class);
    metricConfigManager = DaoProviderUtil.getInstance(MetricConfigManagerImpl.class);

    this.startTime = startTime;
    this.endTime = endTime;
    this.collections = datasets;
    this.dashboardUrl = dashboardUrl;
    this.smtpConfiguration = smtpConfiguration;
    this.emailRecipients = emailRecipients;

    System.out.println(
        "building report : " + startTime + " -- " + endTime + ", for collections " + datasets);
  }

  void listMetric() {
    for (String collection : collections) {
      List<MetricConfigDTO> metrics = metricConfigManager.findActiveByDataset(collection);
      for (MetricConfigDTO metric : metrics) {
        System.out.println(collection + "," + metric.getName());
      }
    }
  }

  void buildReport() {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    for (String collection : collections) {
      anomalies.addAll(anomalyResultManager
          .findByCollectionTime(collection, startTime.getTime(), endTime.getTime()));
    }

    if (anomalies.size() == 0) {
      System.out.println("No anomalies found, please check the report config... exiting");
    }
    else {
      Set<String> metrics = new HashSet<>();
      int alertedAnomalies = 0;
      int feedbackCollected = 0;
      int trueAlert = 0;
      int falseAlert = 0;
      int newTrend = 0;

      List<AnomalyReportDTO> anomalyReportDTOList = new ArrayList<>();

      for (MergedAnomalyResultDTO anomaly : anomalies) {
        metrics.add(anomaly.getMetric());
        AnomalyFeedback feedback = anomaly.getFeedback();
        if (feedback != null) {
          feedbackCollected++;
          if (feedback.getFeedbackType().equals(AnomalyFeedbackType.ANOMALY)) {
            trueAlert++;
          } else if (feedback.getFeedbackType().equals(AnomalyFeedbackType.NOT_ANOMALY)) {
            falseAlert++;
          } else {
            newTrend++;
          }
        }
        String feedbackVal = getFeedback(
            feedback == null ? "NA" : feedback.getFeedbackType().name());

        AnomalyReportDTO anomalyReportDTO =
            new AnomalyReportDTO(String.valueOf(anomaly.getId()), feedbackVal,
                String.format("%+.2f", anomaly.getWeight()), anomaly.getMetric(), new Date(anomaly.getStartTime()).toString(),
                String.format("%.2f", getTimeDiffInHours(anomaly.getStartTime(), anomaly.getEndTime())),
                getAnomalyURL(anomaly));

       // anomalyReportDTOList.add(anomalyReportDTO);

        // include notified alerts only in the email
        if (anomaly.isNotified()) {
          alertedAnomalies++;
          anomalyReportDTOList.add(anomalyReportDTO);
        }
      }

      Map<String, Object> templateData = new HashMap<>();
      templateData.put("startTime", startTime.toString());
      templateData.put("endTime", endTime.toString());
      templateData.put("anomalyCount", anomalies.size());
      templateData.put("metricsCount", metrics.size());
      templateData.put("notifiedCount", alertedAnomalies);
      templateData.put("feedbackCount", feedbackCollected);
      templateData.put("trueAlertCount", trueAlert);
      templateData.put("falseAlertCount", falseAlert);
      templateData.put("newTrendCount", newTrend);
      templateData.put("datasets", StringUtils.join(collections, ", "));
      templateData.put("anomalyDetails", anomalyReportDTOList);
      buildEmailTemplateAndSendAlert(templateData);
    }
  }

  void buildEmailTemplateAndSendAlert(Map<String, Object> paramMap) {
    HtmlEmail email = new HtmlEmail();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (Writer out = new OutputStreamWriter(baos, AlertTaskRunnerV2.CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector");
      freemarkerConfig.setDefaultEncoding(AlertTaskRunnerV2.CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Template template = freemarkerConfig.getTemplate("custom-anomaly-report.ftl");
      template.process(paramMap, out);

      String alertEmailSubject =
          "Thirdeye : Daily anomaly report";
      String alertEmailHtml = new String(baos.toByteArray(), AlertTaskRunnerV2.CHARSET);
      EmailHelper.sendEmailWithHtml(email, smtpConfiguration, alertEmailSubject, alertEmailHtml,
          "thirdeye-dev@linkedin.com", emailRecipients);
    } catch (Exception e) {
      e.printStackTrace();
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
    case "ANOMALY_NEW_TREND":
      return "Anomaly New Trend";
    }
    return "NA";
  }

  String getAnomalyURL(MergedAnomalyResultDTO anomalyResultDTO) {
    String urlPart = "#view=anomalies&dataset=%s&metrics=%s&currentStart=%s&currentEnd=%s";
    return dashboardUrl + String
        .format(urlPart, anomalyResultDTO.getCollection(), anomalyResultDTO.getMetric(),
            anomalyResultDTO.getStartTime(), anomalyResultDTO.getEndTime());
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("please pass report config directory path\n");
      System.exit(1);
    }
    File configFile = new File(args[0]);
    AnomalyReportConfig config = OBJECT_MAPPER.readValue(configFile, AnomalyReportConfig.class);

    File persistenceFile = new File(config.getThirdEyeConfigDirectoryPath() + "/persistence.yml");
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }

    File detectorConfigFile = new File(config.getThirdEyeConfigDirectoryPath() + "/detector.yml");
    if (!detectorConfigFile.exists()) {
      System.err.println("Missing file:" + detectorConfigFile);
      System.exit(1);
    }

    ConfigurationFactory<ThirdEyeAnomalyConfiguration> factory =
        new ConfigurationFactory<>(ThirdEyeAnomalyConfiguration.class,
            Validation.buildDefaultValidatorFactory().getValidator(), Jackson.newObjectMapper(),
            "");
    ThirdEyeAnomalyConfiguration detectorConfig = factory.build(detectorConfigFile);


    // to start cache service
    detectorConfig.setRootDir(config.getThirdEyeConfigDirectoryPath());
    try {
      ThirdEyeCacheRegistry.initializeCaches(detectorConfig);
    } catch (Exception e) {
      e.printStackTrace();
    }

    GenerateAnomalyReport reportGenerator =
        new GenerateAnomalyReport(df.parse(config.getStartTimeIso()),
            df.parse(config.getEndTimeIso()), persistenceFile,
            Arrays.asList(config.getDatasets().split(",")), config.getTeBaseUrl(),
            SmtpConfiguration.createFromProperties(detectorConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY)),
            config.getEmailRecipients());

    reportGenerator.buildReport();
    //    reportGenerator.listMetric();
    //    reportGenerator.updateEmailConfig();
    return;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AnomalyReportDTO {
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

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("metric", metric).add("startDateTime", startDateTime)
          .add("windowSize", windowSize).add("lift", lift).add("feedback", feedback).add("anomalyId", anomalyId)
          .add("anomalyURL", anomalyURL).toString();
    }
  }
}
