package com.linkedin.thirdeye.tools.anomaly.report;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskRunner;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.EmailConfigurationManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
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
import org.apache.commons.mail.HtmlEmail;

public class GenerateAnomalyReport {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

  MergedAnomalyResultManager anomalyResultManager;
  MetricConfigManager metricConfigManager;
  EmailConfigurationManager emailConfigurationManager;

  Date startTime;
  Date endTime;
  List<String> collections;
  String dashboardUrl;
  SmtpConfiguration smtpConfiguration;
  String emailRecipients;

  public GenerateAnomalyReport(Date startTime, Date endTime, File persistenceConfig,
      List<String> datasets, String dashboardUrl, SmtpConfiguration smtpConfiguration,
      String emailRecipients) {
    DaoProviderUtil.init(persistenceConfig);
    anomalyResultManager = DaoProviderUtil.getInstance(MergedAnomalyResultManagerImpl.class);
    metricConfigManager = DaoProviderUtil.getInstance(MetricConfigManagerImpl.class);
    emailConfigurationManager = DaoProviderUtil.getInstance(EmailConfigurationManagerImpl.class);

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

  void updateEmailConfig() {
    String recipients = "xyz@linkedin.com";
    for (String collection : collections) {
      List<EmailConfigurationDTO> emailConfigs =
          emailConfigurationManager.findByCollection(collection);
      for (EmailConfigurationDTO emailConfigurationDTO : emailConfigs) {
        if (emailConfigurationDTO.getFunctions().size() > 0) {
          emailConfigurationDTO.setToAddresses(recipients);
          emailConfigurationManager.update(emailConfigurationDTO);
          System.out.println("Email updated " + emailConfigurationDTO.getMetric());
        }
      }
    }
  }

  void buildReport() {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    for (String collection : collections) {
      anomalies.addAll(anomalyResultManager
          .findByCollectionTime(collection, startTime.getTime(), endTime.getTime(), false));
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
      int nonActionable = 0;

      List<AnomalyReportDTO> anomalyReportDTOList = new ArrayList<>();

      for (MergedAnomalyResultDTO anomaly : anomalies) {
        metrics.add(anomaly.getMetric());
        if (anomaly.getFeedback() != null) {
          feedbackCollected++;
          if (anomaly.getFeedback().getFeedbackType().equals(AnomalyFeedbackType.ANOMALY)) {
            trueAlert++;
          } else if (anomaly.getFeedback().getFeedbackType().equals(AnomalyFeedbackType.NOT_ANOMALY)) {
            falseAlert++;
          } else {
            nonActionable++;
          }
        }
        String feedbackVal = getFeedback(
            anomaly.getFeedback() == null ? "NA" : anomaly.getFeedback().getFeedbackType().name());

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
      templateData.put("nonActionableCount", nonActionable);
      templateData.put("datasets", String.join(", ", collections));
      templateData.put("anomalyDetails", anomalyReportDTOList);
      buildEmailTemplateAndSendAlert(templateData);
    }
  }

  void buildEmailTemplateAndSendAlert(Map<String, Object> paramMap) {
    HtmlEmail email = new HtmlEmail();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (Writer out = new OutputStreamWriter(baos, AlertTaskRunner.CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector");
      freemarkerConfig.setDefaultEncoding(AlertTaskRunner.CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Template template = freemarkerConfig.getTemplate("custom-anomaly-report.ftl");
      template.process(paramMap, out);

      String alertEmailSubject =
          "Thirdeye : Daily anomaly report";
      String alertEmailHtml = new String(baos.toByteArray(), AlertTaskRunner.CHARSET);
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
    case "ANOMALY_NO_ACTION":
      return "Not Actionable";
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

    GenerateAnomalyReport reportGenerator =
        new GenerateAnomalyReport(df.parse(config.getStartTimeIso()),
            df.parse(config.getEndTimeIso()), persistenceFile,
            Arrays.asList(config.getDatasets().split(",")), config.getTeBaseUrl(),
            detectorConfig.getSmtpConfiguration(), config.getEmailRecipients());

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
      return "AnomalyReportDTO{" +
          "anomalyId=" + anomalyId +
          ", metric='" + metric + '\'' +
          ", startDateTime='" + startDateTime + '\'' +
          ", windowSize='" + windowSize + '\'' +
          ", lift='" + lift + '\'' +
          ", feedback='" + feedback + '\'' +
          ", anomalyURL='" + anomalyURL + '\'' +
          '}';
    }
  }
}
