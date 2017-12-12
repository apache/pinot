package com.linkedin.thirdeye.anomaly.alert.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import com.linkedin.thirdeye.anomaly.classification.ClassificationTaskRunner;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.PrecisionRecallEvaluator;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnomalyReportGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(AnomalyReportGenerator.class);

  private static final AnomalyReportGenerator INSTANCE = new AnomalyReportGenerator();
  private static final String DATE_PATTERN = "MMM dd, HH:mm";
  private static final String MULTIPLE_ANOMALIES_EMAIL_TEMPLATE = "holiday-anomaly-report.ftl";

  private static final long EVENT_TIME_TOLERANCE = TimeUnit.DAYS.toMillis(2);

  private static final EventManager eventDAO = DAORegistry.getInstance().getEventDAO();

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

  public List<MergedAnomalyResultDTO> getAnomaliesForFunctions(List<Long> functions, long startTime,
      long endTime) {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    LOG.info("fetching anomalies for functions : " + functions);
    for (long function : functions) {
      List<MergedAnomalyResultDTO> results = anomalyResultManager.
          findByStartTimeInRangeAndFunctionId(startTime, endTime, function, false);
      LOG.info("Found {} result for function {}", results.size(), function);
      anomalies.addAll(results);
    }
    return anomalies;
  }

  /**
   * Build report/alert for the given list of anomalies, which could belong to a grouped anomaly if groupId is not null.
   * @param groupId the group id of the list of anomalies; null means they does not belong to any grouped anomaly.
   * @param groupName the group name, i.e., dimension information, of this report.
   * @param anomalies the list of anomalies to be put in the report/alert.
   * @param configuration the configuration that contains the information of ThirdEye host.
   * @param recipients the recipients of this (group) list of anomalies.
   * @param alertConfig the alert config
   * @param emailSubjectName the name of this alert configuration.
   */
  public void buildReport(Long groupId, String groupName, List<MergedAnomalyResultDTO> anomalies,
      ThirdEyeAnomalyConfiguration configuration, String recipients, String emailSubjectName, AlertConfigDTO alertConfig) {
    String subject = "Thirdeye Alert : " + emailSubjectName;
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
    buildReport(startTime, endTime, groupId, groupName, anomalies, subject, configuration, false,
        recipients, emailSubjectName, alertConfig, false);
  }

  public void buildReport(long startTime, long endTime, Long groupId, String groupName,
      List<MergedAnomalyResultDTO> anomalies, String subject, ThirdEyeAnomalyConfiguration configuration,
      boolean includeSentAnomaliesOnly, String emailRecipients, String emailSubjectName, AlertConfigDTO alertConfig,
      boolean includeSummary) {
    if (anomalies == null || anomalies.size() == 0) {
      LOG.info("No anomalies found to send email, please check the parameters.. exiting");
    } else {
      DateTimeZone timeZone = DateTimeZone.forTimeZone(AlertTaskRunnerV2.DEFAULT_TIME_ZONE);
      Set<String> metrics = new HashSet<>();
      Map<String, Long> functionToId = new HashMap<>();

      Multimap<String, String> anomalyDimensions = ArrayListMultimap.create();
      Multimap<String, AnomalyReportDTO> functionAnomalyReports = ArrayListMultimap.create();
      Multimap<String, AnomalyReportDTO> metricAnomalyReports = ArrayListMultimap.create();
      List<AnomalyReportDTO> anomalyReports = new ArrayList<>();
      List<String> anomalyIds = new ArrayList<>();
      Set<String> datasets = new HashSet<>();
      for (MergedAnomalyResultDTO anomaly : anomalies) {
        metrics.add(anomaly.getMetric());
        datasets.add(anomaly.getCollection());

        AnomalyFeedback feedback = anomaly.getFeedback();

        String feedbackVal = getFeedbackValue(feedback);

        double lift = anomaly.getWeight();
        if (anomaly.getAvgBaselineVal() > 0.0) {
          lift = (anomaly.getAvgCurrentVal() - anomaly.getAvgBaselineVal()) / anomaly.getAvgBaselineVal();
        }
        AnomalyReportDTO anomalyReportDTO = new AnomalyReportDTO(String.valueOf(anomaly.getId()),
            getAnomalyURL(anomaly, configuration.getDashboardHost()),
            ThirdEyeUtils.getRoundedValue(anomaly.getAvgBaselineVal()),
            ThirdEyeUtils.getRoundedValue(anomaly.getAvgCurrentVal()),
            getDimensionsList(anomaly.getDimensions()),
            getTimeDiffInHours(anomaly.getStartTime(), anomaly.getEndTime()), // duration
            feedbackVal,
            anomaly.getFunction().getFunctionName(),
            ThirdEyeUtils.getRoundedValue(lift * 100) + "%",
            getLiftDirection(anomaly.getWeight()),
            anomaly.getMetric(),
            getDateString(anomaly.getStartTime(), timeZone),
            getDateString(anomaly.getEndTime(), timeZone),
            getTimezoneString(timeZone),
            getIssueType(anomaly)
        );

        // function name
        String functionName = "Alerts";
        if (anomaly.getFunction() != null) {
          functionName = anomaly.getFunction().getFunctionName();
        }

        // dimension filters / values
        for (Map.Entry<String, String> entry : anomaly.getDimensions().entrySet()) {
          anomalyDimensions.put(entry.getKey(), entry.getValue());
        }

        // include notified alerts only in the email
        if (!includeSentAnomaliesOnly || anomaly.isNotified()) {
          anomalyReports.add(anomalyReportDTO);
          anomalyIds.add(anomalyReportDTO.getAnomalyId());
          functionAnomalyReports.put(functionName, anomalyReportDTO);
          metricAnomalyReports.put(anomaly.getMetric(), anomalyReportDTO);
          functionToId.put(functionName, anomaly.getFunction().getId());
        }
      }

      // holidays
      final long eventStart = startTime - EVENT_TIME_TOLERANCE;
      final long eventEnd = endTime + EVENT_TIME_TOLERANCE;
      List<EventDTO> holidays = eventDAO.findEventsBetweenTimeRange(EventType.HOLIDAY.toString(), eventStart, eventEnd);

      Collections.sort(holidays, new Comparator<EventDTO>() {
        @Override
        public int compare(EventDTO o1, EventDTO o2) {
          return Long.compare(o1.getStartTime(), o2.getStartTime());
        }
      });

      // TODO filter holidays by country dimension? this could lead to false negatives

      // template data
      PrecisionRecallEvaluator precisionRecallEvaluator = new PrecisionRecallEvaluator(anomalies);

      HtmlEmail email = new HtmlEmail();

      DataReportHelper.DateFormatMethod dateFormatMethod = new DataReportHelper.DateFormatMethod(timeZone);
      Map<String, Object> templateData = new HashMap<>();
      templateData.put("datasets", Joiner.on(", ").join(datasets));
      templateData.put("timeZone", getTimezoneString(timeZone));
      templateData.put("dateFormat", dateFormatMethod);
      templateData.put("startTime", getDateString(startTime, timeZone));
      templateData.put("endTime", getDateString(endTime, timeZone));
      templateData.put("anomalyCount", anomalies.size());
      templateData.put("metricsCount", metrics.size());
      templateData.put("notifiedCount", precisionRecallEvaluator.getTotalAlerts());
      templateData.put("feedbackCount", precisionRecallEvaluator.getTotalResponses());
      templateData.put("trueAnomalyCount", precisionRecallEvaluator.getTrueAnomalies());
      templateData.put("falseAlertCount", precisionRecallEvaluator.getFalseAlarm());
      templateData.put("newTrendCount", precisionRecallEvaluator.getTrueAnomalyNewTrend());
      templateData.put("anomalyDetails", anomalyReports);
      templateData.put("functionAnomalyDetails", functionAnomalyReports.asMap());
      templateData.put("metricAnomalyDetails", metricAnomalyReports.asMap());
      templateData.put("emailSubjectName", emailSubjectName);
      templateData.put("alertConfigName", alertConfig.getName()); // NOTE: breaks backwards compatibility
      templateData.put("includeSummary", includeSummary);
      templateData.put("reportGenerationTimeMillis", System.currentTimeMillis());
      templateData.put("dashboardHost", configuration.getDashboardHost());
      templateData.put("anomalyIds", Joiner.on(",").join(anomalyIds));
      templateData.put("holidays", holidays);
      templateData.put("functionToId", functionToId);
      if (groupId != null) {
        templateData.put("isGroupedAnomaly", true);
        templateData.put("groupId", Long.toString(groupId));
      } else {
        templateData.put("isGroupedAnomaly", false);
        templateData.put("groupId", Long.toString(-1));
      }
      if (StringUtils.isNotBlank(groupName)) {
        templateData.put("groupName", groupName);
        subject = subject + " - " + groupName;
      }
      if(precisionRecallEvaluator.getTotalResponses() > 0) {
        templateData.put("precision", precisionRecallEvaluator.getPrecisionInResponse());
        templateData.put("recall", precisionRecallEvaluator.getRecall());
        templateData.put("falseNegative", precisionRecallEvaluator.getFalseNegativeRate());
      }

      if (CollectionUtils.isNotEmpty(anomalyReports)) {
        Set<String> metricNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Iterator<AnomalyReportDTO> iterator = anomalyReports.iterator();
        while (iterator.hasNext() && metricNames.size() < 2) {
          AnomalyReportDTO anomalyReportDTO = iterator.next();
          metricNames.add(anomalyReportDTO.getMetric());
        }
        if (metricNames.size() == 1) {
          AnomalyReportDTO singleAnomaly = anomalyReports.get(0);
          subject = subject + " - " + singleAnomaly.getMetric();
        }
      }

      String imgPath = null;
      String cid = "";
      if (anomalyReports.size() == 1) {
        AnomalyReportDTO singleAnomaly = anomalyReports.get(0);
        try {
          imgPath = EmailScreenshotHelper.takeGraphScreenShot(singleAnomaly.getAnomalyId(), configuration);
          if (StringUtils.isNotBlank(imgPath)) {
            cid = email.embed(new File(imgPath));
          }
        } catch (Exception e) {
          LOG.error("Exception while embedding screenshot for anomaly {}", singleAnomaly.getAnomalyId(), e);
        }
      }
      templateData.put("cid", cid);

      buildEmailTemplateAndSendAlert(templateData, configuration.getSmtpConfiguration(), subject,
          emailRecipients, alertConfig.getFromAddress(), email);

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
    try (Writer out = new OutputStreamWriter(baos, AlertTaskRunnerV2.CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector");
      freemarkerConfig.setDefaultEncoding(AlertTaskRunnerV2.CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Template template = freemarkerConfig.getTemplate(MULTIPLE_ANOMALIES_EMAIL_TEMPLATE);

      template.process(paramMap, out);

      String alertEmailHtml = new String(baos.toByteArray(), AlertTaskRunnerV2.CHARSET);
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

  private String getTimeDiffInHours(long start, long end) {
    double duration = Double.valueOf((end - start) / 1000) / 3600;
    String durationString = ThirdEyeUtils.getRoundedValue(duration) + ((duration == 1) ? (" hour") : (" hours"));
    return durationString;
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

  private String getFeedbackValue(AnomalyFeedback feedback) {
    String feedbackVal = "Not Resolved";
    if (feedback != null && feedback.getFeedbackType() != null) {
      switch (feedback.getFeedbackType()) {
        case ANOMALY:
          feedbackVal = "Resolved (Confirmed Anomaly)";
          break;
        case NOT_ANOMALY:
          feedbackVal = "Resolved (False Alarm)";
          break;
        case ANOMALY_NEW_TREND:
          feedbackVal = "Resolved (New Trend)";
          break;
      case NO_FEEDBACK:
      default:
        break;
      }
    }
    return feedbackVal;
  }

  private String getAnomalyURL(MergedAnomalyResultDTO anomalyResultDTO, String dashboardUrl) {
    String urlPart = "/thirdeye#investigate?anomalyId=";
    return dashboardUrl + urlPart;
  }

  private String getIssueType(MergedAnomalyResultDTO anomalyResultDTO) {
    Map<String, String> properties = anomalyResultDTO.getProperties();
    if (MapUtils.isNotEmpty(properties) && properties.containsKey(ClassificationTaskRunner.ISSUE_TYPE_KEY)) {
      return properties.get(ClassificationTaskRunner.ISSUE_TYPE_KEY);
    }
    return null;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AnomalyReportDTO {
    String metric;
    String startDateTime;
    String lift; // percentage change
    boolean positiveLift;
    String feedback;
    String anomalyId;
    String anomalyURL;
    String currentVal; // avg current val
    String baselineVal; // avg baseline val
    List<String> dimensions;
    String function;
    String duration;
    String startTime;
    String endTime;
    String timezone;
    String issueType;

    public AnomalyReportDTO(String anomalyId, String anomalyURL, String baselineVal, String currentVal,
        List<String> dimensions, String duration, String feedback, String function, String lift, boolean positiveLift,
        String metric, String startTime, String endTime, String timezone, String issueType) {
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
      this.issueType = issueType;
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

    public String getIssueType() {
      return issueType;
    }

    public void setIssueType(String issueType) {
      this.issueType = issueType;
    }
  }

}

