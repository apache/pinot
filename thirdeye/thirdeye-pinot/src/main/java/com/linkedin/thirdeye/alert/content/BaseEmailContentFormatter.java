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

package com.linkedin.thirdeye.alert.content;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.alert.commons.EmailEntity;
import com.linkedin.thirdeye.anomaly.alert.util.DataReportHelper;
import com.linkedin.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import com.linkedin.thirdeye.anomaly.classification.ClassificationTaskRunner;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.anomaly.events.HolidayEventProvider;
import com.linkedin.thirdeye.anomaly.utils.EmailUtils;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.dashboard.resources.v2.AnomaliesResource;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean.COMPARE_MODE;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.PrecisionRecallEvaluator;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.Weeks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseEmailContentFormatter implements EmailContentFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(BaseEmailContentFormatter.class);

  public static final String INCLUDE_SENT_ANOMALY_ONLY = "includeSentAnomaliesOnly";
  public static final String INCLUDE_SUMMARY = "includeSummary";
  public static final String TIME_ZONE = "timezone";
  /*
  The Event Crawl Offset takes the standard period format, ex: P1D for 1 day, P1W for 1 week
  Y: years
  M: months
  W: weeks
  D: days
  H: hours (after T)
  M: minutes (after T)
  S: seconds along with milliseconds (after T)
   */
  public static final String EVENT_CRAWL_OFFSET = "eventCrawlOffset";
  public static final String PRE_EVENT_CRAWL_OFFSET = "preEventCrawlOffset";
  public static final String POST_EVENT_CRAWL_OFFSET = "postEventCrawlOffset";

  public static final String DEFAULT_INCLUDE_SENT_ANOMALY_ONLY = "false";
  public static final String DEFAULT_INCLUDE_SUMMARY = "false";
  public static final String DEFAULT_DATE_PATTERN = "MMM dd, HH:mm";
  public static final String DEFAULT_TIME_ZONE = "America/Los_Angeles";
  public static final String DEFAULT_EVENT_CRAWL_OFFSET = "P2D";

  public static final String EVENT_FILTER_COUNTRY = "countryCode";

  protected DateTimeZone dateTimeZone;
  protected boolean includeSentAnomaliesOnly;
  protected boolean includeSummary;
  protected String emailTemplate;
  protected Period preEventCrawlOffset;
  protected Period postEventCrawlOffset;
  protected String imgPath = null;
  protected EmailContentFormatterConfiguration emailContentFormatterConfiguration;
  protected MetricConfigManager metricDAO;

  @Override
  public void init(Properties properties, EmailContentFormatterConfiguration configuration) {
    this.includeSentAnomaliesOnly = Boolean.valueOf(
        properties.getProperty(INCLUDE_SENT_ANOMALY_ONLY, DEFAULT_INCLUDE_SENT_ANOMALY_ONLY));
    this.includeSummary = Boolean.valueOf(
        properties.getProperty(INCLUDE_SUMMARY, DEFAULT_INCLUDE_SUMMARY));
    this.dateTimeZone = DateTimeZone.forID(properties.getProperty(TIME_ZONE, DEFAULT_TIME_ZONE));
    Period defaultPeriod = Period.parse(properties.getProperty(EVENT_CRAWL_OFFSET, DEFAULT_EVENT_CRAWL_OFFSET));
    this.preEventCrawlOffset = defaultPeriod;
    this.postEventCrawlOffset = defaultPeriod;
    if (properties.getProperty(PRE_EVENT_CRAWL_OFFSET) != null) {
      this.preEventCrawlOffset = Period.parse(properties.getProperty(PRE_EVENT_CRAWL_OFFSET));
    }
    if (properties.getProperty(POST_EVENT_CRAWL_OFFSET) != null) {
      this.postEventCrawlOffset = Period.parse(properties.getProperty(POST_EVENT_CRAWL_OFFSET));
    }
    this.emailContentFormatterConfiguration = configuration;

    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
  }

  @Override
  public EmailEntity getEmailEntity(AlertConfigDTO alertConfigDTO, DetectionAlertFilterRecipients recipients, String subject,
      Long groupId, String groupName, Collection<AnomalyResult> anomalies, EmailContentFormatterContext context) {
    Map<String, Object> templateData = getTemplateData(alertConfigDTO, groupId, groupName, anomalies);

    updateTemplateDataByAnomalyResults(templateData, anomalies, context);

    String outputSubject = makeSubject(subject, groupName, alertConfigDTO.getSubjectType(), templateData);

    return buildEmailEntity(templateData, outputSubject, recipients, alertConfigDTO.getFromAddress(), emailTemplate);
  }

  /**
   * Generate subject based on configuration.
   * @param baseSubject
   * @param groupName
   * @param type
   * @param templateData
   * @return
   */
  private static String makeSubject(String baseSubject, String groupName, AlertConfigBean.SubjectType type, Map<String, Object> templateData) {
    switch (type) {
      case ALERT:
        if (StringUtils.isNotBlank(groupName)) {
          return baseSubject + " - " + groupName;
        }
        return baseSubject;

      case METRICS:
        return baseSubject + " - " + templateData.get("metrics");

      case DATASETS:
        return baseSubject + " - " + templateData.get("datasets");

      default:
        throw new IllegalArgumentException(String.format("Unknown type '%s'", type));
    }
  }

  /**
   * The actual function that convert anomalies into parameter map
   * @param templateData
   * @param anomalies
   */
  protected abstract void updateTemplateDataByAnomalyResults(Map<String, Object> templateData,
      Collection<AnomalyResult> anomalies, EmailContentFormatterContext context);

  /**
   * Add the auxiliary email information into parameter map
   * @param alertConfigDTO
   * @param groupId
   * @param groupName
   * @param anomalies
   * @return
   */
  protected Map<String, Object> getTemplateData(AlertConfigDTO alertConfigDTO, Long groupId, String groupName,
      Collection<AnomalyResult> anomalies) {
    Map<String, Object> templateData = new HashMap<>();

    DateTimeZone timeZone = DateTimeZone.forTimeZone(AlertTaskRunnerV2.DEFAULT_TIME_ZONE);

    Set<String> metrics = new TreeSet<>();
    Set<String> datasets = new TreeSet<>();
    List<MergedAnomalyResultDTO> mergedAnomalyResults = new ArrayList<>();

    Map<String, MetricConfigDTO> metricsMap = new TreeMap<>();

    // Calculate start and end time of the anomalies
    DateTime startTime = DateTime.now();
    DateTime endTime = new DateTime(0l);
    for (AnomalyResult anomalyResult : anomalies) {
      if (anomalyResult instanceof MergedAnomalyResultDTO) {
        MergedAnomalyResultDTO mergedAnomaly = (MergedAnomalyResultDTO) anomalyResult;
        mergedAnomalyResults.add(mergedAnomaly);
        datasets.add(mergedAnomaly.getCollection());
        metrics.add(mergedAnomaly.getMetric());

        MetricConfigDTO metric = this.metricDAO.findByMetricAndDataset(mergedAnomaly.getMetric(), mergedAnomaly.getCollection());
        if (metric != null) {
          // NOTE: our stale freemarker version doesn't play nice with non-string keys
          metricsMap.put(metric.getId().toString(), metric);
        }
      }
      if (anomalyResult.getStartTime() < startTime.getMillis()) {
        startTime = new DateTime(anomalyResult.getStartTime(), dateTimeZone);
      }
      if (anomalyResult.getEndTime() > endTime.getMillis()) {
        endTime = new DateTime(anomalyResult.getEndTime(), dateTimeZone);
      }
    }

    PrecisionRecallEvaluator precisionRecallEvaluator = new PrecisionRecallEvaluator(new DummyAlertFilter(), mergedAnomalyResults);

    templateData.put("datasetsCount", datasets.size());
    templateData.put("datasets", StringUtils.join(datasets, ", "));
    templateData.put("metricsCount", metrics.size());
    templateData.put("metrics", StringUtils.join(metrics, ", "));
    templateData.put("metricsMap", metricsMap);
    templateData.put("anomalyCount", anomalies.size());
    templateData.put("startTime", getDateString(startTime));
    templateData.put("endTime", getDateString(endTime));
    templateData.put("timeZone", getTimezoneString(dateTimeZone));
    templateData.put("dateFormat", new DataReportHelper.DateFormatMethod(timeZone));
    templateData.put("notifiedCount", precisionRecallEvaluator.getTotalAlerts());
    templateData.put("feedbackCount", precisionRecallEvaluator.getTotalResponses());
    templateData.put("trueAlertCount", precisionRecallEvaluator.getTrueAnomalies());
    templateData.put("falseAlertCount", precisionRecallEvaluator.getFalseAlarm());
    templateData.put("newTrendCount", precisionRecallEvaluator.getTrueAnomalyNewTrend());
    templateData.put("alertConfigName", alertConfigDTO.getName());
    templateData.put("includeSummary", includeSummary);
    templateData.put("reportGenerationTimeMillis", System.currentTimeMillis());
    templateData.put("dashboardHost", emailContentFormatterConfiguration.getDashboardHost());
    if (groupId != null) {
      templateData.put("isGroupedAnomaly", true);
      templateData.put("groupId", Long.toString(groupId));
    } else {
      templateData.put("isGroupedAnomaly", false);
      templateData.put("groupId", Long.toString(-1));
    }
    if (org.apache.commons.lang3.StringUtils.isNotBlank(groupName)) {
      templateData.put("groupName", groupName);
    }
    if(precisionRecallEvaluator.getTotalResponses() > 0) {
      templateData.put("precision", precisionRecallEvaluator.getPrecisionInResponse());
      templateData.put("recall", precisionRecallEvaluator.getRecall());
      templateData.put("falseNegative", precisionRecallEvaluator.getFalseNegativeRate());
    }
    if (alertConfigDTO.getReferenceLinks() != null) {
      templateData.put("referenceLinks", alertConfigDTO.getReferenceLinks());
    }

    return templateData;
  }

  /**
   * Apply the parameter map to given email template, and format it as EmailEntity
   * @param paramMap
   * @param subject
   * @param recipients
   * @param fromEmail
   * @param emailTemplate
   * @return
   */
  public EmailEntity buildEmailEntity(Map<String, Object> paramMap, String subject,
      DetectionAlertFilterRecipients recipients, String fromEmail, String emailTemplate) {
    if (Strings.isNullOrEmpty(fromEmail)) {
      throw new IllegalArgumentException("Invalid sender's email");
    }

    HtmlEmail email = new HtmlEmail();
    String cid = "";
    try {
      if (org.apache.commons.lang3.StringUtils.isNotBlank(imgPath)) {
        cid = email.embed(new File(imgPath));
      }
    } catch (Exception e) {
      LOG.error("Exception while embedding screenshot for anomaly", e);
    }
    paramMap.put("cid", cid);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EmailEntity emailEntity = new EmailEntity();
    try (Writer out = new OutputStreamWriter(baos, AlertTaskRunnerV2.CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector");
      freemarkerConfig.setDefaultEncoding(AlertTaskRunnerV2.CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Template template = freemarkerConfig.getTemplate(emailTemplate);

      template.process(paramMap, out);

      String alertEmailHtml = new String(baos.toByteArray(), AlertTaskRunnerV2.CHARSET);

      emailEntity.setFrom(fromEmail);
      emailEntity.setTo(recipients);
      emailEntity.setSubject(subject);
      email.setHtmlMsg(alertEmailHtml);
      emailEntity.setContent(email);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    return emailEntity;
  }

  /**
   * Get the Date String
   * @param dateTime
   * @return
   */
  public static String getDateString(DateTime dateTime) {
    return dateTime.toString(DEFAULT_DATE_PATTERN);
  }
  public static String getDateString(long millis, DateTimeZone dateTimeZone) {
    return (new DateTime(millis, dateTimeZone)).toString(DEFAULT_DATE_PATTERN);
  }

  /**
   * Get the timezone in String
   * @param dateTimeZone
   * @return
   */
  public String getTimezoneString(DateTimeZone dateTimeZone) {
    TimeZone tz = TimeZone.getTimeZone(dateTimeZone.getID());
    return tz.getDisplayName(true, 0);
  }

  /**
   * Convert the duration into hours, represented in String
   * @param start
   * @param end
   * @return
   */
  public static String getTimeDiffInHours(long start, long end) {
    double duration = Double.valueOf((end - start) / 1000) / 3600;
    String durationString = ThirdEyeUtils.getRoundedValue(duration) + ((duration == 1) ? (" hour") : (" hours"));
    return durationString;
  }

  /**
   * Flatten the dimension map
   * @param dimensionMap
   * @return
   */
  public static List<String> getDimensionsList(DimensionMap dimensionMap) {
    List<String> dimensionsList = new ArrayList<>();
    if (dimensionMap != null && !dimensionMap.isEmpty()) {
      for (Map.Entry<String, String> entry : dimensionMap.entrySet()) {
        dimensionsList.add(entry.getKey() + " : " + entry.getValue());
      }
    }
    return dimensionsList;
  }

  /**
   * Get the sign of the severity change
   * @param lift
   * @return
   */
  public static boolean getLiftDirection(double lift) {
    return lift < 0 ? false : true;
  }

  /**
   * Get the url of given anomaly result
   * @param anomalyResultDTO
   * @param dashboardUrl
   * @return
   */
  public static String getAnomalyURL(MergedAnomalyResultDTO anomalyResultDTO, String dashboardUrl) {
    String urlPart = "/app/#/rootcause?anomalyId=";
    return dashboardUrl + urlPart;
  }

  /**
   * Retrieve the issue type of an anomaly
   * @param anomalyResultDTO
   * @return
   */
  public static String getIssueType(MergedAnomalyResultDTO anomalyResultDTO) {
    Map<String, String> properties = anomalyResultDTO.getProperties();
    if (MapUtils.isNotEmpty(properties) && properties.containsKey(ClassificationTaskRunner.ISSUE_TYPE_KEY)) {
      return properties.get(ClassificationTaskRunner.ISSUE_TYPE_KEY);
    }
    return null;
  }

  /**
   * Convert Feedback value to user readable values
   * @param feedback
   * @return
   */
  public static String getFeedbackValue(AnomalyFeedback feedback) {
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

  /**
   * Taking advantage of event data provider, extract the events around the given start and end time
   * @param eventTypes the list of event types
   * @param start the start time of the event, preEventCrawlOffset is added before the given date time
   * @param end the end time of the event, postEventCrawlOffset is added after the given date time
   * @param metricName the affected metric name
   * @param serviceName the affected service name
   * @param targetDimensions the affected dimensions
   * @return a list of related events
   */
  public List<EventDTO> getRelatedEvents(List<EventType> eventTypes, DateTime start, DateTime end
      , String metricName, String serviceName, Map<String, List<String>> targetDimensions) {
    List<EventDTO> relatedEvents = new ArrayList<>();
    for (EventType eventType : eventTypes) {
      relatedEvents.addAll(getRelatedEvents(eventType, start, end, metricName, serviceName, targetDimensions));
    }
    return relatedEvents;
  }

  /**
   * Retrieve the average wow baseline values
   * @param anomaly an instance of MergedAnomalyResultDTO
   * @param compareMode the way to compare, WoW, Wo2W, Wo3W, and Wo4W
   * @param start the start time of the monitoring window in millis
   * @param end the end time of the monitoring window in millis
   * @return baseline values based on compareMode
   * @throws Exception
   */
  protected Double getAvgComparisonBaseline(MergedAnomalyResultDTO anomaly, COMPARE_MODE compareMode,
      long start, long end) throws Exception{
    AnomalyFunctionFactory anomalyFunctionFactory = new AnomalyFunctionFactory(emailContentFormatterConfiguration.getFunctionConfigPath());
    AnomalyFunctionDTO anomalyFunction = anomaly.getFunction();
    DatasetConfigDTO datasetConfigDTO = DAORegistry.getInstance().getDatasetConfigDAO()
        .findByDataset(anomalyFunction.getCollection());
    AnomalyDetectionInputContextBuilder contextBuilder = new AnomalyDetectionInputContextBuilder(anomalyFunctionFactory);
    contextBuilder.setFunction(anomalyFunction);

    DateTimeZone timeZone = DateTimeZone.forID(datasetConfigDTO.getTimezone());
    DateTime startTime = new DateTime(start, timeZone);
    DateTime endTime = new DateTime(end, timeZone);

    Period baselinePeriod = getBaselinePeriod(compareMode);
    DateTime baselineStartTime = startTime.minus(baselinePeriod);
    DateTime baselineEndTime = endTime.minus(baselinePeriod);

    Pair<Long, Long> timeRange = new Pair<>(baselineStartTime.getMillis(), baselineEndTime.getMillis());
    MetricTimeSeries baselineTimeSeries = contextBuilder.fetchTimeSeriesDataByDimension(Arrays.asList(timeRange), anomaly.getDimensions(), false)
        .build().getDimensionMapMetricTimeSeriesMap().get(anomaly.getDimensions());

    return baselineTimeSeries.getMetricAvgs(0d)[0];
  }

  /**
   * Convert comparison mode to Period
   * @param compareMode
   * @return
   */
  public static Period getBaselinePeriod(COMPARE_MODE compareMode) {
    switch (compareMode) {
      case Wo2W:
        return Weeks.TWO.toPeriod();
      case Wo3W:
        return Weeks.THREE.toPeriod();
      case Wo4W:
        return Weeks.weeks(4).toPeriod();
      case WoW:
      default:
        return Weeks.ONE.toPeriod();
    }
  }

  /**
   * Taking advantage of event data provider, extract the events around the given start and end time
   * @param eventType a given event type
   * @param start the start time of the event, preEventCrawlOffset is added before the given date time
   * @param end the end time of the event, postEventCrawlOffset is added after the given date time
   * @param metricName the affected metric name
   * @param serviceName the affected service name
   * @param targetDimensions the affected dimensions
   * @return a list of related events
   */
  public List<EventDTO> getRelatedEvents(EventType eventType, DateTime start, DateTime end
      , String metricName, String serviceName, Map<String, List<String>> targetDimensions) {
    List<EventDTO> relatedEvents = new ArrayList<>();

    // Set Event Filters
    EventFilter eventFilter = new EventFilter();
    eventFilter.setStartTime(start.minus(preEventCrawlOffset).getMillis());
    eventFilter.setEndTime(end.plus(postEventCrawlOffset).getMillis());
    eventFilter.setMetricName(metricName);
    eventFilter.setServiceName(serviceName);
    eventFilter.setTargetDimensionMap(targetDimensions);
    eventFilter.setEventType(eventType.toString());

    return new HolidayEventProvider().getEvents(eventFilter);
  }

  /**
   * Get the value of matched filter key of given anomaly result
   * @param anomaly a MergedAnomalyResultDTO instance
   * @param matchText a text to be matched in the filter keys
   * @return a list of filter values
   */
  public List<String> getMatchedFilterValues(MergedAnomalyResultDTO anomaly, String matchText) {
    Multimap<String, String> filterSet = AnomaliesResource.generateFilterSetForTimeSeriesQuery(anomaly);
    for (String filterKey : filterSet.keySet()) {
      if (filterKey.contains(matchText)) {
        return new ArrayList<>(filterSet.get(filterKey));
      }
    }
    return Collections.emptyList();
  }


  public static double getLift(double current, double expected) {
    if (expected == 0) {
      return 1d;
    } else {
      return current/expected - 1;
    }
  }

  @Override
  public void cleanup() {
    if (org.apache.commons.lang3.StringUtils.isNotBlank(imgPath)) {
      try {
        Files.deleteIfExists(new File(imgPath).toPath());
      } catch (IOException e) {
        LOG.error("Exception in deleting screenshot {}", imgPath, e);
      }
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AnomalyReportEntity {
    String metric;
    String startDateTime;
    String lift; // percentage change
    boolean positiveLift;
    boolean positiveWoWLift;
    boolean positiveWo2WLift;
    boolean positiveWo3WLift;
    boolean positiveWo4WLift;
    String wowValue;
    String wowLift;
    String wo2wValue;
    String wo2wLift;
    String wo3wValue;
    String wo3wLift;
    String wo4wValue;
    String wo4wLift;
    String feedback;
    String anomalyId;
    String anomalyURL;
    String currentVal; // avg current val
    String baselineVal; // avg baseline val
    List<String> dimensions;
    String swi;
    String function;
    String duration;
    String startTime;
    String endTime;
    String timezone;
    String issueType;

    private static String RAW_VALUE_FORMAT = "%.0f";
    private static String PERCENTAGE_FORMAT = "%.2f %%";

    public AnomalyReportEntity(String anomalyId, String anomalyURL, String baselineVal, String currentVal, Double swi,
        List<String> dimensions, String duration, String feedback, String function,
        String metric, String startTime, String endTime, String timezone, String issueType) {
      this.anomalyId = anomalyId;
      this.anomalyURL = anomalyURL;
      this.baselineVal = baselineVal;
      this.currentVal = currentVal;
      this.dimensions = dimensions;
      this.duration = duration;
      this.feedback = feedback;
      this.function = function;
      this.swi = "";
      if (swi != null) {
        this.swi = String.format(PERCENTAGE_FORMAT, swi * 100);
      }
      double lift = BaseEmailContentFormatter.getLift(Double.valueOf(currentVal), Double.valueOf(baselineVal));
      this.lift = String.format(PERCENTAGE_FORMAT, lift * 100);
      this.positiveLift = getLiftDirection(lift);
      this.metric = metric;
      this.startDateTime = startTime;
      this.endTime = endTime;
      this.timezone = timezone;
      this.issueType = issueType;
    }

    public void setSeasonalValues(COMPARE_MODE compareMode, double seasonalValue, double current) {
      double lift = BaseEmailContentFormatter.getLift(current, seasonalValue);
      switch (compareMode) {
        case Wo4W:
          this.wo4wValue = String.format(RAW_VALUE_FORMAT, seasonalValue);
          this.wo4wLift = String.format(PERCENTAGE_FORMAT, lift);
          this.positiveWo4WLift = getLiftDirection(lift);
          break;
        case Wo3W:
          this.wo3wValue = String.format(RAW_VALUE_FORMAT, seasonalValue);
          this.wo3wLift = String.format(PERCENTAGE_FORMAT, lift * 100);
          this.positiveWo3WLift = getLiftDirection(lift);
          break;
        case Wo2W:
          this.wo2wValue = String.format(RAW_VALUE_FORMAT, seasonalValue);
          this.wo2wLift = String.format(PERCENTAGE_FORMAT, lift * 100);
          this.positiveWo2WLift = getLiftDirection(lift);
          break;
        case WoW:
          this.wowValue = String.format(RAW_VALUE_FORMAT, seasonalValue);
          this.wowLift = String.format(PERCENTAGE_FORMAT, lift * 100);
          this.positiveWoWLift = getLiftDirection(lift);
          break;
        default:
      }
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

    public String getSwi() {
      return swi;
    }

    public void setSwi(String swi) {
      this.swi = swi;
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

    public boolean isPositiveWoWLift() {
      return positiveWoWLift;
    }

    public void setPositiveWoWLift(boolean positiveWoWLift) {
      this.positiveWoWLift = positiveWoWLift;
    }

    public boolean isPositiveWo2WLift() {
      return positiveWo2WLift;
    }

    public void setPositiveWo2WLift(boolean positiveWo2WLift) {
      this.positiveWo2WLift = positiveWo2WLift;
    }

    public boolean isPositiveWo3WLift() {
      return positiveWo3WLift;
    }

    public void setPositiveWo3WLift(boolean positiveWo3WLift) {
      this.positiveWo3WLift = positiveWo3WLift;
    }

    public boolean isPositiveWo4WLift() {
      return positiveWo4WLift;
    }

    public void setPositiveWo4WLift(boolean positiveWo4WLift) {
      this.positiveWo4WLift = positiveWo4WLift;
    }

    public String getWowValue() {
      return wowValue;
    }

    public void setWowValue(String wowValue) {
      this.wowValue = wowValue;
    }

    public String getWowLift() {
      return wowLift;
    }

    public void setWowLift(String wowLift) {
      this.wowLift = wowLift;
    }

    public String getWo2wValue() {
      return wo2wValue;
    }

    public void setWo2wValue(String wo2wValue) {
      this.wo2wValue = wo2wValue;
    }

    public String getWo2wLift() {
      return wo2wLift;
    }

    public void setWo2wLift(String wo2wLift) {
      this.wo2wLift = wo2wLift;
    }

    public String getWo3wValue() {
      return wo3wValue;
    }

    public void setWo3wValue(String wo3wValue) {
      this.wo3wValue = wo3wValue;
    }

    public String getWo3wLift() {
      return wo3wLift;
    }

    public void setWo3wLift(String wo3wLift) {
      this.wo3wLift = wo3wLift;
    }

    public String getWo4wValue() {
      return wo4wValue;
    }

    public void setWo4wValue(String wo4wValue) {
      this.wo4wValue = wo4wValue;
    }

    public String getWo4wLift() {
      return wo4wLift;
    }

    public void setWo4wLift(String wo4wLift) {
      this.wo4wLift = wo4wLift;
    }
  }
}
