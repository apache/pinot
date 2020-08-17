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

package org.apache.pinot.thirdeye.notification.content;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import joptsimple.internal.Strings;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.anomaly.AnomalyType;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.events.EventFilter;
import org.apache.pinot.thirdeye.anomaly.events.EventType;
import org.apache.pinot.thirdeye.anomaly.events.HolidayEventProvider;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.dashboard.resources.v2.AnomaliesResource;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean.COMPARE_MODE;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detector.email.filter.DummyAlertFilter;
import org.apache.pinot.thirdeye.detector.email.filter.PrecisionRecallEvaluator;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.Weeks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class (helper) defines the overall alert message content. This will
 * be derived to implement various anomaly alerting templates.
 */
public abstract class BaseNotificationContent implements NotificationContent {
  private static final Logger LOG = LoggerFactory.getLogger(BaseNotificationContent.class);

  /*  The Event Crawl Offset takes the standard period format, ex: P1D for 1 day, P1W for 1 week
  Y: years     M: months              W: weeks
  D: days      H: hours (after T)     M: minutes (after T)
  S: seconds along with milliseconds (after T) */
  private static final String EVENT_CRAWL_OFFSET = "eventCrawlOffset";
  private static final String PRE_EVENT_CRAWL_OFFSET = "preEventCrawlOffset";
  private static final String POST_EVENT_CRAWL_OFFSET = "postEventCrawlOffset";

  private static final String INCLUDE_SENT_ANOMALY_ONLY = "includeSentAnomaliesOnly";
  private static final String INCLUDE_SUMMARY = "includeSummary";
  private static final String TIME_ZONE = "timezone";
  private static final String DEFAULT_INCLUDE_SENT_ANOMALY_ONLY = "false";
  private static final String DEFAULT_INCLUDE_SUMMARY = "false";
  private static final String DEFAULT_DATE_PATTERN = "MMM dd, HH:mm";
  private static final String DEFAULT_TIME_ZONE = "America/Los_Angeles";
  private static final String DEFAULT_EVENT_CRAWL_OFFSET = "P2D";

  protected static final String EVENT_FILTER_COUNTRY = "countryCode";

  private static final String RAW_VALUE_FORMAT = "%.0f";
  private static final String PERCENTAGE_FORMAT = "%.2f %%";

  protected boolean includeSentAnomaliesOnly;
  protected DateTimeZone dateTimeZone;
  protected boolean includeSummary;
  protected Period preEventCrawlOffset;
  protected Period postEventCrawlOffset;
  protected String imgPath = null;
  protected MetricConfigManager metricDAO;
  protected ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfig;
  protected Properties properties;

  public void init(Properties properties, ThirdEyeAnomalyConfiguration config) {
    this.properties = properties;
    this.thirdEyeAnomalyConfig = config;

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

    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
  }

  public String getSnaphotPath() {
    return imgPath;
  }

  public void cleanup() {
    if (StringUtils.isNotBlank(imgPath)) {
      try {
        Files.deleteIfExists(new File(imgPath).toPath());
      } catch (IOException e) {
        LOG.error("Exception in deleting screenshot {}", imgPath, e);
      }
    }
  }

  /**
   * Generate subject based on configuration.
   */
  public static String makeSubject(AlertConfigBean.SubjectType subjectType, DetectionAlertConfigDTO notificationConfig, Map<String, Object> templateData) {
    String baseSubject = "Thirdeye Alert : " + notificationConfig.getName();

    switch (subjectType) {
      case ALERT:
        return baseSubject;

      case METRICS:
        return baseSubject + " - " + templateData.get("metrics");

      case DATASETS:
        return baseSubject + " - " + templateData.get("datasets");

      default:
        throw new IllegalArgumentException(String.format("Unknown type '%s'", notificationConfig.getSubjectType()));
    }
  }

  protected void enrichMetricInfo(Map<String, Object> templateData, Collection<AnomalyResult> anomalies) {
    Set<String> metrics = new TreeSet<>();
    Set<String> datasets = new TreeSet<>();

    Map<String, MetricConfigDTO> metricsMap = new TreeMap<>();
    for (AnomalyResult anomalyResult : anomalies) {
      if (anomalyResult instanceof MergedAnomalyResultDTO) {
        MergedAnomalyResultDTO mergedAnomaly = (MergedAnomalyResultDTO) anomalyResult;
        datasets.add(mergedAnomaly.getCollection());
        metrics.add(mergedAnomaly.getMetric());

        MetricConfigDTO metric = this.metricDAO.findByMetricAndDataset(mergedAnomaly.getMetric(), mergedAnomaly.getCollection());
        if (metric != null) {
          metricsMap.put(metric.getId().toString(), metric);
        }
      }
    }

    templateData.put("datasetsCount", datasets.size());
    templateData.put("datasets", StringUtils.join(datasets, ", "));
    templateData.put("metricsCount", metrics.size());
    templateData.put("metrics", StringUtils.join(metrics, ", "));
    templateData.put("metricsMap", metricsMap);
  }

  protected Map<String, Object> getTemplateData(DetectionAlertConfigDTO notificationConfig, Collection<AnomalyResult> anomalies) {
    Map<String, Object> templateData = new HashMap<>();

    DateTimeZone timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(DEFAULT_TIME_ZONE));
    List<MergedAnomalyResultDTO> mergedAnomalyResults = new ArrayList<>();

    // Calculate start and end time of the anomalies
    DateTime startTime = DateTime.now();
    DateTime endTime = new DateTime(0l);
    for (AnomalyResult anomalyResult : anomalies) {
      if (anomalyResult instanceof MergedAnomalyResultDTO) {
        MergedAnomalyResultDTO mergedAnomaly = (MergedAnomalyResultDTO) anomalyResult;
        mergedAnomalyResults.add(mergedAnomaly);
      }
      if (anomalyResult.getStartTime() < startTime.getMillis()) {
        startTime = new DateTime(anomalyResult.getStartTime(), dateTimeZone);
      }
      if (anomalyResult.getEndTime() > endTime.getMillis()) {
        endTime = new DateTime(anomalyResult.getEndTime(), dateTimeZone);
      }
    }

    PrecisionRecallEvaluator precisionRecallEvaluator = new PrecisionRecallEvaluator(new DummyAlertFilter(), mergedAnomalyResults);

    templateData.put("anomalyCount", anomalies.size());
    templateData.put("startTime", getDateString(startTime));
    templateData.put("endTime", getDateString(endTime));
    templateData.put("timeZone", getTimezoneString(dateTimeZone));
    templateData.put("notifiedCount", precisionRecallEvaluator.getTotalAlerts());
    templateData.put("feedbackCount", precisionRecallEvaluator.getTotalResponses());
    templateData.put("trueAlertCount", precisionRecallEvaluator.getTrueAnomalies());
    templateData.put("falseAlertCount", precisionRecallEvaluator.getFalseAlarm());
    templateData.put("newTrendCount", precisionRecallEvaluator.getTrueAnomalyNewTrend());
    templateData.put("alertConfigName", notificationConfig.getName());
    templateData.put("includeSummary", includeSummary);
    templateData.put("reportGenerationTimeMillis", System.currentTimeMillis());
    if(precisionRecallEvaluator.getTotalResponses() > 0) {
      templateData.put("precision", precisionRecallEvaluator.getPrecisionInResponse());
      templateData.put("recall", precisionRecallEvaluator.getRecall());
      templateData.put("falseNegative", precisionRecallEvaluator.getFalseNegativeRate());
    }
    if (notificationConfig.getReferenceLinks() != null) {
      templateData.put("referenceLinks", notificationConfig.getReferenceLinks());
    }

    return templateData;
  }

  protected static String getDateString(DateTime dateTime) {
    return dateTime.toString(DEFAULT_DATE_PATTERN);
  }

  protected static String getDateString(long millis, DateTimeZone dateTimeZone) {
    return (new DateTime(millis, dateTimeZone)).toString(DEFAULT_DATE_PATTERN);
  }

  protected static double getLift(double current, double expected) {
    if (expected == 0) {
      return 1d;
    } else {
      return current/expected - 1;
    }
  }

  /**
   * Get the sign of the severity change
   */
  protected static boolean getLiftDirection(double lift) {
    return lift < 0 ? false : true;
  }

  /**
   * Get the timezone in String
   */
  protected String getTimezoneString(DateTimeZone dateTimeZone) {
    TimeZone tz = TimeZone.getTimeZone(dateTimeZone.getID());
    return tz.getDisplayName(true, 0);
  }

  /**
   * Convert the duration into hours, represented in String
   */
  protected static String getTimeDiffInHours(long start, long end) {
    double duration = Double.valueOf((end - start) / 1000) / 3600;
    String durationString = ThirdEyeUtils.getRoundedValue(duration) + ((duration == 1) ? (" hour") : (" hours"));
    return durationString;
  }

  /**
   * Flatten the dimension map
   */
  protected static List<String> getDimensionsList(Multimap<String, String> dimensions) {
    List<String> dimensionsList = new ArrayList<>();
    if (dimensions != null && !dimensions.isEmpty()) {
      for (Map.Entry<String, Collection<String>> entry : dimensions.asMap().entrySet()) {
        dimensionsList.add(entry.getKey() + " : " + String.join(",", entry.getValue()));
      }
    }
    return dimensionsList;
  }

  /**
   * Get the url of given anomaly result
   */
  protected static String getAnomalyURL(MergedAnomalyResultDTO anomalyResultDTO, String dashboardUrl) {
    String urlPart = "/app/#/rootcause?anomalyId=";
    return dashboardUrl + urlPart;
  }

  /**
   * Retrieve the issue type of an anomaly
   */
  protected static String getIssueType(MergedAnomalyResultDTO anomalyResultDTO) {
    Map<String, String> properties = anomalyResultDTO.getProperties();
    if (MapUtils.isNotEmpty(properties) && properties.containsKey(MergedAnomalyResultDTO.ISSUE_TYPE_KEY)) {
      return properties.get(MergedAnomalyResultDTO.ISSUE_TYPE_KEY);
    }
    return null;
  }

  /**
   * Returns a human readable lift value to be displayed in the notification templates
   */
  protected static String getFormattedLiftValue(MergedAnomalyResultDTO anomaly, double lift) {
    String liftValue = String.format(PERCENTAGE_FORMAT, lift * 100);

    // Fetch the lift value for a SLA anomaly
    if (anomaly.getType().equals(AnomalyType.DATA_SLA)) {
      liftValue = getFormattedSLALiftValue(anomaly);
    }

    return liftValue;
   }

  /**
   * The lift value for an SLA anomaly is delay from the configured sla. (Ex: 2 days & 3 hours)
   */
  protected static String getFormattedSLALiftValue(MergedAnomalyResultDTO anomaly) {
    if (!anomaly.getType().equals(AnomalyType.DATA_SLA)
        || anomaly.getProperties() == null || anomaly.getProperties().isEmpty()
        || !anomaly.getProperties().containsKey("sla")
        || !anomaly.getProperties().containsKey("datasetLastRefreshTime")) {
      return Strings.EMPTY;
    }

    long delayInMillis = anomaly.getEndTime() - Long.parseLong(anomaly.getProperties().get("datasetLastRefreshTime"));
    long days = TimeUnit.MILLISECONDS.toDays(delayInMillis);
    long hours = TimeUnit.MILLISECONDS.toHours(delayInMillis) % TimeUnit.DAYS.toHours(1);
    long minutes = TimeUnit.MILLISECONDS.toMinutes(delayInMillis) % TimeUnit.HOURS.toMinutes(1);

    String liftValue;
    if (days > 0) {
      liftValue = String.format("%d days & %d hours", days, hours);
    } else if (hours > 0) {
      liftValue = String.format("%d hours & %d mins", hours, minutes);
    } else {
      liftValue = String.format("%d mins", minutes);
    }

    return liftValue;
  }

  /**
   * The predicted value for an SLA anomaly is the configured sla. (Ex: 2_DAYS)
   */
  protected static String getSLAPredictedValue(MergedAnomalyResultDTO anomaly) {
    if (!anomaly.getType().equals(AnomalyType.DATA_SLA)
        || anomaly.getProperties() == null || anomaly.getProperties().isEmpty()
        || !anomaly.getProperties().containsKey("sla")) {
      return "-";
    }

    return anomaly.getProperties().get("sla");
  }

  /**
   * Retrieve the predicted value for the anomaly
   */
  protected static String getPredictedValue(MergedAnomalyResultDTO anomaly) {
    String predicted = ThirdEyeUtils.getRoundedValue(anomaly.getAvgBaselineVal());

    // For SLA anomalies, we use the sla as the predicted value
    if (anomaly.getType().equals(AnomalyType.DATA_SLA)) {
      predicted = getSLAPredictedValue(anomaly);
    }

    if (predicted.equalsIgnoreCase(String.valueOf(Double.NaN))) {
      predicted = "-";
    }
    return predicted;
  }

  /**
   * Retrieve the current value for the anomaly
   */
  protected static String getCurrentValue(MergedAnomalyResultDTO anomaly) {
    String current = ThirdEyeUtils.getRoundedValue(anomaly.getAvgCurrentVal());

    if (current.equalsIgnoreCase(String.valueOf(Double.NaN))) {
      current = "-";
    }
    return current;
  }
  /**
   * Convert Feedback value to user readable values
   */
  protected static String getFeedbackValue(AnomalyFeedback feedback) {
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
  protected List<EventDTO> getRelatedEvents(List<EventType> eventTypes, DateTime start, DateTime end
      , String metricName, String serviceName, Map<String, List<String>> targetDimensions) {
    List<EventDTO> relatedEvents = new ArrayList<>();
    for (EventType eventType : eventTypes) {
      relatedEvents.addAll(getHolidayEvents(start, end, targetDimensions));
    }
    return relatedEvents;
  }

  /**
   * Convert comparison mode to Period
   */
  protected static Period getBaselinePeriod(COMPARE_MODE compareMode) {
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
   * @param start the start time of the event, preEventCrawlOffset is added before the given date time
   * @param end the end time of the event, postEventCrawlOffset is added after the given date time
   * @param targetDimensions the affected dimensions
   * @return a list of related events
   */
  protected List<EventDTO> getHolidayEvents(DateTime start, DateTime end, Map<String, List<String>> targetDimensions) {
    EventFilter eventFilter = new EventFilter();
    eventFilter.setEventType(EventType.HOLIDAY.name());
    eventFilter.setStartTime(start.minus(preEventCrawlOffset).getMillis());
    eventFilter.setEndTime(end.plus(postEventCrawlOffset).getMillis());
    eventFilter.setTargetDimensionMap(targetDimensions);

    LOG.info("Fetching holidays with preEventCrawlOffset {} and postEventCrawlOffset {}", preEventCrawlOffset, postEventCrawlOffset);
    return new HolidayEventProvider().getEvents(eventFilter);
  }

  /**
   * Get the value of matched filter key of given anomaly result
   * @param anomaly a MergedAnomalyResultDTO instance
   * @param matchText a text to be matched in the filter keys
   * @return a list of filter values
   */
  protected List<String> getMatchedFilterValues(MergedAnomalyResultDTO anomaly, String matchText) {
    Multimap<String, String> filterSet = AnomaliesResource.generateFilterSetForTimeSeriesQuery(anomaly);
    for (String filterKey : filterSet.keySet()) {
      if (filterKey.contains(matchText)) {
        return new ArrayList<>(filterSet.get(filterKey));
      }
    }
    return Collections.emptyList();
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
    String funcDescription;
    String duration;
    String startTime;
    String endTime;
    String timezone;
    String issueType;
    String score;
    Double weight;
    String groupKey;
    String entityName;
    String anomalyType;
    String properties;
    String metricUrn;

    public AnomalyReportEntity(String anomalyId, String anomalyURL, String baselineVal, String currentVal, String lift,
        boolean positiveLift, Double swi, List<String> dimensions, String duration, String feedback, String function,
        String funcDescription, String metric, String startTime, String endTime, String timezone, String issueType,
        String anomalyType, String properties, String metricUrn) {
      this.anomalyId = anomalyId;
      this.anomalyURL = anomalyURL;
      this.baselineVal = baselineVal;
      this.currentVal = currentVal;
      this.dimensions = dimensions;
      this.duration = duration;
      this.feedback = feedback;
      this.function = function;
      this.funcDescription = funcDescription;
      this.swi = "";
      if (swi != null) {
        this.swi = String.format(PERCENTAGE_FORMAT, swi * 100);
      }
      if (baselineVal.equals("-")) {
        this.lift = Strings.EMPTY;
      } else {
        this.lift = lift;
      }
      this.positiveLift = positiveLift;
      this.metric = metric;
      this.startDateTime = startTime;
      this.endTime = endTime;
      this.timezone = timezone;
      this.issueType = issueType;
      this.anomalyType = anomalyType;
      this.properties = properties;
      this.metricUrn = metricUrn;
    }

    public void setSeasonalValues(COMPARE_MODE compareMode, double seasonalValue, double current) {
      double lift = BaseNotificationContent.getLift(current, seasonalValue);
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

    public String getFuncDescription() {
      return funcDescription;
    }

    public void setFuncDescription(String funcDescription) {
      this.funcDescription = funcDescription;
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

    public String getAnomalyType() {
      return anomalyType;
    }

    public void setAnomalyType(String anomalyType) {
      this.anomalyType = anomalyType;
    }

    public String getProperties() {
      return properties;
    }

    public void setProperties(String properties) {
      this.properties = properties;
    }

    public void setWeight(Double weight) {
      this.weight = weight;
    }

    public double getWeight() {
      return weight;
    }

    public String getScore() {
      return score;
    }

    public void setScore(String score) {
      this.score = score;
    }

    public String getEntityName() {
      return entityName;
    }

    public void setEntityName(String entityName) {
      this.entityName = entityName;
    }

    public String getGroupKey() {
      return groupKey;
    }

    public void setGroupKey(String groupKey) {
      this.groupKey = groupKey;
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

    public String getMetricUrn() {
      return metricUrn;
    }

    public void setMetricUrn(String metricUrn) {
      this.metricUrn = metricUrn;
    }
  }
}
