package com.linkedin.thirdeye.alert.content;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.anomaly.alert.util.EmailScreenshotHelper;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This email formatter lists the anomalies by their functions or metric.
 */
public class MultipleAnomaliesEmailContentFormatter extends BaseEmailContentFormatter{
  private static final Logger LOG = LoggerFactory.getLogger(MultipleAnomaliesEmailContentFormatter.class);

  public static final String EMAIL_TEMPLATE = "emailTemplate";
  private static final String DATE_PATTERN = "MMM dd, HH:mm";

  public static final String DEFAULT_EMAIL_TEMPLATE = "holiday-anomaly-report.ftl";
  private static final long EVENT_TIME_TOLERANCE = TimeUnit.DAYS.toMillis(2);

  private EventManager eventDAO = null;


  public MultipleAnomaliesEmailContentFormatter(){

  }

  @Override
  public void init(Properties properties, EmailContentFormatterConfiguration configuration) {
    super.init(properties, configuration);
    this.emailTemplate = properties.getProperty(EMAIL_TEMPLATE, DEFAULT_EMAIL_TEMPLATE);
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
  }

  @Override
  protected void updateTemplateDataByAnomalyResults(Map<String, Object> templateData,
      Collection<AnomalyResult> anomalies, EmailContentFormatterContext context) {
    DateTime windowStart = DateTime.now();
    DateTime windowEnd = new DateTime(0);

    Map<String, Long> functionToId = new HashMap<>();
    Multimap<String, String> anomalyDimensions = ArrayListMultimap.create();
    Multimap<String, AnomalyReportEntity> functionAnomalyReports = ArrayListMultimap.create();
    Multimap<String, AnomalyReportEntity> metricAnomalyReports = ArrayListMultimap.create();
    List<AnomalyReportEntity> anomalyDetails = new ArrayList<>();
    List<String> anomalyIds = new ArrayList<>();

    List<AnomalyResult> sortedAnomalies = new ArrayList<>(anomalies);
    Collections.sort(sortedAnomalies, new Comparator<AnomalyResult>() {
      @Override
      public int compare(AnomalyResult o1, AnomalyResult o2) {
        return Double.compare(o1.getWeight(), o2.getWeight());
      }
    });

    for (AnomalyResult anomalyResult : anomalies) {
      if (!(anomalyResult instanceof MergedAnomalyResultDTO)) {
        LOG.warn("Anomaly result {} isn't an instance of MergedAnomalyResultDTO. Skip from alert.", anomalyResult);
        continue;
      }
      MergedAnomalyResultDTO anomaly = (MergedAnomalyResultDTO) anomalyResult;

      DateTime anomalyStartTime = new DateTime(anomaly.getStartTime(), dateTimeZone);
      DateTime anomalyEndTime = new DateTime(anomaly.getEndTime(), dateTimeZone);

      if (anomalyStartTime.isBefore(windowStart)) {
        windowStart = anomalyStartTime;
      }
      if (anomalyEndTime.isAfter(windowEnd)) {
        windowEnd = anomalyEndTime;
      }

      AnomalyFeedback feedback = anomaly.getFeedback();

      String feedbackVal = getFeedbackValue(feedback);

      AnomalyReportEntity anomalyReport = new AnomalyReportEntity(String.valueOf(anomaly.getId()),
          getAnomalyURL(anomaly, emailContentFormatterConfiguration.getDashboardHost()),
          ThirdEyeUtils.getRoundedValue(anomaly.getAvgBaselineVal()),
          ThirdEyeUtils.getRoundedValue(anomaly.getAvgCurrentVal()),
          0d,
          getDimensionsList(anomaly.getDimensions()),
          getTimeDiffInHours(anomaly.getStartTime(), anomaly.getEndTime()), // duration
          feedbackVal,
          anomaly.getFunction().getFunctionName(),
          anomaly.getMetric(),
          getDateString(anomaly.getStartTime(), dateTimeZone),
          getDateString(anomaly.getEndTime(), dateTimeZone),
          getTimezoneString(dateTimeZone),
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
        anomalyDetails.add(anomalyReport);
        anomalyIds.add(anomalyReport.getAnomalyId());
        functionAnomalyReports.put(functionName, anomalyReport);
        metricAnomalyReports.put(anomaly.getMetric(), anomalyReport);
        functionToId.put(functionName, anomaly.getFunction().getId());
      }
    }

    // holidays
    final DateTime eventStart = windowStart.minus(preEventCrawlOffset);
    final DateTime eventEnd = windowEnd.plus(postEventCrawlOffset);
    List<EventDTO> holidays = eventDAO.findEventsBetweenTimeRange(EventType.HOLIDAY.toString(),
        eventStart.getMillis(), eventEnd.getMillis());
    Collections.sort(holidays, new Comparator<EventDTO>() {
      @Override
      public int compare(EventDTO o1, EventDTO o2) {
        return Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    });

    // Insert anomaly snapshot image
    if (anomalyDetails.size() == 1) {
      AnomalyReportEntity singleAnomaly = anomalyDetails.get(0);
      try {
        imgPath = EmailScreenshotHelper.takeGraphScreenShot(singleAnomaly.getAnomalyId(),
            emailContentFormatterConfiguration);
      } catch (Exception e) {
        LOG.error("Exception while embedding screenshot for anomaly {}", singleAnomaly.getAnomalyId(), e);
      }
    }

    templateData.put("anomalyDetails", anomalyDetails);
    templateData.put("anomalyIds", Joiner.on(",").join(anomalyIds));
    templateData.put("holidays", holidays);
    templateData.put("functionAnomalyDetails", functionAnomalyReports.asMap());
    templateData.put("metricAnomalyDetails", metricAnomalyReports.asMap());
    templateData.put("functionToId", functionToId);
  }
}
