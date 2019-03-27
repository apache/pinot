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

package org.apache.pinot.thirdeye.alert.content;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.anomaly.alert.util.EmailScreenshotHelper;
import org.apache.pinot.thirdeye.anomaly.events.EventType;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
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

  public static final String DEFAULT_EMAIL_TEMPLATE = "holiday-anomaly-report.ftl";

  private DetectionConfigManager configDAO = null;

  public MultipleAnomaliesEmailContentFormatter(){

  }

  @Override
  public void init(Properties properties, EmailContentFormatterConfiguration configuration) {
    super.init(properties, configuration);
    this.emailTemplate = properties.getProperty(EMAIL_TEMPLATE, DEFAULT_EMAIL_TEMPLATE);
    this.configDAO = DAORegistry.getInstance().getDetectionConfigManager();
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

      String functionName = "Alerts";
      String funcDescription = "";
      Long id = -1L;

      if (anomaly.getFunction() != null){
        functionName = anomaly.getFunction().getFunctionName();
        id = anomaly.getFunction().getId();
      } else if ( anomaly.getDetectionConfigId() != null){
        DetectionConfigDTO config = this.configDAO.findById(anomaly.getDetectionConfigId());
        Preconditions.checkNotNull(config, String.format("Cannot find detection config %d", anomaly.getDetectionConfigId()));
        functionName = config.getName();
        funcDescription = config.getDescription();
        id = config.getId();
      }

      AnomalyReportEntity anomalyReport = new AnomalyReportEntity(String.valueOf(anomaly.getId()),
          getAnomalyURL(anomaly, emailContentFormatterConfiguration.getDashboardHost()),
          ThirdEyeUtils.getRoundedValue(anomaly.getAvgBaselineVal()),
          ThirdEyeUtils.getRoundedValue(anomaly.getAvgCurrentVal()),
          0d,
          getDimensionsList(anomaly.getDimensions()),
          getTimeDiffInHours(anomaly.getStartTime(), anomaly.getEndTime()), // duration
          feedbackVal,
          functionName,
          funcDescription,
          anomaly.getMetric(),
          getDateString(anomaly.getStartTime(), dateTimeZone),
          getDateString(anomaly.getEndTime(), dateTimeZone),
          getTimezoneString(dateTimeZone),
          getIssueType(anomaly)
      );

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
        functionToId.put(functionName, id);
      }
    }

    // holidays
    final DateTime eventStart = windowStart.minus(preEventCrawlOffset);
    final DateTime eventEnd = windowEnd.plus(postEventCrawlOffset);
    Map<String, List<String>> targetDimensions = new HashMap<>();
    if (emailContentFormatterConfiguration.getHolidayCountriesWhitelist() != null) {
      targetDimensions.put(EVENT_FILTER_COUNTRY, emailContentFormatterConfiguration.getHolidayCountriesWhitelist());
    }
    List<EventDTO> holidays = getHolidayEvents(eventStart, eventEnd, targetDimensions);
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
    templateData.put("detectionToAnomalyDetailsMap", functionAnomalyReports.asMap());
    templateData.put("metricToAnomalyDetailsMap", metricAnomalyReports.asMap());
    templateData.put("functionToId", functionToId);
  }
}
