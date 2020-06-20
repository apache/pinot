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

package org.apache.pinot.thirdeye.notification.content.templates;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.alert.util.AlertScreenshotHelper;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.util.ThirdEyeStringUtils;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator.*;


/**
 * This email formatter generates a report/alert from the anomalies having a groupKey
 * and optionally a whitelist metric which will be listed at the top of the alert report
 */
public class EntityGroupKeyContent extends BaseNotificationContent {
  private static final Logger LOG = LoggerFactory.getLogger(EntityGroupKeyContent.class);

  // Give some kind of special status to this metric entity. Anomalies from this whitelisted metric entity
  // will appear at the top of the alert report. Specify the entity name of the metric alert here.
  static final String PROP_ENTITY_WHITELIST = "entityWhitelist";
  static final String PROP_ENTITY_ANOMALIES_MAP_KEY = "entityToSortedAnomaliesMap";

  static final String PROP_ANOMALY_SCORE = "groupScore";
  static final String PROP_GROUP_KEY = "groupKey";

  private DetectionConfigManager configDAO = null;
  private Multimap<String, AnomalyReportEntity> entityToAnomaliesMap = ArrayListMultimap.create();
  private Multimap<String, AnomalyReportEntity> entityToSortedAnomaliesMap = ArrayListMultimap.create();
  private Set<Long> visitedAnomaliesSet = new HashSet<>();

  // WhitelistMetric is usually a top level metric which should be given special status in the alert report.
  // This map holds info on all the whitelisted metric anomalies which will appear at the top of the alert report.
  private Multimap<String, AnomalyReportEntity> whitelistMetricToAnomaliesMap = ArrayListMultimap.create();

  private Map<AnomalyReportEntity, Double> anomalyToGroupScoreMap = new HashMap<>();
  private Map<String, String> anomalyToChildIdsMap = new HashMap<>();
  private List<String> entityWhitelist = new ArrayList<>();

  public EntityGroupKeyContent() {}

  @Override
  public void init(Properties properties, ThirdEyeAnomalyConfiguration config) {
    super.init(properties, config);
    this.configDAO = DAORegistry.getInstance().getDetectionConfigManager();
    if (properties.containsKey(PROP_ENTITY_WHITELIST)) {
      // Support only one whitelist entity. This can be extended in the future.
      entityWhitelist.add(properties.get(PROP_ENTITY_WHITELIST).toString());
    }
  }

  @Override
  public String getTemplate() {
    return EntityGroupKeyContent.class.getSimpleName();
  }

  @Override
  public Map<String, Object> format(Collection<AnomalyResult> anomalies, DetectionAlertConfigDTO subsConfig) {
    Map<String, Object> templateData = super.getTemplateData(subsConfig, anomalies);

    DetectionConfigDTO config = null;
    Preconditions.checkArgument(anomalies != null && !anomalies.isEmpty(), "Report has empty anomalies");

    for (AnomalyResult anomalyResult : anomalies) {
      if (!(anomalyResult instanceof MergedAnomalyResultDTO)) {
        LOG.warn("Anomaly result {} isn't an instance of MergedAnomalyResultDTO. Skip from alert.", anomalyResult);
        continue;
      }

      MergedAnomalyResultDTO anomaly = (MergedAnomalyResultDTO) anomalyResult;
      if (config == null) {
        config = this.configDAO.findById(anomaly.getDetectionConfigId());
        Preconditions.checkNotNull(config, String.format("Cannot find detection config %d", anomaly.getDetectionConfigId()));
      }

      updateEntityToAnomalyDetailsMap(anomaly, config);
    }

    // Sort the anomalies within each entity by crticality score
    for (String entityName : entityToAnomaliesMap.keySet()) {
      List<AnomalyReportEntity> groupedAnomalies = (List<AnomalyReportEntity>) entityToAnomaliesMap.get(entityName);
      groupedAnomalies.sort((u1, u2) -> anomalyToGroupScoreMap.get(u2).compareTo(anomalyToGroupScoreMap.get(u1)));
      entityToSortedAnomaliesMap.putAll(entityName, groupedAnomalies);
    }

    // Insert anomaly snapshot image
    if (whitelistMetricToAnomaliesMap.size() == 1 && whitelistMetricToAnomaliesMap.values().size() == 1) {
      AnomalyReportEntity singleAnomaly = whitelistMetricToAnomaliesMap.values().iterator().next();
      try {
        this.imgPath = AlertScreenshotHelper.takeGraphScreenShot(singleAnomaly.getAnomalyId(), thirdEyeAnomalyConfig);
      } catch (Exception e) {
        LOG.error("Exception while embedding screenshot for anomaly {}", singleAnomaly.getAnomalyId(), e);
      }
    }

    templateData.put("anomalyDetails", whitelistMetricToAnomaliesMap.values());
    templateData.put("whitelistMetricToAnomaliesMap", whitelistMetricToAnomaliesMap.asMap());
    templateData.put("emailHeading", config.getName());
    templateData.put("emailDescription", config.getDescription());
    templateData.put("anomalyToChildIdsMap", anomalyToChildIdsMap);
    templateData.put(PROP_ENTITY_ANOMALIES_MAP_KEY, entityToSortedAnomaliesMap.asMap());

    return templateData;
  }

  /**
   * Recursively find the anomalies having a groupKey and display them in the email
   */
  private void updateEntityToAnomalyDetailsMap(MergedAnomalyResultDTO anomaly, DetectionConfigDTO detectionConfig) {
    Properties props = new Properties();
    props.putAll(anomaly.getProperties());
    double lift = BaseNotificationContent.getLift(anomaly.getAvgCurrentVal(), anomaly.getAvgBaselineVal());
    AnomalyReportEntity anomalyReport = new AnomalyReportEntity(String.valueOf(anomaly.getId()),
        getAnomalyURL(anomaly, thirdEyeAnomalyConfig.getDashboardHost()),
        getPredictedValue(anomaly),
        getCurrentValue(anomaly),
        getFormattedLiftValue(anomaly, lift),
        getLiftDirection(lift),
        0d, getDimensionsList(anomaly.getDimensionMap()),
        getTimeDiffInHours(anomaly.getStartTime(), anomaly.getEndTime()), getFeedbackValue(anomaly.getFeedback()),
        detectionConfig.getName(), detectionConfig.getDescription(), anomaly.getMetric(),
        getDateString(anomaly.getStartTime(), dateTimeZone), getDateString(anomaly.getEndTime(), dateTimeZone),
        getTimezoneString(dateTimeZone), getIssueType(anomaly), anomaly.getType().getLabel(),
        ThirdEyeStringUtils.encodeCompactedProperties(props), anomaly.getMetricUrn());

    // Extract out the whitelisted metrics
    if (anomaly.getProperties() != null && anomaly.getProperties().containsKey(PROP_SUB_ENTITY_NAME)
        && this.entityWhitelist.contains(anomaly.getProperties().get(PROP_SUB_ENTITY_NAME))) {
      if (whitelistMetricToAnomaliesMap.get(anomalyReport.getMetric()) != null) {
        whitelistMetricToAnomaliesMap.get(anomalyReport.getMetric()).add(anomalyReport);
      } else {
        whitelistMetricToAnomaliesMap.put(anomaly.getMetric(), anomalyReport);
      }
    }

    if (anomaly.getProperties() != null && anomaly.getProperties().containsKey(PROP_GROUP_KEY)) {
      // Criticality score ranges from [0 to infinity)
      double score = -1;
      if (anomaly.getProperties().containsKey(PROP_ANOMALY_SCORE)) {
        score = Double.parseDouble(anomaly.getProperties().get(PROP_ANOMALY_SCORE));
        anomalyReport.setScore(ThirdEyeUtils.getRoundedValue(score));
      } else {
        anomalyReport.setScore("-");
      }
      anomalyReport.setWeight(anomaly.getWeight());
      anomalyReport.setGroupKey(anomaly.getProperties().get(PROP_GROUP_KEY));
      anomalyReport.setEntityName(anomaly.getProperties().get(PROP_SUB_ENTITY_NAME));

      Set<Long> childIds = new HashSet<>();
      if (anomaly.getChildIds() != null) {
        childIds.addAll(anomaly.getChildIds());
      }

      // include notified alerts only in the email
      if (!includeSentAnomaliesOnly || anomaly.isNotified()) {
        // Freemarker doesn't support non-string key in the map. Hence the custom generated key using groupKey and startTime
        // See https://freemarker.apache.org/docs/app_faq.html#faq_nonstring_keys
        anomalyToChildIdsMap.put(anomalyReport.getGroupKey() + anomalyReport.getStartDateTime(), Joiner.on(",").join(childIds));

        anomalyToGroupScoreMap.put(anomalyReport, score);
        entityToAnomaliesMap.put(anomaly.getProperties().get(PROP_SUB_ENTITY_NAME), anomalyReport);
      }
    } else {
      for (MergedAnomalyResultDTO childAnomaly : anomaly.getChildren()) {
        // Since an anomaly can have two parents (due to merge across entity reports), we do not want them
        // to be displayed twice in the notification report.
        if (!visitedAnomaliesSet.contains(childAnomaly.getId())) {
          visitedAnomaliesSet.add(childAnomaly.getId());
          updateEntityToAnomalyDetailsMap(childAnomaly, detectionConfig);
        }
      }
    }
  }
}
