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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This email formatter generates a report/alert from the anomalies having a groupKey
 */
public class EntityGroupByContentFormatter extends BaseEmailContentFormatter{
  private static final Logger LOG = LoggerFactory.getLogger(EntityGroupByContentFormatter.class);

  private static final String EMAIL_TEMPLATE = "emailTemplate";
  private static final String DEFAULT_EMAIL_TEMPLATE = "entity-groupby-anomaly-report.ftl";

  static final String PROP_ENTITY_NAME = "entityName";
  static final String PROP_ANOMALY_SCORE = "groupScore";
  static final String PROP_GROUP_KEY = "groupKey";

  private DetectionConfigManager configDAO = null;
  private Multimap<String, AnomalyReportEntity> entityToAnomaliesMap = ArrayListMultimap.create();
  private Map<String, List<AnomalyReportEntity>> entityToSortedAnomaliesMap = new HashMap<>();
  private Map<AnomalyReportEntity, Double> anomalyToGroupScoreMap = new HashMap<>();
  private Map<String, String> anomalyToChildIdsMap = new HashMap<>();

  public EntityGroupByContentFormatter() {}

  @Override
  public void init(Properties properties, EmailContentFormatterConfiguration configuration) {
    super.init(properties, configuration);
    this.emailTemplate = properties.getProperty(EMAIL_TEMPLATE, DEFAULT_EMAIL_TEMPLATE);
    this.configDAO = DAORegistry.getInstance().getDetectionConfigManager();
  }

  @Override
  protected void updateTemplateDataByAnomalyResults(Map<String, Object> templateData,
      Collection<AnomalyResult> anomalies, EmailContentFormatterContext context) {
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
      entityToSortedAnomaliesMap.put(entityName, groupedAnomalies);
    }

    templateData.put("emailHeading", config.getName());
    templateData.put("anomalyToChildIdsMap", anomalyToChildIdsMap);
    templateData.put("entityToSortedAnomaliesMap", entityToSortedAnomaliesMap);
  }

  /**
   * Recursively find the anomalies having a groupKey and display them in the email
   */
  private void updateEntityToAnomalyDetailsMap(MergedAnomalyResultDTO anomaly, DetectionConfigDTO detectionConfig) {
    if (anomaly.getProperties() != null && anomaly.getProperties().containsKey(PROP_GROUP_KEY)) {
      AnomalyReportEntity anomalyReport = new AnomalyReportEntity(String.valueOf(anomaly.getId()),
          getAnomalyURL(anomaly, emailContentFormatterConfiguration.getDashboardHost()),
          anomaly.getAvgBaselineVal(),
          anomaly.getAvgCurrentVal(), 0d, null,
          getTimeDiffInHours(anomaly.getStartTime(), anomaly.getEndTime()), getFeedbackValue(anomaly.getFeedback()),
          detectionConfig.getName(), detectionConfig.getDescription(), anomaly.getMetric(),
          getDateString(anomaly.getStartTime(), dateTimeZone), getDateString(anomaly.getEndTime(), dateTimeZone),
          getTimezoneString(dateTimeZone), getIssueType(anomaly));

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
      anomalyReport.setEntityName(anomaly.getProperties().getOrDefault(PROP_ENTITY_NAME, "UNKNOWN_ENTITY"));

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
        entityToAnomaliesMap.put(anomaly.getProperties().get(PROP_ENTITY_NAME), anomalyReport);
      }
    } else {
      for (MergedAnomalyResultDTO childAnomaly : anomaly.getChildren()) {
        updateEntityToAnomalyDetailsMap(childAnomaly, detectionConfig);
      }
    }
  }
}
