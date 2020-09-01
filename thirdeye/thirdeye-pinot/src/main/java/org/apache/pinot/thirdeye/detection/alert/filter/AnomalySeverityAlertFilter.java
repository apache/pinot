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

package org.apache.pinot.thirdeye.detection.alert.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.constant.AnomalySeverity;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalySubscriptionGroupNotificationManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalySubscriptionGroupNotificationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.detection.alert.StatefulDetectionAlertFilter;
import org.apache.pinot.thirdeye.detection.annotation.AlertFilter;


/**
 * The detection alert filter that can send notifications through multiple channels
 * to a set of unconditional and another set of conditional recipients, based on the
 * value of a specified anomaly severities.
 *
 * You can configure anomaly severities along with a variety of alerting
 * channels and reference links.
 *
 * This alert pipeline have the capability of re-notify anomalies if the anomaly's severity is
 * changed after it's created.
 *
 * <pre>
 * severityRecipients:
 *   - severity:
 *       - LOW
 *     notify:
 *       jiraScheme:
 *         project: PROJECT
 *         assignee: oncall
 *       emailScheme:
 *         recipients:
 *           to:
 *           - "oncall@comany.com"
 *   - severity:
 *       - HIGH
 *       - CRITICAL
 *     notify:
 *       jiraScheme:
 *         project: PROJECT
 *         assignee: manager
 * </pre>
 */
@AlertFilter(type = "SEVERITY_ALERTER_PIPELINE")
public class AnomalySeverityAlertFilter extends StatefulDetectionAlertFilter {
  public static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  public static final String PROP_SEVERITY = "severity";
  public static final String PROP_NOTIFY = "notify";
  public static final String PROP_REF_LINKS = "referenceLinks";
  public static final String PROP_SEVERITY_RECIPIENTS = "severityRecipients";

  private final List<Map<String, Object>> severityRecipients;
  private final List<Long> detectionConfigIds;

  private final AnomalySubscriptionGroupNotificationManager anomalySubscriptionGroupNotificationDAO;


  public AnomalySeverityAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);
    this.severityRecipients = ConfigUtils.getList(this.config.getProperties().get(PROP_SEVERITY_RECIPIENTS));
    this.detectionConfigIds = ConfigUtils.getLongs(this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    this.anomalySubscriptionGroupNotificationDAO =
        DAORegistry.getInstance().getAnomalySubscriptionGroupNotificationManager();
  }

  @Override
  public DetectionAlertFilterResult run() {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();

    // retrieve the anomalies based on vector clocks
    Set<MergedAnomalyResultDTO> anomalies = this.filter(this.makeVectorClocks(this.detectionConfigIds));
    // find the anomalies that needs re-notifying.
    anomalies.addAll(this.retrieveRenotifyAnomalies(this.detectionConfigIds));
    // Prepare mapping from severity-recipients to anomalies
    for (Map<String, Object> severityRecipient : this.severityRecipients) {
      List<AnomalySeverity> severities = ConfigUtils.getList(severityRecipient.get(PROP_SEVERITY))
          .stream()
          .map(s -> AnomalySeverity.valueOf((String) s))
          .collect(Collectors.toList());
      Set<MergedAnomalyResultDTO> notifyAnomalies = new HashSet<>();
      for (MergedAnomalyResultDTO anomaly : anomalies) {
        if (severities.contains(anomaly.getSeverity())) {
          notifyAnomalies.add(anomaly);
        }
      }

      if (!notifyAnomalies.isEmpty()) {
        DetectionAlertConfigDTO subsConfig = SubscriptionUtils.makeChildSubscriptionConfig(config,
            ConfigUtils.getMap(severityRecipient.get(PROP_NOTIFY)),
            ConfigUtils.getMap(severityRecipient.get(PROP_REF_LINKS)));
        result.addMapping(new DetectionAlertFilterNotification(subsConfig), notifyAnomalies);
      }
    }

    // Notify the remaining anomalies to default recipients
    Set<MergedAnomalyResultDTO> allNotifiedAnomalies = new HashSet<>(result.getAllAnomalies());
    Set<MergedAnomalyResultDTO> defaultAnomalies = new HashSet<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (!allNotifiedAnomalies.contains(anomaly)) {
        defaultAnomalies.add(anomaly);
      }
    }
    if (!defaultAnomalies.isEmpty()) {
      result.addMapping(new DetectionAlertFilterNotification(config), defaultAnomalies);
    }

    return result;
  }


  protected Collection<MergedAnomalyResultDTO> retrieveRenotifyAnomalies(Collection<Long> detectionConfigIds) {
    // find if any notification is needed
    List<AnomalySubscriptionGroupNotificationDTO> anomalySubscriptionGroupNotificationDTOs =
        this.anomalySubscriptionGroupNotificationDAO.findByPredicate(
            Predicate.IN("detectionConfigId", detectionConfigIds.toArray()));

    List<Long> anomalyIds = new ArrayList<>();
    for (AnomalySubscriptionGroupNotificationDTO anomalySubscriptionGroupNotification : anomalySubscriptionGroupNotificationDTOs) {
      // notify the anomalies if this subscription group have not sent out this anomaly yet
      if (!anomalySubscriptionGroupNotification.getNotifiedSubscriptionGroupIds().contains(this.config.getId())) {
        anomalyIds.add(anomalySubscriptionGroupNotification.getAnomalyId());
        // add this subscription group to the notification record and update
        anomalySubscriptionGroupNotification.getNotifiedSubscriptionGroupIds().add(this.config.getId());
        this.anomalySubscriptionGroupNotificationDAO.save(anomalySubscriptionGroupNotification);
      }
    }
    return anomalyIds.isEmpty() ? Collections.emptyList()
        : DAORegistry.getInstance().getMergedAnomalyResultDAO().findByIds(anomalyIds);
  }
}
