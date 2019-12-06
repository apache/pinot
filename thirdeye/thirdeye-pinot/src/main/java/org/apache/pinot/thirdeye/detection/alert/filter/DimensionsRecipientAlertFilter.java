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

import com.google.common.collect.Multimap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.detection.alert.StatefulDetectionAlertFilter;
import org.apache.pinot.thirdeye.detection.annotation.AlertFilter;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;


/**
 * The detection alert filter that can send notifications through multiple channels
 * to a set of unconditional and another set of conditional recipients, based on the
 * value of a specified anomaly dimension combinations.
 *
 * Unlike {@link DimensionDetectionAlertFilter}, here you can configure multiple dimension
 * combinations along with a variety of alerting channels and reference links.
 *
 * <pre>
 * dimensionRecipients:
 *   - dimensions:
 *       country: IN
 *       device: Android
 *     notify:
 *       jiraScheme:
 *         project: ANDROID
 *         assignee: android-oncall
 *       emailScheme:
 *         recipients:
 *           to:
 *           - "android-team@comany.com"
 *   - dimension:
 *       country: US
 *       device: IOS
 *     notify:
 *       jiraScheme:
 *         project: IOS
 *         assignee: ios-oncall
 * </pre>
 */
@AlertFilter(type = "DIMENSIONS_ALERTER_PIPELINE")
public class DimensionsRecipientAlertFilter extends StatefulDetectionAlertFilter {
  public static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  public static final String PROP_DIMENSION = "dimensions";
  public static final String PROP_NOTIFY = "notify";
  public static final String PROP_REF_LINKS = "referenceLinks";
  public static final String PROP_DIMENSION_RECIPIENTS = "dimensionRecipients";
  private static final String PROP_SEND_ONCE = "sendOnce";

  final List<Map<String, Object>> dimensionRecipients;
  final List<Long> detectionConfigIds;
  final boolean sendOnce;

  public DimensionsRecipientAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);
    this.dimensionRecipients = ConfigUtils.getList(this.config.getProperties().get(PROP_DIMENSION_RECIPIENTS));
    this.detectionConfigIds = ConfigUtils.getLongs(this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    this.sendOnce = MapUtils.getBoolean(this.config.getProperties(), PROP_SEND_ONCE, true);
  }

  @Override
  public DetectionAlertFilterResult run(Map<Long, Long> vectorClocks, long highWaterMark) {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();
    final long minId = getMinId(highWaterMark);

    Set<MergedAnomalyResultDTO> anomalies = this.filter(this.makeVectorClocks(this.detectionConfigIds), minId);

    // Prepare mapping from dimension-recipients to anomalies
    for (Map<String, Object> dimensionRecipient : this.dimensionRecipients) {
      Multimap<String, String> dimensionFilters = ConfigUtils.getMultimap(dimensionRecipient.get(PROP_DIMENSION));
      Set<MergedAnomalyResultDTO> notifyAnomalies = new HashSet<>();
      for (MergedAnomalyResultDTO anomaly : anomalies) {
        Multimap<String, String> anomalousDims = MetricEntity.fromURN(anomaly.getMetricUrn()).getFilters();
        if (anomalousDims.entries().containsAll(dimensionFilters.entries())) {
          notifyAnomalies.add(anomaly);
        }
      }

      if (!notifyAnomalies.isEmpty()) {
        DetectionAlertConfigDTO subsConfig = SubscriptionUtils.makeChildSubscriptionConfig(config,
            ConfigUtils.getMap(dimensionRecipient.get(PROP_NOTIFY)), ConfigUtils.getMap(dimensionRecipient.get(PROP_REF_LINKS)));
        result.addMapping(
            new DetectionAlertFilterNotification(subsConfig, dimensionFilters),
            notifyAnomalies);
      }
    }

    // Notify the remaining anomalies to default recipients
    Set<MergedAnomalyResultDTO> notifiedAnomalies = new HashSet<>(result.getAllAnomalies());
    Set<MergedAnomalyResultDTO> defaultAnomalies = new HashSet<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (!notifiedAnomalies.contains(anomaly)) {
        defaultAnomalies.add(anomaly);
      }
    }
    if (!defaultAnomalies.isEmpty()) {
      result.addMapping(new DetectionAlertFilterNotification(config), defaultAnomalies);
    }

    return result;
  }

  private long getMinId(long highWaterMark) {
    if (this.sendOnce) {
      return highWaterMark + 1;
    } else {
      return 0;
    }
  }
}
