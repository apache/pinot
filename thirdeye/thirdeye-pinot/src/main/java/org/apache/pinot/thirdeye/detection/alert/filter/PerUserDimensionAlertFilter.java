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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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


/**
 * The detection alert filter that sends the anomaly email to a set of unconditional and another
 * set of conditional recipients, based on the value of a specified anomaly dimension
 *
 * However, unlike the {$link {@link DimensionDetectionAlertFilter}}, this filter consolidates
 * anomalies across dimensions and sends alert per user. This is used in scenarios where there
 * is an overlap of recipients across dimensions, to reduce the number of alerts sent out to a
 * specific user.
 */
@AlertFilter(type = "PER_USER_DIMENSION_ALERTER_PIPELINE")
public class PerUserDimensionAlertFilter extends StatefulDetectionAlertFilter {
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String PROP_DIMENSION = "dimension";
  private static final String PROP_DIMENSION_RECIPIENTS = "dimensionRecipients";
  private static final String PROP_SEND_ONCE = "sendOnce";

  final String dimension;
  final Map<String, Set<String>> recipients;
  final SetMultimap<String, String> dimensionRecipients;
  final List<Long> detectionConfigIds;
  final boolean sendOnce;

  public PerUserDimensionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);
    Preconditions.checkNotNull(config.getProperties().get(PROP_DIMENSION), "Dimension name not specified");

    this.recipients = ConfigUtils.getMap(this.config.getProperties().get(PROP_RECIPIENTS));
    this.dimension = MapUtils.getString(this.config.getProperties(), PROP_DIMENSION);
    this.dimensionRecipients = HashMultimap.create(ConfigUtils.<String, String>getMultimap(this.config.getProperties().get(PROP_DIMENSION_RECIPIENTS)));
    this.detectionConfigIds = ConfigUtils.getLongs(this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    this.sendOnce = MapUtils.getBoolean(this.config.getProperties(), PROP_SEND_ONCE, true);
  }

  @Override
  public DetectionAlertFilterResult run(Map<Long, Long> vectorClocks, long highWaterMark) {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();

    final long minId = getMinId(highWaterMark);

    Set<MergedAnomalyResultDTO> anomalies = this.filter(this.makeVectorClocks(this.detectionConfigIds), minId);

    // group anomalies by dimensions value
    Multimap<String, MergedAnomalyResultDTO> grouped = Multimaps.index(anomalies, new Function<MergedAnomalyResultDTO, String>() {
      @Override
      public String apply(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
        return MapUtils.getString(mergedAnomalyResultDTO.getDimensions(), PerUserDimensionAlertFilter.this.dimension, "");
      }
    });

    // generate recipients-anomalies mapping
    Map<String, List<MergedAnomalyResultDTO>> perUserAnomalies = new HashMap<>();
    for (Map.Entry<String, Collection<MergedAnomalyResultDTO>> entry : grouped.asMap().entrySet()) {
      Set<String> recipients = getRecipients(entry.getKey());
      for (String recipient : recipients) {
        if (!perUserAnomalies.containsKey(recipient)) {
          List<MergedAnomalyResultDTO> userAnomalies = new ArrayList<>();
          perUserAnomalies.put(recipient, userAnomalies);
        }
        perUserAnomalies.get(recipient).addAll(entry.getValue());
      }
    }

    for (Map.Entry<String, List<MergedAnomalyResultDTO>> userAnomalyMapping : perUserAnomalies.entrySet()) {
      result.addMapping(
          new DetectionAlertFilterNotification(
              generateNotificationSchemeProps(
                  this.config,
                  this.makeGroupRecipients(userAnomalyMapping.getKey()),
                  this.recipients.get(PROP_CC),
                  this.recipients.get(PROP_BCC))),
          new HashSet<>(userAnomalyMapping.getValue()));
    }

    return result;
  }

  private Set<String> getRecipients(String key) {
    Set<String> recipients = new HashSet<>();
    if (this.dimensionRecipients.containsKey(key)) {
      recipients.addAll(this.dimensionRecipients.get(key));
    }
    return recipients;
  }

  private Set<String> makeGroupRecipients(String key) {
    Set<String> recipients = new HashSet<>(this.recipients.get(PROP_TO));
    recipients.add(key);
    return recipients;
  }

  private long getMinId(long highWaterMark) {
    if (this.sendOnce) {
      return highWaterMark + 1;
    } else {
      return 0;
    }
  }
}
