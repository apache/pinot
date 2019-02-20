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

import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.detection.alert.StatefulDetectionAlertFilter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.thirdeye.detection.annotation.AlertFilter;


/**
 * The detection alert filter that sends the anomaly email to all recipients
 */
@AlertFilter(type = "DEFAULT_ALERTER_PIPELINE")
public class ToAllRecipientsDetectionAlertFilter extends StatefulDetectionAlertFilter {

  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String PROP_SEND_ONCE = "sendOnce";

  Set<String> to;
  Set<String> cc;
  Set<String> bcc;
  final Map<String, Set<String>> recipients;
  List<Long> detectionConfigIds;
  boolean sendOnce;

  public ToAllRecipientsDetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);

    this.recipients = ConfigUtils.getMap(this.config.getProperties().get(PROP_RECIPIENTS));
    this.detectionConfigIds = ConfigUtils.getLongs(this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    this.sendOnce = MapUtils.getBoolean(this.config.getProperties(), PROP_SEND_ONCE, true);
  }

  @Override
  public DetectionAlertFilterResult run(Map<Long, Long> vectorClocks, long highWaterMark) {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();

    final long minId = getMinId(highWaterMark);

    Set<MergedAnomalyResultDTO> anomalies = this.filter(this.makeVectorClocks(this.detectionConfigIds), minId);

    to = (this.recipients.get(PROP_TO) == null) ? Collections.emptySet() : new HashSet<>(this.recipients.get(PROP_TO));
    cc = (this.recipients.get(PROP_CC) == null) ? Collections.emptySet() : new HashSet<>(this.recipients.get(PROP_CC));
    bcc = (this.recipients.get(PROP_BCC) == null) ? Collections.emptySet() : new HashSet<>(this.recipients.get(PROP_BCC));

    return result.addMapping(new DetectionAlertFilterRecipients(to, cc, bcc), anomalies);
  }

  private long getMinId(long highWaterMark) {
    if (this.sendOnce) {
      return highWaterMark + 1;
    } else {
      return 0;
    }
  }

}
