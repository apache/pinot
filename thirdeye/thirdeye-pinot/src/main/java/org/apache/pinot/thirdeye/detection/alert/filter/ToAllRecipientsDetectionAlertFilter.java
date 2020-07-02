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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.detection.alert.StatefulDetectionAlertFilter;
import java.util.List;
import java.util.Set;
import org.apache.pinot.thirdeye.detection.annotation.AlertFilter;


/**
 * The detection alert filter that sends the anomaly email to all recipients
 */
@AlertFilter(type = "DEFAULT_ALERTER_PIPELINE")
public class ToAllRecipientsDetectionAlertFilter extends StatefulDetectionAlertFilter {

  public static final String PROP_RECIPIENTS = "recipients";
  public static final String PROP_TO = "to";
  public static final String PROP_CC = "cc";
  public static final String PROP_BCC = "bcc";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";

  final SetMultimap<String, String> recipients;
  List<Long> detectionConfigIds;

  public ToAllRecipientsDetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);

    this.recipients = HashMultimap.create(ConfigUtils.<String, String>getMultimap(this.config.getProperties().get(PROP_RECIPIENTS)));
    this.detectionConfigIds = ConfigUtils.getLongs(this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
  }

  @Override
  public DetectionAlertFilterResult run() {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();

    // Fetch all the anomalies to be notified to the recipients
    Set<MergedAnomalyResultDTO> anomalies = this.filter(this.makeVectorClocks(this.detectionConfigIds));

    // Handle legacy recipients yaml syntax
    if (SubscriptionUtils.isEmptyEmailRecipients(this.config) && CollectionUtils.isNotEmpty(this.recipients.get(PROP_TO))) {
      // recipients are configured using the older syntax
      this.config.setAlertSchemes(generateAlertSchemeProps(
          this.config,
          this.recipients.get(PROP_TO),
          this.recipients.get(PROP_CC),
          this.recipients.get(PROP_BCC)));
    }

    return result.addMapping(new DetectionAlertFilterNotification(this.config), anomalies);
  }
}
