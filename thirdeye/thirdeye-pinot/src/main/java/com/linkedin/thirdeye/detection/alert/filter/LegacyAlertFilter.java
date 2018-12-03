/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.alert.filter;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import com.linkedin.thirdeye.detection.spi.model.AnomalySlice;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilter;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import com.linkedin.thirdeye.detector.email.filter.BaseAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LegacyAlertFilter extends DetectionAlertFilter {
  private final static Logger LOG = LoggerFactory.getLogger(LegacyAlertFilter.class);

  private static final String PROP_LEGACY_ALERT_FILTER_CONFIGS = "legacyAlertFilterConfigs";
  private static final String PROP_LEGACY_ALERT_FILTER_CLASS_NAME = "legacyAlertFilterClassName";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";

  private final List<Long> detectionConfigIds;
  private final Map<Long, Long> vectorClocks;

  public LegacyAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) throws Exception {
    super(provider, config, endTime);

    this.detectionConfigIds = ConfigUtils.getLongs(this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    this.vectorClocks = this.config.getVectorClocks();
  }

  @Override
  public DetectionAlertFilterResult run() throws Exception {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();

    Map<String, Set<String>> recipientsMap = ConfigUtils.getMap(this.config.getProperties().get(PROP_RECIPIENTS));
    Set<String> to = (recipientsMap.get(PROP_TO) == null) ? Collections.emptySet() : new HashSet<>(recipientsMap.get(PROP_TO));
    Set<String> cc = (recipientsMap.get(PROP_CC) == null) ? Collections.emptySet() : new HashSet<>(recipientsMap.get(PROP_CC));
    Set<String> bcc = (recipientsMap.get(PROP_BCC) == null) ? Collections.emptySet() : new HashSet<>(recipientsMap.get(PROP_BCC));
    DetectionAlertFilterRecipients recipients = new DetectionAlertFilterRecipients(to, cc, bcc);

    Map<String, Object> alertFilterConfig = MapUtils.getMap(config.getProperties(), PROP_LEGACY_ALERT_FILTER_CONFIGS);
    if (alertFilterConfig == null || alertFilterConfig.size() == 0) {
      LOG.warn("alertFilterConfig is null or empty in notification group {}", this.config.getId());
    }

    for (Long functionId : this.detectionConfigIds) {
      long startTime = MapUtils.getLong(this.vectorClocks, functionId, 0L);

      AnomalySlice slice = new AnomalySlice()
          .withStart(startTime)
          .withEnd(this.endTime);

      Collection<MergedAnomalyResultDTO> candidates;
      if (this.config.isOnlyFetchLegacyAnomalies()) {
        candidates = this.provider.fetchLegacyAnomalies(Collections.singletonList(slice), functionId).get(slice);
      } else {
        candidates = this.provider.fetchAnomalies(Collections.singletonList(slice), functionId).get(slice);
      }

      BaseAlertFilter alertFilter = new DummyAlertFilter();
      if (config.getProperties().containsKey(PROP_LEGACY_ALERT_FILTER_CLASS_NAME)) {
        String className = MapUtils.getString(config.getProperties(), PROP_LEGACY_ALERT_FILTER_CLASS_NAME);
        alertFilter = (BaseAlertFilter) Class.forName(className).newInstance();
        Map<String, String> params = MapUtils.getMap(alertFilterConfig, functionId.toString());
        if (params == null) {
          LOG.warn("AlertFilter cannot be found for function {} in notification group {}", functionId, this.config.getId());
        }

        alertFilter.setParameters(params);
      }

      BaseAlertFilter finalAlertFilter = alertFilter;
      Collection<MergedAnomalyResultDTO> anomalies =
          Collections2.filter(candidates, new Predicate<MergedAnomalyResultDTO>() {
            @Override
            public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomaly) {
              return mergedAnomaly != null && !mergedAnomaly.isChild() && finalAlertFilter.isQualified(mergedAnomaly);
            }
          });

      if (result.getResult().isEmpty()) {
        result.addMapping(recipients, new HashSet<>(anomalies));
      } else {
        result.getResult().get(recipients).addAll(anomalies);
      }
    }

    return result;
  }
}
