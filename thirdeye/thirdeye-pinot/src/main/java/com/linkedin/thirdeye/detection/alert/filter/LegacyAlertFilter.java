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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
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
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;


public class LegacyAlertFilter extends DetectionAlertFilter {
  private static final String PROP_LEGACY_ALERT_FILTER_CONFIG = "legacyAlertFilterConfig";
  private static final String PROP_LEGACY_ALERT_CONFIG = "legacyAlertConfig";
  private static final String PROP_LEGACY_ALERT_FILTER_CLASS_NAME = "legacyAlertFilterClassName";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private AlertConfigDTO alertConfig;
  private BaseAlertFilter alertFilter;
  private final List<Long> detectionConfigIds;
  private final Map<Long, Long> vectorClocks;

  public LegacyAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) throws Exception {
    super(provider, config, endTime);

    String alertConfigStr = OBJECT_MAPPER.writeValueAsString(MapUtils.getMap(config.getProperties(), PROP_LEGACY_ALERT_CONFIG));
    alertConfig = OBJECT_MAPPER.readValue(alertConfigStr, AlertConfigDTO.class);
    alertFilter = new DummyAlertFilter();
    if (config.getProperties().containsKey(PROP_LEGACY_ALERT_FILTER_CLASS_NAME)) {
      String className = MapUtils.getString(config.getProperties(), PROP_LEGACY_ALERT_FILTER_CLASS_NAME);
      alertFilter = (BaseAlertFilter) Class.forName(className).newInstance();
      alertFilter.setParameters(MapUtils.getMap(config.getProperties(), PROP_LEGACY_ALERT_FILTER_CONFIG));
    }
    this.detectionConfigIds = ConfigUtils.getLongs(this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    this.vectorClocks = this.config.getVectorClocks();
  }

  @Override
  public DetectionAlertFilterResult run() {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();

    for (Long detectionConfigId : this.detectionConfigIds) {
      long startTime = MapUtils.getLong(this.vectorClocks, detectionConfigId, 0L);

      AnomalySlice slice =
          new AnomalySlice().withStart(startTime).withEnd(this.endTime);
      Collection<MergedAnomalyResultDTO> candidates =
          this.provider.fetchAnomalies(Collections.singletonList(slice), detectionConfigId).get(slice);

      Collection<MergedAnomalyResultDTO> anomalies =
          Collections2.filter(candidates, new Predicate<MergedAnomalyResultDTO>() {
            @Override
            public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomaly) {
              return mergedAnomaly != null && !mergedAnomaly.isChild() && alertFilter.isQualified(mergedAnomaly);
            }
          });

      result.addMapping(this.alertConfig.getReceiverAddresses(), new HashSet<>(anomalies));
    }

    return result;
  }
}
