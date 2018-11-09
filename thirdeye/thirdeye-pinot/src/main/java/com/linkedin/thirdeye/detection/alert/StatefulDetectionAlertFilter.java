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

package com.linkedin.thirdeye.detection.alert;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.spi.model.AnomalySlice;
import com.linkedin.thirdeye.detection.DataProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;


public abstract class StatefulDetectionAlertFilter extends DetectionAlertFilter {
  public StatefulDetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);
  }

  @Override
  public DetectionAlertFilterResult run() throws Exception {
    return this.run(this.config.getVectorClocks(), this.getAnomalyMinId());
  }

  protected abstract DetectionAlertFilterResult run(Map<Long, Long> vectorClocks, long highWaterMark);

  protected final Set<MergedAnomalyResultDTO> filter(Map<Long, Long> vectorClocks, final long minId) {
    // retrieve all candidate anomalies
    Set<MergedAnomalyResultDTO> allAnomalies = new HashSet<>();
    for (Long detectionConfigId : vectorClocks.keySet()) {
      long startTime = vectorClocks.get(detectionConfigId);

      AnomalySlice slice = new AnomalySlice().withStart(startTime).withEnd(this.endTime);
      Collection<MergedAnomalyResultDTO> candidates = this.provider.fetchAnomalies(Collections.singletonList(slice), detectionConfigId).get(slice);

      Collection<MergedAnomalyResultDTO> anomalies =
          Collections2.filter(candidates, new Predicate<MergedAnomalyResultDTO>() {
            @Override
            public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
              return mergedAnomalyResultDTO != null
                  && !mergedAnomalyResultDTO.isChild()
                  && !AlertUtils.hasFeedback(mergedAnomalyResultDTO)
                  && (mergedAnomalyResultDTO.getId() == null || mergedAnomalyResultDTO.getId() >= minId);
            }
          });

      allAnomalies.addAll(anomalies);
    }
    return allAnomalies;
  }

  protected final Map<Long, Long> makeVectorClocks(Collection<Long> detectionConfigIds) {
    Map<Long, Long> clocks = new HashMap<>();

    for (Long id : detectionConfigIds) {
      clocks.put(id, MapUtils.getLong(this.config.getVectorClocks(), id, 0L));
    }

    return clocks;
  }

  private long getAnomalyMinId() {
    if (this.config.getHighWaterMark() != null) {
      return this.config.getHighWaterMark();
    }
    return 0;
  }
}
