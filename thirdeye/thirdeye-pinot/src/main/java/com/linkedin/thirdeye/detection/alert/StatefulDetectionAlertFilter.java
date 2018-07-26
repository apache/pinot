package com.linkedin.thirdeye.detection.alert;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
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

      AnomalySlice slice = new AnomalySlice().withConfigId(detectionConfigId).withStart(startTime).withEnd(this.endTime);
      Collection<MergedAnomalyResultDTO> candidates = this.provider.fetchAnomalies(Collections.singletonList(slice)).get(slice);

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
