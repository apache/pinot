package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import java.util.Map;


public abstract class StatefulDetectionAlertFilter extends DetectionAlertFilter {
  public StatefulDetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);
  }

  @Override
  public DetectionAlertFilterResult run() throws Exception {
    return this.run(this.config.getVectorClocks(), this.getAnomalyMinId());
  }

  protected abstract DetectionAlertFilterResult run(Map<Long, Long> vectorClocks, long highWaterMark);

  private long getAnomalyMinId() {
    if (this.config.getHighWaterMark() != null) {
      return this.config.getHighWaterMark();
    }
    return 0;
  }
}
