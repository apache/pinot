package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DummyAlertFilterAutoTune implements AlertFilterAutoTune {

  @Override
  public Map<String, String> initiateAutoTune(List<MergedAnomalyResultDTO> anomalyResults, int nExpectedAnomalies)
      throws Exception {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, String> tuneAlertFilter(List<MergedAnomalyResultDTO> anomalyResults, double currentPrecision,
      double currentRecall) throws Exception {
    // do nothing and return empty map
    return Collections.emptyMap();
  }

  @Override
  public boolean isUpdated() {
    return false;
  }
}
