package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DetectionAlertFilterResult {
  Map<List<MergedAnomalyResultDTO>, List<String>> result;

  public DetectionAlertFilterResult() {
    this.result = new HashMap<>();
  }

  public DetectionAlertFilterResult(Map<List<MergedAnomalyResultDTO>, List<String>> result, long lastTimestamp) {
    this.result = result;
  }

  public Map<List<MergedAnomalyResultDTO>, List<String>> getResult() {
    return result;
  }

  public DetectionAlertFilterResult addMapping(List<MergedAnomalyResultDTO> anomalies, Collection<String> recipients) {
    if (!this.result.containsKey(anomalies)) {
      this.result.put(anomalies, new ArrayList<String>());
    }
    this.result.get(anomalies).addAll(recipients);
    return this;
  }
}
