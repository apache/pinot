package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;

/**
 * The grouped anomaly results for alerter. Each group of anomalies should be sent through the same email.
 */
public class GroupedAnomalyResults {
  private List<MergedAnomalyResultDTO> anomalyResults = new ArrayList<>();

  public List<MergedAnomalyResultDTO> getAnomalyResults() {
    return anomalyResults;
  }

  public void setAnomalyResults(List<MergedAnomalyResultDTO> anomalyResults) {
    this.anomalyResults = anomalyResults;
  }
}
