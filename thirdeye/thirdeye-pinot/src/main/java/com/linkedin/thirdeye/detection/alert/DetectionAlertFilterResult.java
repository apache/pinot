package com.linkedin.thirdeye.detection.alert;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * The Detection alert filter result.
 */
public class DetectionAlertFilterResult {
  /**
   * The Result.
   */
  private Map<Set<String>, Set<MergedAnomalyResultDTO>> result;

  /**
   * Instantiates a new Detection alert filter result.
   */
  public DetectionAlertFilterResult() {
    this.result = new HashMap<>();
  }

  /**
   * Instantiates a new Detection alert filter result.
   *
   * @param result the result
   */
  public DetectionAlertFilterResult(Map<Set<String>, Set<MergedAnomalyResultDTO>> result) {
    Preconditions.checkNotNull(result);
    this.result = result;
  }

  /**
   * Gets result.
   *
   * @return the result
   */
  public Map<Set<String>, Set<MergedAnomalyResultDTO>> getResult() {
    return result;
  }

  /**
   * Gets all anomalies.
   *
   * @return the all anomalies
   */
  public List<MergedAnomalyResultDTO> getAllAnomalies() {
    List<MergedAnomalyResultDTO> allAnomalies = new ArrayList<>();
    for (Set<MergedAnomalyResultDTO> anomalies : result.values()) {
      allAnomalies.addAll(anomalies);
    }
    return allAnomalies;
  }

  /**
   * Add a mapping from anomalies to recipients in this detection alert filter result.
   *
   * @param recipients the recipients
   * @param anomalies the anomalies
   * @return the detection alert filter result
   */
  public DetectionAlertFilterResult addMapping(Set<String> recipients, Set<MergedAnomalyResultDTO> anomalies) {
    if (!this.result.containsKey(recipients)) {
      this.result.put(recipients, new HashSet<MergedAnomalyResultDTO>());
    }
    this.result.get(recipients).addAll(anomalies);
    return this;
  }
}
