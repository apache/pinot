package com.linkedin.thirdeye.detection.alert;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * The Detection alert filter result.
 */
public class DetectionAlertFilterResult {
  /**
   * The Result.
   */
  private Map<List<MergedAnomalyResultDTO>, List<String>> result;

  Map<Long, Long> vectorClocks;
  /**
   * Instantiates a new Detection alert filter result.
   */
  public DetectionAlertFilterResult() {
    this.result = new HashMap<>();
  }

  public Map<Long, Long> getVectorClocks() {
    return vectorClocks;
  }

  public void setVectorClocks(Map<Long, Long> vectorClock) {
    this.vectorClocks = vectorClock;
  }

  /**
   * Instantiates a new Detection alert filter result.
   *
   * @param result the result
   * @param lastTimestamp the last timestamp
   */
  public DetectionAlertFilterResult(Map<List<MergedAnomalyResultDTO>, List<String>> result, long lastTimestamp) {
    Preconditions.checkNotNull(result);
    this.result = result;
  }

  /**
   * Gets result.
   *
   * @return the result
   */
  public Map<List<MergedAnomalyResultDTO>, List<String>> getResult() {
    return result;
  }

  /**
   * Gets all anomalies.
   *
   * @return the all anomalies
   */
  public List<MergedAnomalyResultDTO> getAllAnomalies() {
    List<MergedAnomalyResultDTO> allAnomalies = new ArrayList<>();
    for (List<MergedAnomalyResultDTO> anomalies : result.keySet()) {
      allAnomalies.addAll(anomalies);
    }
    return allAnomalies;
  }

  /**
   * Add a mapping from anomalies to recipients in this detection alert filter result.
   *
   * @param anomalies the anomalies
   * @param recipients the recipients
   * @return the detection alert filter result
   */
  public DetectionAlertFilterResult addMapping(List<MergedAnomalyResultDTO> anomalies, Collection<String> recipients) {
    if (!this.result.containsKey(anomalies)) {
      this.result.put(anomalies, new ArrayList<String>());
    }
    this.result.get(anomalies).addAll(recipients);
    return this;
  }
}
