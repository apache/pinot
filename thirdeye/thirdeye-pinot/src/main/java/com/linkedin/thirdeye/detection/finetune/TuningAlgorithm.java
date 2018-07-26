package com.linkedin.thirdeye.detection.finetune;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;


/**
 * The interface Tuning algorithm.
 */
public interface TuningAlgorithm {
  /**
   * Fit the time series and anomalies between start and end time stamps, and score the detection configs.
   *
   * @param slice anomaly slice
   * @throws Exception the exception
   */
  void fit(AnomalySlice slice) throws Exception;

  /**
   * Return the best detection config detection config dto.
   *
   * @return the detection config dto
   */
  DetectionConfigDTO bestDetectionConfig();
}
