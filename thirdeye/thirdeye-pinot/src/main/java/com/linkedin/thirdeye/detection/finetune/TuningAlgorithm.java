package com.linkedin.thirdeye.detection.finetune;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;


/**
 * The interface Tuning algorithm.
 */
public interface TuningAlgorithm {
  /**
   * Fit the time series and anomalies between start and end, and score the detection configs.
   *
   * @param start the start
   * @param end the end
   * @throws Exception the exception
   */
  public void fit(long start, long end) throws Exception;

  /**
   * Return the best detection config detection config dto.
   *
   * @return the detection config dto
   */
  public DetectionConfigDTO bestDetectionConfig();
}
