package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;


public abstract class DetectionAlertFilter {
  protected final DataProvider provider;
  protected final AlertConfigDTO config;
  protected final long startTime;
  protected final long endTime;

  public DetectionAlertFilter(DataProvider provider, AlertConfigDTO config, long startTime, long endTime) {
    this.provider = provider;
    this.config = config;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  /**
   * Returns a detection alert filter result for the time range between {@code startTime} and {@code endTime}.
   *
   * @return alert filter result
   * @throws Exception
   */
  public abstract DetectionAlertFilterResult run() throws Exception;
}
