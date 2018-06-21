package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;


/**
 * The Detection alert filter.
 */
public abstract class DetectionAlertFilter {
  /**
   * The Provider.
   */
  protected final DataProvider provider;
  /**
   * The Config.
   */
  protected final DetectionAlertConfigDTO config;
  /**
   * The End time.
   */
  protected final long endTime;

  /**
   * Instantiates a new Detection alert filter.
   *
   * @param provider the provider
   * @param config the config
   * @param endTime the end time
   */
  public DetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    this.provider = provider;
    this.config = config;
    this.endTime = endTime;
  }

  /**
   * Returns a detection alert filter result for the time range between {@code startTime} and {@code endTime}.
   *
   * @return alert filter result
   * @throws Exception the exception
   */
  public abstract DetectionAlertFilterResult run() throws Exception;
}
