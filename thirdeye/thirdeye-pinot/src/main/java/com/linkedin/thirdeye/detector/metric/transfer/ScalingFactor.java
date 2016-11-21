package com.linkedin.thirdeye.detector.metric.transfer;

import com.linkedin.thirdeye.api.TimeRange;


/**
 * Scaling factor to rescale the metric within a time window
 * For any ts within tht timeWindow the value will be modified as
 *   value' <- value * scalingFactor
 */
public class ScalingFactor {

  /** The time range of the scaling window */
  private final TimeRange timeWindow;

  /** Scaling factor during the scaling window */
  private final double scalingFactor;

  public ScalingFactor(TimeRange timeWindow, double scalingFactor) {
    this.timeWindow = timeWindow;
    this.scalingFactor = scalingFactor;
  }

  public double getScalingFactor() {
    return scalingFactor;
  }

  public TimeRange getTimeWindow() {
    return timeWindow;
  }

}
