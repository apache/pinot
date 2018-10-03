/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detector.metric.transfer;

/**
 * Scaling factor to rescale the metric within a time window
 * For any ts within tht timeWindow the value will be modified as
 *   value' <- value * scalingFactor
 */
public class ScalingFactor {

  public static final String SCALING_FACTOR = "scalingFactor";

  /** The time range of the scaling window */
  private final long windowStart; // inclusive
  private final long windowEnd; // exclusive

  /** Scaling factor during the scaling window */
  private final double scalingFactor;

  /**
   * Construct a scaling factor with the given window start, inclusive, and window end,
   * exclusive, and scaling factor.
   * @param windowStart the start time of the window, inclusive
   * @param windowEnd the end time of the window, exclusive
   * @param scalingFactor the scaling factor
   */
  public ScalingFactor(long windowStart, long windowEnd, double scalingFactor) {
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.scalingFactor = scalingFactor;
  }

  public double getScalingFactor() {
    return scalingFactor;
  }

  /**
   * Checks if the given timestamp is located between the start, inclusive, and end, exclusive,
   * window of this scaling.
   *
   * @param timestamp the timestamp to check
   * @return true if the timestamp is located between start and end window
   */
  public boolean isInTimeWindow(long timestamp) {
    return windowStart <= timestamp && timestamp < windowEnd;
  }
}
