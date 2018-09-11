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

package com.linkedin.thirdeye.anomalydetection.model.data;

import java.util.List;
import java.util.Properties;
import org.joda.time.Interval;

public interface DataModel {
  /**
   * Initializes this model with the given properties.
   * @param properties the given properties.
   */
  void init(Properties properties);

  /**
   * Returns properties of this model.
   */
  Properties getProperties();

  /**
   * Given the interval of the observed (current) time series, returns all intervals of time series
   * that are used by this anomaly detection.
   * @param monitoringWindowStartTime inclusive milliseconds
   * @param monitoringWindowEndTime exclusive milliseconds
   * @return all intervals of time series that are used by this anomaly detection.
   */
  List<Interval> getAllDataIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime);

  /**
   * Given the interval of the observed (current) time series, returns the intervals of time series
   * that are used by this prediction model for training purpose.
   *
   * @param monitoringWindowStartTime inclusive milliseconds
   * @param monitoringWindowEndTime exclusive milliseconds
   *
   * @return intervals of time series that are used for training.
   */
  List<Interval> getTrainingDataIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime);
}
