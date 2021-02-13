/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.metrics.base;

import java.util.Map;
import java.util.concurrent.TimeUnit;


public interface PinotMetricsRegistry {

  void removeMetric(PinotMetricName name);

  <T>PinotGauge<T> newGauge(PinotMetricName name, PinotGauge<T> gauge);

  PinotMeter newMeter(PinotMetricName name, String eventType, TimeUnit unit);

  PinotCounter newCounter(PinotMetricName name);

  PinotTimer newTimer(PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit);

  PinotHistogram newHistogram(PinotMetricName name, boolean biased);

  /**
   * Returns an unmodifiable map of all metrics and their names.
   *
   * @return an unmodifiable map of all metrics and their names
   */
  Map<PinotMetricName, PinotMetric> allMetrics();

  Object getMetricsRegistry();

//  newAggregatedMeter(PinotMetricName name);

  void shutdown();
}
