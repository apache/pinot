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
package org.apache.pinot.common.metrics;

import org.apache.pinot.spi.metrics.PinotMetricName;


/**
 * A helper for accessing metric values from tests in an implementation-agnostic way. Concrete subclasses are provided
 * by each metrics plugin (yammer, dropwizard) and by the in-memory fake registry used by pinot-common's own tests.
 *
 * <p>General usage:
 * <ol>
 *   <li>Build an inspector around a {@code PinotMetricsRegistry}. The inspector wires itself as a listener so it can
 *       observe metric registrations as they happen.</li>
 *   <li>After invoking an {@code AbstractMetrics} mutator that registers a new metric, call {@link #lastMetric()} to
 *       retrieve the {@link PinotMetricName} of the most recently added metric.</li>
 *   <li>Use {@link #getTimerSumMs(PinotMetricName)} / {@link #getMeteredCount(PinotMetricName)} to read values without
 *       knowing the concrete metric type.</li>
 * </ol>
 */
public abstract class MetricsInspector {

  /**
   * @return the {@code PinotMetricName} of the last metric that was added to the registry.
   */
  public abstract PinotMetricName lastMetric();

  /**
   * Total elapsed time recorded on the timer, normalized to milliseconds.
   *
   * @throws IllegalArgumentException if the provided name does not correspond to a timer
   */
  public abstract long getTimerSumMs(PinotMetricName name);

  /**
   * Total count of events recorded on the metered metric (meter or timer).
   *
   * @throws IllegalArgumentException if the provided name does not correspond to a metered metric
   */
  public abstract long getMeteredCount(PinotMetricName name);
}
