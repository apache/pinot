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
package org.apache.pinot.spi.metrics;

import java.util.concurrent.TimeUnit;

/**
 * An object which maintains mean and exponentially-weighted rate in Pinot.
 */
public interface PinotMetered extends PinotMetric {

  Object getMetered();

  /**
   * Returns the meter's rate unit.
   *
   * @return the meter's rate unit
   */
  TimeUnit rateUnit();

  /**
   * Returns the type of events the meter is measuring.
   *
   * @return the meter's event type
   */
  String eventType();

  /**
   * Returns the number of events which have been marked.
   *
   * @return the number of events which have been marked
   */
  long count();

  /**
   * Returns the fifteen-minute exponentially-weighted moving average rate at which events have
   * occurred since the meter was created.
   * <p/>
   * This rate has the same exponential decay factor as the fifteen-minute load average in the
   * {@code top} Unix command.
   *
   * @return the fifteen-minute exponentially-weighted moving average rate at which events have
   *         occurred since the meter was created
   */
  double fifteenMinuteRate();

  /**
   * Returns the five-minute exponentially-weighted moving average rate at which events have
   * occurred since the meter was created.
   * <p/>
   * This rate has the same exponential decay factor as the five-minute load average in the {@code
   * top} Unix command.
   *
   * @return the five-minute exponentially-weighted moving average rate at which events have
   *         occurred since the meter was created
   */
  double fiveMinuteRate();

  /**
   * Returns the mean rate at which events have occurred since the meter was created.
   *
   * @return the mean rate at which events have occurred since the meter was created
   */
  double meanRate();

  /**
   * Returns the one-minute exponentially-weighted moving average rate at which events have
   * occurred since the meter was created.
   * <p/>
   * This rate has the same exponential decay factor as the one-minute load average in the {@code
   * top} Unix command.
   *
   * @return the one-minute exponentially-weighted moving average rate at which events have
   *         occurred since the meter was created
   */
  double oneMinuteRate();
}
