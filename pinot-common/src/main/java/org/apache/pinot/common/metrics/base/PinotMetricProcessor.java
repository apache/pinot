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


public interface PinotMetricProcessor<T> {
  /**
   * Process the given {@link PinotMetered} instance.
   *
   * @param name       the name of the meter
   * @param meter      the meter
   * @param context    the context of the meter
   * @throws Exception if something goes wrong
   */
  void processMeter(PinotMetricName name, PinotMetered meter, T context) throws Exception;

  /**
   * Process the given counter.
   *
   * @param name       the name of the counter
   * @param counter    the counter
   * @param context    the context of the meter
   * @throws Exception if something goes wrong
   */
  void processCounter(PinotMetricName name, PinotCounter counter, T context) throws Exception;

  /**
   * Process the given histogram.
   *
   * @param name       the name of the histogram
   * @param histogram  the histogram
   * @param context    the context of the meter
   * @throws Exception if something goes wrong
   */
  void processHistogram(PinotMetricName name, PinotHistogram histogram, T context) throws Exception;

  /**
   * Process the given timer.
   *
   * @param name       the name of the timer
   * @param timer      the timer
   * @param context    the context of the meter
   * @throws Exception if something goes wrong
   */
  void processTimer(PinotMetricName name, PinotTimer timer, T context) throws Exception;

  /**
   * Process the given gauge.
   *
   * @param name       the name of the gauge
   * @param gauge      the gauge
   * @param context    the context of the meter
   * @throws Exception if something goes wrong
   */
  void processGauge(PinotMetricName name, PinotGauge<?> gauge, T context) throws Exception;
}
