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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.metrics.NoopPinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.DEFAULT_ENABLE_TABLE_LEVEL_METRICS;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.DEFAULT_METRICS_NAME_PREFIX;


/**
 * Broker metrics utility class, which provides facilities to log the execution performance of queries.
 *
 */
public class BrokerMetrics extends AbstractMetrics<BrokerQueryPhase, BrokerMeter, BrokerGauge, BrokerTimer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerMetrics.class);

  private static final BrokerMetrics NOOP = new BrokerMetrics(new NoopPinotMetricsRegistry());
  private static final AtomicReference<BrokerMetrics> BROKER_METRICS_INSTANCE = new AtomicReference<>(NOOP);

  /**
   * register the brokerMetrics onto this class, so that we don't need to pass it down as a parameter
   */
  public static boolean register(BrokerMetrics brokerMetrics) {
    return BROKER_METRICS_INSTANCE.compareAndSet(NOOP, brokerMetrics);
  }

  /**
   * should always call after registration
   */
  public static BrokerMetrics get() {
    return BROKER_METRICS_INSTANCE.get();
  }

  /**
   * Constructs the broker metrics.
   *
   * @param metricsRegistry The metric registry used to register timers and meters.
   */
  public BrokerMetrics(PinotMetricsRegistry metricsRegistry) {
    this(metricsRegistry, DEFAULT_ENABLE_TABLE_LEVEL_METRICS, Collections.emptySet());
  }

  public BrokerMetrics(PinotMetricsRegistry metricsRegistry, boolean isTableLevelMetricsEnabled,
      Collection<String> allowedTables) {
    this(DEFAULT_METRICS_NAME_PREFIX, metricsRegistry, isTableLevelMetricsEnabled, allowedTables);
  }

  public BrokerMetrics(String prefix, PinotMetricsRegistry metricsRegistry, boolean isTableLevelMetricsEnabled,
      Collection<String> allowedTables) {
    super(prefix, metricsRegistry, BrokerMetrics.class, isTableLevelMetricsEnabled, allowedTables);
  }

  @Override
  protected BrokerQueryPhase[] getQueryPhases() {
    return BrokerQueryPhase.values();
  }

  @Override
  protected BrokerMeter[] getMeters() {
    return BrokerMeter.values();
  }

  @Override
  protected BrokerGauge[] getGauges() {
    return BrokerGauge.values();
  }

  // Emits metrics based on BrokerResponse. Currently only emits metrics for exceptions.
  // If a broker response has multiple exceptions, we will emit metrics for all of them.
  // Thus, the sum total of all exceptions is >= total number of queries impacted.
  // Additionally, some parts of code might already be emitting metrics for individual error codes.
  // But that list isn't accurate with a many-to-many relationship (or no metrics) between error codes and metrics.
  // This method ensures we emit metrics for all queries that have exceptions with a one-to-one mapping.
  public void emitBrokerResponseMetrics(BrokerResponse brokerResponse) {
    for (QueryProcessingException exception : brokerResponse.getExceptions()) {
      QueryErrorCode queryErrorCode;
      try {
        queryErrorCode = QueryErrorCode.fromErrorCode(exception.getErrorCode());
      } catch (IllegalArgumentException e) {
        LOGGER.warn("Invalid error code: " + exception.getErrorCode(), e);
        queryErrorCode = QueryErrorCode.UNKNOWN;
      }
      this.addMeteredGlobalValue(BrokerMeter.getQueryErrorMeter(queryErrorCode), 1);
    }
  }
}
