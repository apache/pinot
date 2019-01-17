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
package org.apache.pinot.broker.requesthandler;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles the setting of QPS related metrics to the metrics registry
 *
 * We want to get as close as possible to the true count of queries we received.
 * Whenever a query is received, increment in _tableQueriesCount
 * Two schedules will be running:
 * 1) Every 10 seconds, for each table, report the queryCount in _tableQueriesCount to _tableMaxQueriesCount - which maintains the max value - and reset the queryCount
 * 2) Every 1 minute, for each table, emit the maxQueryCount, and reset
 */
public class BrokerQPSMetricsHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerQPSMetricsHandler.class);

  // the frequency at which we want to reset the collected query count
  private static final int RESET_QUERY_COUNT_FREQ_SECONDS = 10;
  private static final int RESET_QUERY_COUNT_INITIAL_DELAY_SECONDS = 10;

  // the frequency at which {@link BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE} should be set
  private static final int SET_MAX_QUERIES_PER_10_SEC_GAUGE_FREQ_SECONDS = 60;
  private static final int SET_GAUGE_INITIAL_DELAY_SECONDS = 60;

  private ConcurrentMap<String, LongAccumulator> _tableMaxQueriesCount = new ConcurrentHashMap<>();
  private ConcurrentMap<String, LongAdder> _tableQueriesCount = new ConcurrentHashMap<>();
  private BrokerMetrics _brokerMetrics;

  private ScheduledExecutorService _scheduledExecutorService;

  BrokerQPSMetricsHandler(BrokerMetrics brokerMetrics) {
    _brokerMetrics = brokerMetrics;
  }

  /**
   * Start the executor service and schedule the jobs
   */
  void start() {
    try {
      _scheduledExecutorService = Executors.newScheduledThreadPool(2);

      // kick off 10 second freq schedule, to report query counts
      _scheduledExecutorService.scheduleAtFixedRate(this::reportQueryCount, RESET_QUERY_COUNT_INITIAL_DELAY_SECONDS,
          RESET_QUERY_COUNT_FREQ_SECONDS, TimeUnit.SECONDS);

      // kick off 1 minute freq schedule to set gauge
      _scheduledExecutorService.scheduleAtFixedRate(this::setMaxQueriesPer10SecGauge, SET_GAUGE_INITIAL_DELAY_SECONDS,
          SET_MAX_QUERIES_PER_10_SEC_GAUGE_FREQ_SECONDS, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOGGER.error("Caught exception in starting BrokerQPSMetricsHandler", e);
    }
  }

  /**
   * Increments the query count for the given table
   * @param rawTableName
   */
  void incrementQueryCount(String rawTableName) {
    _tableQueriesCount.computeIfAbsent(rawTableName, k -> new LongAdder()).increment();
  }

  /**
   * Report query count from _tableQueriesCount for each table to _tableMaxQueriesCount and then reset the query count
   */
  @VisibleForTesting
  void reportQueryCount() {
    _tableQueriesCount.forEach(
        (table, queriesCount) -> _tableMaxQueriesCount.computeIfAbsent(table, k -> new LongAccumulator(Long::max, 0))
            .accumulate(queriesCount.sumThenReset()));
  }

  /**
   * Set max queries value to gauge {BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE} for each table in _tableMaxQueriesCount,
   * and then reset max value
   */
  @VisibleForTesting
  void setMaxQueriesPer10SecGauge() {
    _tableMaxQueriesCount.forEach((table, maxQueriesCount) -> _brokerMetrics.setValueOfTableGauge(table,
        BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE, maxQueriesCount.getThenReset()));
  }

  /**
   * Stop the executor service
   */
  void stop() {
    try {
      if (_scheduledExecutorService != null) {
        _scheduledExecutorService.shutdown();
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception in stopping BrokerQPSMetricsHandler", e);
    }
  }
}
