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

import org.apache.pinot.broker.queryquota.HitCounter;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;

/**
 * Handles the setting of QPS related metrics to the metrics registry
 */
public class BrokerPeakQPSMetricsHandler {
  private static final int TIME_RANGE_IN_SECONDS = 60;
  // create 600 buckets of 100milliseconds width each to cover a range of 60secs
  private static final int HIT_COUNTER_BUCKETS = 600;
  private final HitCounter _hitCounter;

  BrokerPeakQPSMetricsHandler(final BrokerMetrics brokerMetrics) {
    _hitCounter = new HitCounter(TIME_RANGE_IN_SECONDS, HIT_COUNTER_BUCKETS);
    brokerMetrics.addCallbackGauge(BrokerGauge.PEAK_QPS_PER_MINUTE.getGaugeName(),
        () -> _hitCounter.getMaxHitCount());
  }

  void incrementQueryCount() {
    _hitCounter.hitAndUpdateLatestTime();
  }
}