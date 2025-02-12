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
package org.apache.pinot.core.transport.server.routing.stats;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.common.utils.ExponentialMovingAverage;


/**
 *
 *  {@code ServerRoutingStatsEntry} contains the query routing stats for a server. All access to
 *  ServerRoutingStatsEntry should be made through ServerRoutingStatsManager.
 */
public class ServerRoutingStatsEntry {
  String _serverInstanceId;
  private final ReentrantReadWriteLock _serverLock;

  // Fields related to number of in-flight requests.
  private volatile int _numInFlightRequests;
  private final ExponentialMovingAverage _inFlighRequestsEMA;

  // Fields related to latency
  private final ExponentialMovingAverage _latencyMsEMA;

  // Hybrid score exponent.
  private final int _hybridScoreExponent;

  public ServerRoutingStatsEntry(String serverInstanceId, double alphaEMA, long autoDecayWindowMsEMA,
      long warmupDurationMsEMA, double avgInitializationValEMA, int scoreExponent,
      ScheduledExecutorService periodicTaskExecutor) {
    _serverInstanceId = serverInstanceId;
    _serverLock = new ReentrantReadWriteLock();

    _inFlighRequestsEMA =
        new ExponentialMovingAverage(alphaEMA, autoDecayWindowMsEMA, warmupDurationMsEMA, avgInitializationValEMA,
            periodicTaskExecutor);
    _latencyMsEMA =
        new ExponentialMovingAverage(alphaEMA, autoDecayWindowMsEMA, warmupDurationMsEMA, avgInitializationValEMA,
            periodicTaskExecutor);

    _hybridScoreExponent = scoreExponent;
  }

  @JsonIgnore
  public ReentrantReadWriteLock.ReadLock getServerReadLock() {
    return _serverLock.readLock();
  }

  @JsonIgnore
  public ReentrantReadWriteLock.WriteLock getServerWriteLock() {
    return _serverLock.writeLock();
  }

  public Integer getNumInFlightRequests() {
    return _numInFlightRequests;
  }

  public Double getInFlightRequestsEMA() {
    return _inFlighRequestsEMA.getAverage();
  }

  public Double getLatencyEMA() {
    return _latencyMsEMA.getAverage();
  }

  @JsonProperty("hybridScore")
  public double computeHybridScore() {
    double estimatedQSize = _numInFlightRequests + _inFlighRequestsEMA.getAverage();
    return Math.pow(estimatedQSize, _hybridScoreExponent) * _latencyMsEMA.getAverage();
  }

  public void updateNumInFlightRequestsForQuerySubmission() {
    ++_numInFlightRequests;
    _inFlighRequestsEMA.compute(_numInFlightRequests);
  }

  public void updateNumInFlightRequestsForResponseArrival() {
    --_numInFlightRequests;
  }

  public void updateLatency(double latencyMs) {
    _latencyMsEMA.compute(latencyMs);
  }
}
