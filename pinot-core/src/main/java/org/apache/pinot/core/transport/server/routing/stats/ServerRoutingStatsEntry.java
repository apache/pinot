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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.utils.ExponentialMovingAverage;


@ThreadSafe
public class ServerRoutingStatsEntry {
  String _serverInstanceId;

  private AtomicInteger _numInFlightRequests;

  private final ExponentialMovingAverage _emaLatencyMs;
  private final ReentrantReadWriteLock _emaLatencyLock;

  public ServerRoutingStatsEntry(String serverInstanceId) {
    _serverInstanceId = serverInstanceId;
    _numInFlightRequests = new AtomicInteger();

    // TODO(Vivek): Change this based on config.
    _emaLatencyMs = new ExponentialMovingAverage(0.01);
    _emaLatencyLock = new ReentrantReadWriteLock();
  }

  public Integer getNumInFlightRequests() {
    return _numInFlightRequests.get();
  }

  public Integer incrementNumInFlightRequests() {
    return _numInFlightRequests.incrementAndGet();
  }

  public Integer decrementNumInFlightRequests() {
    return _numInFlightRequests.decrementAndGet();
  }

  public double getEmaLatency() {
    _emaLatencyLock.readLock().lock();
    try {
      return _emaLatencyMs.getLatency();
    } finally {
      _emaLatencyLock.readLock().unlock();
    }
  }

  public double recordLatency(double newValue) {
    _emaLatencyLock.writeLock().lock();
    try {
      return _emaLatencyMs.compute(newValue);
    } finally {
      _emaLatencyLock.writeLock().unlock();
    }
  }
}
