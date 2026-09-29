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

import javax.annotation.Nullable;
import org.apache.pinot.common.Utils;


/// Timers for the multi-stage engine, emitted via [MseMetrics] as `pinot.mse.*` when the cluster
/// is configured for [MseMetricsMode#MSE] or [MseMetricsMode#DUAL].
///
/// Each entry optionally carries a [ServerTimer] counterpart. When present,
/// [MseMetricsMode#SERVER] and [MseMetricsMode#DUAL] forward emissions to the existing
/// `pinot.server.*` series. Entries with no counterpart (MSE-native timers added after the
/// migration) are emitted only under MSE / DUAL modes and silently dropped in SERVER mode.
public enum MseTimer implements AbstractMetrics.Timer {
  HASH_JOIN_BUILD_TABLE_CPU_TIME_MS(true, ServerTimer.HASH_JOIN_BUILD_TABLE_CPU_TIME_MS),
  SERIALIZATION_CPU_TIME_MS(true, ServerTimer.MULTI_STAGE_SERIALIZATION_CPU_TIME_MS),
  DESERIALIZATION_CPU_TIME_MS(true, ServerTimer.MULTI_STAGE_DESERIALIZATION_CPU_TIME_MS),
  RECEIVE_DOWNSTREAM_WAIT_CPU_TIME_MS(true, ServerTimer.RECEIVE_DOWNSTREAM_WAIT_CPU_TIME_MS),
  RECEIVE_UPSTREAM_WAIT_CPU_TIME_MS(true, ServerTimer.RECEIVE_UPSTREAM_WAIT_CPU_TIME_MS);

  private final String _timerName;
  private final boolean _global;
  @Nullable
  private final ServerTimer _serverTimer;

  MseTimer(boolean global) {
    this(global, null);
  }

  MseTimer(boolean global, @Nullable ServerTimer serverTimer) {
    _global = global;
    _serverTimer = serverTimer;
    _timerName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getTimerName() {
    return _timerName;
  }

  @Override
  public boolean isGlobal() {
    return _global;
  }

  /// Existing [ServerTimer] this entry forwards to in SERVER / DUAL mode, or `null` for
  /// MSE-native timers with no legacy `pinot.server.*` series.
  @Nullable
  public ServerTimer getServerTimer() {
    return _serverTimer;
  }
}
