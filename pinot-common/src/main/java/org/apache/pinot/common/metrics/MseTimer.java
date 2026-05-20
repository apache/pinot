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

import java.util.Objects;
import org.apache.pinot.common.Utils;


/**
 * Timers for the multi-stage engine, emitted via {@link MseMetrics} as {@code pinot.mse.*} when
 * the cluster is configured for {@link MseMetricsMode#MSE} or {@link MseMetricsMode#DUAL}.
 *
 * <p>Each entry carries the corresponding {@link ServerTimer} so {@link MseMetricsMode#SERVER} and
 * {@link MseMetricsMode#DUAL} can forward to the existing {@code pinot.server.*} series.
 */
public enum MseTimer implements AbstractMetrics.Timer {
  HASH_JOIN_BUILD_TABLE_CPU_TIME_MS("millis", true, ServerTimer.HASH_JOIN_BUILD_TABLE_CPU_TIME_MS),
  SERIALIZATION_CPU_TIME_MS("millis", true, ServerTimer.MULTI_STAGE_SERIALIZATION_CPU_TIME_MS),
  DESERIALIZATION_CPU_TIME_MS("millis", true, ServerTimer.MULTI_STAGE_DESERIALIZATION_CPU_TIME_MS),
  RECEIVE_DOWNSTREAM_WAIT_CPU_TIME_MS("millis", true, ServerTimer.RECEIVE_DOWNSTREAM_WAIT_CPU_TIME_MS),
  RECEIVE_UPSTREAM_WAIT_CPU_TIME_MS("millis", true, ServerTimer.RECEIVE_UPSTREAM_WAIT_CPU_TIME_MS);

  private final String _timerName;
  private final boolean _global;
  private final ServerTimer _serverTimer;

  MseTimer(String unit, boolean global, ServerTimer serverTimer) {
    _global = global;
    // Every MseTimer must have a ServerTimer counterpart so SERVER and DUAL modes can forward.
    _serverTimer = Objects.requireNonNull(serverTimer, "serverTimer");
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

  /** Existing {@link ServerTimer} this entry forwards to in SERVER / DUAL mode. */
  public ServerTimer getServerTimer() {
    return _serverTimer;
  }
}
