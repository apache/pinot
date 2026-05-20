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
 * Meters for the multi-stage engine, emitted via {@link MseMetrics} as {@code pinot.mse.*} when
 * the cluster is configured for {@link MseMetricsMode#MSE} or {@link MseMetricsMode#DUAL}.
 *
 * <p>Each entry carries the corresponding {@link ServerMeter} so {@link MseMetricsMode#SERVER} and
 * {@link MseMetricsMode#DUAL} can forward to the existing {@code pinot.server.*} series.
 */
public enum MseMeter implements AbstractMetrics.Meter {
  QUERIES("queries", true, ServerMeter.MSE_QUERIES),
  OPCHAINS_STARTED("opchains", true, ServerMeter.MSE_OPCHAINS_STARTED),
  OPCHAINS_COMPLETED("opchains", true, ServerMeter.MSE_OPCHAINS_COMPLETED),
  CPU_EXECUTION_TIME_MS("milliseconds", true, ServerMeter.MSE_CPU_EXECUTION_TIME_MS),
  MEMORY_ALLOCATED_BYTES("bytes", true, ServerMeter.MSE_MEMORY_ALLOCATED_BYTES),
  EMITTED_ROWS("rows", true, ServerMeter.MSE_EMITTED_ROWS),
  RUNNER_STARTED_TASKS("tasks", true, ServerMeter.MULTI_STAGE_RUNNER_STARTED_TASKS),
  RUNNER_COMPLETED_TASKS("tasks", true, ServerMeter.MULTI_STAGE_RUNNER_COMPLETED_TASKS),
  SUBMISSION_STARTED_TASKS("tasks", true, ServerMeter.MULTI_STAGE_SUBMISSION_STARTED_TASKS),
  SUBMISSION_COMPLETED_TASKS("tasks", true, ServerMeter.MULTI_STAGE_SUBMISSION_COMPLETED_TASKS),
  HASH_JOIN_TIMES_MAX_ROWS_REACHED("times", true, ServerMeter.HASH_JOIN_TIMES_MAX_ROWS_REACHED),
  WINDOW_TIMES_MAX_ROWS_REACHED("times", true, ServerMeter.WINDOW_TIMES_MAX_ROWS_REACHED),
  IN_MEMORY_MESSAGES("messages", true, ServerMeter.MULTI_STAGE_IN_MEMORY_MESSAGES),
  RAW_MESSAGES("messages", true, ServerMeter.MULTI_STAGE_RAW_MESSAGES),
  RAW_BYTES("bytes", true, ServerMeter.MULTI_STAGE_RAW_BYTES);

  private final String _meterName;
  private final String _unit;
  private final boolean _global;
  private final ServerMeter _serverMeter;

  MseMeter(String unit, boolean global, ServerMeter serverMeter) {
    _unit = unit;
    _global = global;
    // Every MseMeter must have a ServerMeter counterpart so SERVER and DUAL modes can forward.
    _serverMeter = Objects.requireNonNull(serverMeter, "serverMeter");
    _meterName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getMeterName() {
    return _meterName;
  }

  @Override
  public String getUnit() {
    return _unit;
  }

  @Override
  public boolean isGlobal() {
    return _global;
  }

  /** Existing {@link ServerMeter} this entry forwards to in SERVER / DUAL mode. */
  public ServerMeter getServerMeter() {
    return _serverMeter;
  }
}
