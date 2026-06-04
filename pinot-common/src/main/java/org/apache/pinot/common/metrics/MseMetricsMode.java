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

/// Selects how [MseMetrics] emits multi-stage engine metrics.
///
/// - [#SERVER] (default): forward to [ServerMetrics] only (existing `pinot.server.*` series).
/// - [#MSE]: emit to a dedicated `pinot.mse.*` registry only.
/// - [#DUAL]: emit to both, for dashboard migration windows.
///
/// Read at startup from cluster config; mode changes require restart.
///
/// **Migration path:** SERVER is the default to preserve `pinot.server.*` dashboards. Operators
/// migrating to the `pinot.mse.*` surface should flip to DUAL for an overlap window, point
/// dashboards/alerts at `pinot.mse.*`, then flip to MSE. SERVER mode (and the
/// [MseMeter#getServerMeter()] / [MseTimer#getServerTimer()] forwarding links) is the
/// backward-compat shim and can be removed once the legacy series has no remaining consumers.
public enum MseMetricsMode {
  SERVER,
  MSE,
  DUAL
}
