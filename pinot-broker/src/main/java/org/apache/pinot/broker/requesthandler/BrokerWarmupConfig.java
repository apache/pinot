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

import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;


/// Settings for the broker startup warmup performed before readiness is granted.
///
/// Warmup always runs the same static probe -- `SELECT * FROM "<t>" LIMIT 1` over a set-cover of tables
/// spanning every routable server -- with no query or table overrides to configure.
///
/// The exit condition is a **depth floor OR a budget ceiling**, never a latency guess:
///   - [#budgetMs] is a hard ceiling. Warmup releases the readiness gate when it expires whatever the
///     probe progress, so a slow or unreachable server cannot stall a rolling restart.
///   - [#minIterations] is the depth floor: it declares the broker warm once this many probe queries have
///     completed successfully -- enough invocations to drive the query path's JIT to its top tier. (A
///     latency target was deliberately avoided: a trivial probe reaches low latency after a handful of
///     iterations while the code is still only partially compiled, so latency is a false early-exit signal.)
///
/// [#concurrency] is how many probes are fired at once per round: serial (1, the default) warms the
/// serve path; a higher value additionally warms the concurrency step the first real burst hits.
///
/// Immutable; safe to share across threads.
public record BrokerWarmupConfig(boolean enabled, long budgetMs, int minIterations, int concurrency) {

  /// Upper bound on the probe concurrency. Warmup runs before any real traffic, so a modest cap is plenty;
  /// this only exists so a fat-fingered config value cannot ask for a pathological thread pool (an
  /// OutOfMemoryError creating native threads), which would defeat the whole point of warming up.
  private static final int MAX_CONCURRENCY = 64;

  public static BrokerWarmupConfig from(PinotConfiguration config) {
    return new BrokerWarmupConfig(
        config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_ENABLED,
            Broker.DEFAULT_BROKER_STARTUP_WARMUP_ENABLED),
        config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_BUDGET_MS,
            Broker.DEFAULT_BROKER_STARTUP_WARMUP_BUDGET_MS),
        Math.max(1, config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_MIN_ITERATIONS,
            Broker.DEFAULT_BROKER_STARTUP_WARMUP_MIN_ITERATIONS)),
        Math.min(MAX_CONCURRENCY, Math.max(1, config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_CONCURRENCY,
            Broker.DEFAULT_BROKER_STARTUP_WARMUP_CONCURRENCY))));
  }
}
