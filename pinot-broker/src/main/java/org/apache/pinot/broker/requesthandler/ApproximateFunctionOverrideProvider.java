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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.aggregation.function.DistinctCountSmartHLLAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.PercentileSmartTDigestAggregationFunction;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Resolves the defaults for the approximate function rewrite, from the broker conf and from the Helix cluster config.
/// The cluster config wins where it sets a key, and it is applied live: registering this as a
/// [PinotClusterConfigChangeListener] means a change takes effect on the next query without a broker restart.
///
/// Thread-safe. [#getSettings()] hands out one immutable snapshot, so a query that reads it once cannot see a config
/// change half applied.
public class ApproximateFunctionOverrideProvider implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApproximateFunctionOverrideProvider.class);

  private static final ExpressionContext PROBE_COLUMN = ExpressionContext.forIdentifier("__probe__");
  private static final ExpressionContext PROBE_PERCENTILE = ExpressionContext.forLiteral(Literal.doubleValue(50.0));

  private final Settings _brokerConfSettings;
  private volatile Settings _settings;

  public ApproximateFunctionOverrideProvider(PinotConfiguration config) {
    _brokerConfSettings = new Settings(
        config.getProperty(Broker.USE_APPROXIMATE_FUNCTION, Broker.DEFAULT_USE_APPROXIMATE_FUNCTION),
        validated(config.getProperty(Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS,
            Broker.DEFAULT_APPROXIMATE_FUNCTION_PARAMS), Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS,
            Broker.DEFAULT_APPROXIMATE_FUNCTION_PARAMS, ApproximateFunctionOverrideProvider::probeDistinctCount),
        validated(config.getProperty(Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS,
            Broker.DEFAULT_APPROXIMATE_FUNCTION_PARAMS), Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS,
            Broker.DEFAULT_APPROXIMATE_FUNCTION_PARAMS, ApproximateFunctionOverrideProvider::probePercentile));
    _settings = _brokerConfSettings;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(Broker.USE_APPROXIMATE_FUNCTION)
        && !changedConfigs.contains(Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS)
        && !changedConfigs.contains(Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS)) {
      return;
    }
    // A key the cluster config does not set, or that was removed from it, falls back to the broker conf value. The
    // fallback is the broker conf rather than the last good live value, so a broker that restarts resolves the same.
    String enabled = clusterConfigs.get(Broker.USE_APPROXIMATE_FUNCTION);
    Settings updated = new Settings(
        enabled != null ? Boolean.parseBoolean(enabled) : _brokerConfSettings._enabled,
        validated(clusterConfigs.get(Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS),
            Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS, _brokerConfSettings._distinctCountParams,
            ApproximateFunctionOverrideProvider::probeDistinctCount),
        validated(clusterConfigs.get(Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS),
            Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS, _brokerConfSettings._percentileParams,
            ApproximateFunctionOverrideProvider::probePercentile));
    _settings = updated;
    LOGGER.info("Updated approximate function override: enabled: {}, distinctCountParams: '{}', percentileParams: '{}'",
        updated._enabled, updated._distinctCountParams, updated._percentileParams);
  }

  /// Returns the current defaults. Read it once per query, so that the enabled flag and the parameters a query uses
  /// come from the same version of the config.
  public Settings getSettings() {
    return _settings;
  }

  /// Parses the value with the aggregation function itself, so an unknown key or a non-numeric value is caught here
  /// instead of failing every rewritten query on the servers. A value the parser accepts but the sketch rejects, such
  /// as an out-of-range `log2m`, still fails on the servers.
  ///
  /// @param params the configured value, or `null` if unset
  /// @param fallback the value to use if `params` is unset or invalid
  private static String validated(@Nullable String params, String key, String fallback, Consumer<String> probe) {
    if (params == null) {
      return fallback;
    }
    if (params.isEmpty()) {
      return params;
    }
    try {
      probe.accept(params);
      return params;
    } catch (Exception e) {
      LOGGER.error("Ignoring invalid value '{}' for {}, falling back to '{}'", params, key, fallback, e);
      return fallback;
    }
  }

  private static void probeDistinctCount(String params) {
    new DistinctCountSmartHLLAggregationFunction(
        List.of(PROBE_COLUMN, ExpressionContext.forLiteral(Literal.stringValue(params))), false);
  }

  private static void probePercentile(String params) {
    new PercentileSmartTDigestAggregationFunction(
        List.of(PROBE_COLUMN, PROBE_PERCENTILE, ExpressionContext.forLiteral(Literal.stringValue(params))), false);
  }

  /// The resolved defaults. Fields are read directly by the request handlers in this package.
  public static class Settings {
    final boolean _enabled;
    final String _distinctCountParams;
    final String _percentileParams;

    Settings(boolean enabled, String distinctCountParams, String percentileParams) {
      _enabled = enabled;
      _distinctCountParams = distinctCountParams;
      _percentileParams = percentileParams;
    }

    /// Applies the precedence query option > table config > cluster config > broker conf. `null` means unset.
    boolean isEnabled(@Nullable Boolean queryOptionOverride, @Nullable Boolean tableConfigOverride) {
      if (queryOptionOverride != null) {
        return queryOptionOverride;
      }
      return tableConfigOverride != null ? tableConfigOverride : _enabled;
    }
  }
}
