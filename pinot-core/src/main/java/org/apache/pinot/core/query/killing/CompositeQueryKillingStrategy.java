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
package org.apache.pinot.core.query.killing;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.spi.query.QueryScanCostContext;


/**
 * Combines multiple {@link QueryKillingStrategy} instances with AND/OR semantics.
 *
 * <p>Strategies are sorted by {@link QueryKillingStrategy#priority()} (lower = checked first).
 * In {@link Mode#ANY} mode, the first strategy that triggers produces the kill report.
 * In {@link Mode#ALL} mode, all strategies must trigger for a kill.</p>
 *
 */
public class CompositeQueryKillingStrategy implements QueryKillingStrategy {

  /** Composition mode for combining strategies. */
  public enum Mode {
    /** Kill if ANY strategy triggers (OR). */
    ANY,
    /** Kill only if ALL strategies trigger (AND). */
    ALL
  }

  private final List<QueryKillingStrategy> _strategies;
  private final Mode _mode;

  public CompositeQueryKillingStrategy(List<QueryKillingStrategy> strategies, Mode mode) {
    _strategies = strategies.stream()
        .sorted(Comparator.comparingInt(QueryKillingStrategy::priority))
        .collect(Collectors.toList());
    _mode = mode;
  }

  @Override
  public boolean shouldTerminate(QueryScanCostContext ctx) {
    if (_mode == Mode.ANY) {
      for (QueryKillingStrategy s : _strategies) {
        if (s.shouldTerminate(ctx)) {
          return true;
        }
      }
      return false;
    } else {
      for (QueryKillingStrategy s : _strategies) {
        if (!s.shouldTerminate(ctx)) {
          return false;
        }
      }
      return !_strategies.isEmpty();
    }
  }

  @Override
  public QueryKillReport buildKillReport(QueryScanCostContext ctx,
      String queryId, String tableName, String configSource) {
    for (QueryKillingStrategy s : _strategies) {
      if (s.shouldTerminate(ctx)) {
        return s.buildKillReport(ctx, queryId, tableName, configSource);
      }
    }
    throw new IllegalStateException("buildKillReport called but no strategy triggered");
  }
}
