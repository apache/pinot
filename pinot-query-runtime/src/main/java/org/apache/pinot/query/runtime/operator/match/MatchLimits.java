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
package org.apache.pinot.query.runtime.operator.match;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;


/// Resolution of the two MATCH_RECOGNIZE resource limits.
///
/// ## Both limits throw, they never truncate
///
/// Pattern recognition has no meaningful partial answer: dropping a match, or cutting a match short, returns rows that
/// look plausible but are wrong, and nothing in the response says so. Unlike the window operator, which offers a
/// `BREAK` overflow mode, both limits here can only raise an error.
///
/// ## Resolution order
///
/// Highest precedence first, mirroring `maxRowsInWindow`:
/// 1. the per node hint, read from the `matchOptions` hint of the plan node;
/// 2. the query option, e.g. `SET maxRowsInMatchPartition = 50000`;
/// 3. the server cluster config, e.g. `pinot.query.match.max.rows.per.partition`, which `QueryRunner` folds into the
///    op chain metadata under the query option key when the query did not set one;
/// 4. the default declared here.
///
/// The hint tier exists so a planner rule can pin a limit on an individual MATCH_RECOGNIZE node; no SQL syntax
/// attaches such a hint yet, so in practice the query option is the highest tier a user can reach today.
public final class MatchLimits {
  private MatchLimits() {
  }

  /// Hint namespace on the MATCH_RECOGNIZE plan node.
  public static final String MATCH_HINT_OPTIONS = "matchOptions";
  /// Hint key for [QueryOptionKey#MAX_ROWS_IN_MATCH_PARTITION].
  public static final String MAX_ROWS_IN_MATCH_PARTITION_HINT = "max_rows_in_match_partition";
  /// Hint key for [QueryOptionKey#MAX_STEPS_PER_MATCH_ATTEMPT].
  public static final String MAX_STEPS_PER_MATCH_ATTEMPT_HINT = "max_steps_per_match_attempt";

  /// Server config key backing [QueryOptionKey#MAX_ROWS_IN_MATCH_PARTITION].
  public static final String KEY_OF_MAX_ROWS_IN_MATCH_PARTITION = "pinot.query.match.max.rows.per.partition";
  /// Server config key backing [QueryOptionKey#MAX_STEPS_PER_MATCH_ATTEMPT].
  public static final String KEY_OF_MAX_STEPS_PER_MATCH_ATTEMPT = "pinot.query.match.max.steps.per.attempt";

  public static final int DEFAULT_MAX_ROWS_IN_MATCH_PARTITION = 1_000_000;
  /// A linear, non-backtracking match costs a small constant number of automaton transitions per row - measured at 2
  /// for `(A A)+`, 3 for `A+` and 5 for `(A|B)+` - so this budget has to dominate that constant
  /// times [#DEFAULT_MAX_ROWS_IN_MATCH_PARTITION], or a partition that the row limit explicitly admits would be
  /// rejected as if its PATTERN were ambiguous. At the previous value of one million steps, `PATTERN (A+)` could not
  /// span a partition of more than 333,332 rows even though [#DEFAULT_MAX_ROWS_IN_MATCH_PARTITION] sanctions three
  /// times that.
  /// The factor of 16 leaves headroom for realistic linear patterns while still capping a catastrophically
  /// backtracking one well below a query timeout: 16M transitions is a fraction of a second of matcher work, so
  /// cancellation latency is unchanged in practice.
  public static final long DEFAULT_MAX_STEPS_PER_MATCH_ATTEMPT = 16L * DEFAULT_MAX_ROWS_IN_MATCH_PARTITION;

  public static int getMaxRowsInMatchPartition(Map<String, String> opChainMetadata, PlanNode.NodeHint nodeHint) {
    String hintValue = resolveHint(nodeHint, MAX_ROWS_IN_MATCH_PARTITION_HINT);
    if (hintValue != null) {
      return parsePositiveInt(QueryOptionKey.MAX_ROWS_IN_MATCH_PARTITION, hintValue);
    }
    Integer optionValue = QueryOptionsUtils.getMaxRowsInMatchPartition(opChainMetadata);
    return optionValue != null ? optionValue : DEFAULT_MAX_ROWS_IN_MATCH_PARTITION;
  }

  public static long getMaxStepsPerMatchAttempt(Map<String, String> opChainMetadata, PlanNode.NodeHint nodeHint) {
    String hintValue = resolveHint(nodeHint, MAX_STEPS_PER_MATCH_ATTEMPT_HINT);
    if (hintValue != null) {
      return parsePositiveLong(QueryOptionKey.MAX_STEPS_PER_MATCH_ATTEMPT, hintValue);
    }
    Long optionValue = QueryOptionsUtils.getMaxStepsPerMatchAttempt(opChainMetadata);
    return optionValue != null ? optionValue : DEFAULT_MAX_STEPS_PER_MATCH_ATTEMPT;
  }

  @Nullable
  private static String resolveHint(PlanNode.NodeHint nodeHint, String hintKey) {
    Map<String, String> matchOptions = nodeHint.getHintOptions().get(MATCH_HINT_OPTIONS);
    if (matchOptions != null) {
      String hintValue = matchOptions.get(hintKey);
      if (hintValue != null) {
        return hintValue;
      }
    }
    return null;
  }

  private static int parsePositiveInt(String name, String value) {
    return (int) parsePositiveLong(name, value, Integer.MAX_VALUE);
  }

  private static long parsePositiveLong(String name, String value) {
    return parsePositiveLong(name, value, Long.MAX_VALUE);
  }

  private static long parsePositiveLong(String name, String value, long maxValue) {
    long parsed;
    try {
      parsed = Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be a positive integer, got: '" + value + "'");
    }
    if (parsed <= 0 || parsed > maxValue) {
      throw new IllegalArgumentException(
          name + " must be a positive integer no larger than " + maxValue + ", got: " + parsed);
    }
    return parsed;
  }
}
