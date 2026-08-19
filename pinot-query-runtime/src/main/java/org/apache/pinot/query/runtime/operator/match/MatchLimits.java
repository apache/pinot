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
import org.apache.pinot.query.planner.plannode.PlanNode;


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
/// 2. the query option, e.g. `SET maxRowsInMatch = 50000`;
/// 3. the server cluster config, e.g. `pinot.query.match.max.rows`, which `QueryRunner` folds into the
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
  /// Hint key for [#MAX_ROWS_IN_MATCH].
  public static final String MAX_ROWS_IN_MATCH_HINT = "max_rows_in_match";
  /// Hint key for [#MAX_STEPS_PER_MATCH_ATTEMPT].
  public static final String MAX_STEPS_PER_MATCH_ATTEMPT_HINT = "max_steps_per_match_attempt";

  /// Query option capping the number of rows buffered for a single MATCH_RECOGNIZE partition. A match cannot span
  /// partitions, so this also caps the number of rows in one match.
  public static final String MAX_ROWS_IN_MATCH = "maxRowsInMatch";
  /// Query option capping the number of automaton transitions explored while looking for a match that starts at one
  /// particular row. It bounds the backtracking of an ambiguous pattern such as `(A|B)*(A|B)*`.
  public static final String MAX_STEPS_PER_MATCH_ATTEMPT = "maxStepsPerMatchAttempt";

  /// Server config key backing [#MAX_ROWS_IN_MATCH].
  public static final String KEY_OF_MAX_ROWS_IN_MATCH = "pinot.query.match.max.rows";
  /// Server config key backing [#MAX_STEPS_PER_MATCH_ATTEMPT].
  public static final String KEY_OF_MAX_STEPS_PER_MATCH_ATTEMPT = "pinot.query.match.max.steps.per.attempt";

  public static final int DEFAULT_MAX_ROWS_IN_MATCH = 1_000_000;
  /// A linear, non-backtracking match costs a small constant number of automaton transitions per row - measured at 2
  /// for `(A A)+`, 3 for `A+` and 5 for `(A|B)+` - so this budget has to dominate that constant
  /// times [#DEFAULT_MAX_ROWS_IN_MATCH], or a partition that the row limit explicitly admits would be rejected
  /// as if its PATTERN were ambiguous. At the previous value of one million steps, `PATTERN (A+)` could not span
  /// a partition of more than 333,332 rows even though [#DEFAULT_MAX_ROWS_IN_MATCH] sanctions three times that.
  /// The factor of 16 leaves headroom for realistic linear patterns while still capping a catastrophically
  /// backtracking one well below a query timeout: 16M transitions is a fraction of a second of matcher work, so
  /// cancellation latency is unchanged in practice.
  public static final long DEFAULT_MAX_STEPS_PER_MATCH_ATTEMPT = 16L * DEFAULT_MAX_ROWS_IN_MATCH;

  public static int getMaxRowsInMatch(Map<String, String> opChainMetadata, PlanNode.NodeHint nodeHint) {
    String value = resolve(opChainMetadata, nodeHint, MAX_ROWS_IN_MATCH_HINT, MAX_ROWS_IN_MATCH);
    return value != null ? parsePositiveInt(MAX_ROWS_IN_MATCH, value) : DEFAULT_MAX_ROWS_IN_MATCH;
  }

  public static long getMaxStepsPerMatchAttempt(Map<String, String> opChainMetadata, PlanNode.NodeHint nodeHint) {
    String value =
        resolve(opChainMetadata, nodeHint, MAX_STEPS_PER_MATCH_ATTEMPT_HINT, MAX_STEPS_PER_MATCH_ATTEMPT);
    return value != null ? parsePositiveLong(MAX_STEPS_PER_MATCH_ATTEMPT, value)
        : DEFAULT_MAX_STEPS_PER_MATCH_ATTEMPT;
  }

  @Nullable
  private static String resolve(Map<String, String> opChainMetadata, PlanNode.NodeHint nodeHint, String hintKey,
      String queryOptionKey) {
    Map<String, String> matchOptions = nodeHint.getHintOptions().get(MATCH_HINT_OPTIONS);
    if (matchOptions != null) {
      String hintValue = matchOptions.get(hintKey);
      if (hintValue != null) {
        return hintValue;
      }
    }
    return opChainMetadata.get(queryOptionKey);
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
