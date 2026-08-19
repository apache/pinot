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
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the precedence between the three tiers that can set a MATCH_RECOGNIZE limit, and that a nonsensical value is
/// rejected rather than silently clamped.
///
/// The cluster config tier is not visible here on purpose: `QueryRunner` folds it into the op chain metadata
/// under the query option key, and only when the query did not set that option itself, so from this class's point of
/// view it is indistinguishable from a query option. That is exactly how `maxRowsInWindow` behaves.
public class MatchLimitsTest {
  private static final PlanNode.NodeHint NO_HINT = PlanNode.NodeHint.EMPTY;

  @Test
  public void testDefaultsApplyWhenNothingIsSet() {
    assertEquals(MatchLimits.getMaxRowsInMatch(Map.of(), NO_HINT), MatchLimits.DEFAULT_MAX_ROWS_IN_MATCH);
    assertEquals(MatchLimits.getMaxStepsPerMatchAttempt(Map.of(), NO_HINT),
        MatchLimits.DEFAULT_MAX_STEPS_PER_MATCH_ATTEMPT);
    // The defaults the task pins: a million rows, and sixteen million steps.
    assertEquals(MatchLimits.DEFAULT_MAX_ROWS_IN_MATCH, 1_000_000);
    assertEquals(MatchLimits.DEFAULT_MAX_STEPS_PER_MATCH_ATTEMPT, 16_000_000L);
  }

  @Test
  public void testTheStepDefaultDominatesThePerRowCostOfALinearMatch() {
    // The two defaults have to be mutually consistent: a linear, zero-backtracking match costs a small constant
    // number of transitions per row (3 for PATTERN (A+), 5 for PATTERN ((A|B)+)), so a step budget below that
    // constant times the row budget would reject a partition that the row budget explicitly admits.
    assertTrue(
        MatchLimits.DEFAULT_MAX_STEPS_PER_MATCH_ATTEMPT >= 6L * MatchLimits.DEFAULT_MAX_ROWS_IN_MATCH,
        "The step default (" + MatchLimits.DEFAULT_MAX_STEPS_PER_MATCH_ATTEMPT + ") must dominate the per-row cost "
            + "of a linear match over the largest permitted partition (" + MatchLimits.DEFAULT_MAX_ROWS_IN_MATCH
            + " rows)");
  }

  @Test
  public void testQueryOptionOverridesTheDefault() {
    assertEquals(MatchLimits.getMaxRowsInMatch(Map.of(MatchLimits.MAX_ROWS_IN_MATCH, "42"), NO_HINT), 42);
    assertEquals(
        MatchLimits.getMaxStepsPerMatchAttempt(Map.of(MatchLimits.MAX_STEPS_PER_MATCH_ATTEMPT, "77"), NO_HINT), 77L);
  }

  @Test
  public void testHintOverridesTheQueryOption() {
    PlanNode.NodeHint hint = hint(Map.of(MatchLimits.MAX_ROWS_IN_MATCH_HINT, "7",
        MatchLimits.MAX_STEPS_PER_MATCH_ATTEMPT_HINT, "9"));
    Map<String, String> queryOptions = Map.of(MatchLimits.MAX_ROWS_IN_MATCH, "1000",
        MatchLimits.MAX_STEPS_PER_MATCH_ATTEMPT, "1000");
    assertEquals(MatchLimits.getMaxRowsInMatch(queryOptions, hint), 7);
    assertEquals(MatchLimits.getMaxStepsPerMatchAttempt(queryOptions, hint), 9L);
  }

  @Test
  public void testHintOfADifferentLimitDoesNotShadowTheQueryOption() {
    // A hint that only pins the row limit must leave the step limit resolving through the query option.
    PlanNode.NodeHint hint = hint(Map.of(MatchLimits.MAX_ROWS_IN_MATCH_HINT, "7"));
    Map<String, String> queryOptions = Map.of(MatchLimits.MAX_STEPS_PER_MATCH_ATTEMPT, "1000");
    assertEquals(MatchLimits.getMaxRowsInMatch(queryOptions, hint), 7);
    assertEquals(MatchLimits.getMaxStepsPerMatchAttempt(queryOptions, hint), 1000L);
  }

  @Test
  public void testHintsOfOtherOperatorsAreIgnored() {
    PlanNode.NodeHint windowHint = new PlanNode.NodeHint(Map.of("windowOptions", Map.of("max_rows_in_window", "3")));
    assertEquals(MatchLimits.getMaxRowsInMatch(Map.of(), windowHint), MatchLimits.DEFAULT_MAX_ROWS_IN_MATCH);
  }

  @Test
  public void testNonPositiveAndUnparseableValuesAreRejected() {
    // Silently falling back to the default would hide a typo behind a limit the user did not ask for.
    for (String bad : new String[]{"0", "-1", "abc", ""}) {
      IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
          () -> MatchLimits.getMaxRowsInMatch(Map.of(MatchLimits.MAX_ROWS_IN_MATCH, bad), NO_HINT));
      assertTrue(exception.getMessage().contains(MatchLimits.MAX_ROWS_IN_MATCH), exception.getMessage());
    }
  }

  @Test
  public void testMaxRowsInMatchDoesNotOverflowAnInt() {
    // The value is used as an int row count; a long that does not fit must be an error rather than a wrapped value.
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> MatchLimits
        .getMaxRowsInMatch(Map.of(MatchLimits.MAX_ROWS_IN_MATCH, Long.toString(Integer.MAX_VALUE + 1L)), NO_HINT));
    assertTrue(exception.getMessage().contains(MatchLimits.MAX_ROWS_IN_MATCH), exception.getMessage());
    // The step budget is a long, so the same value is perfectly legal there.
    assertEquals(MatchLimits.getMaxStepsPerMatchAttempt(
        Map.of(MatchLimits.MAX_STEPS_PER_MATCH_ATTEMPT, Long.toString(Integer.MAX_VALUE + 1L)), NO_HINT),
        Integer.MAX_VALUE + 1L);
  }

  private static PlanNode.NodeHint hint(Map<String, String> matchOptions) {
    return new PlanNode.NodeHint(Map.of(MatchLimits.MATCH_HINT_OPTIONS, matchOptions));
  }
}
