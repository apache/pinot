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
package org.apache.pinot.materializedview.rewrite.equivalence;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Regression coverage for the documented M2 split-safety invariant:
///
/// > A rule may declare [AggregationEquivalence#isSplitSafe()] `== true` only if the user-side
/// > aggregation function and the MV-side re-aggregation function produce DataTable rows whose
/// > column data types match in the broker reducer. Otherwise the reducer silently drops the MV
/// > side (or returns wrong types) when SPLIT_REWRITE merges base + MV intermediates.
///
/// The closest-to-runtime check we can perform inside this module (without pulling pinot-core)
/// is: when a rule's user-function name differs from the re-aggregation function name, it MUST
/// declare itself NOT split-safe. The reverse direction (same name → split-safe) is also
/// asserted for [PassthroughEquivalence] so a refactor cannot silently re-introduce schema drift
/// for distributive aggregations.
public class AggregationEquivalenceRegistryTest {

  @Test
  public void testCountToSumIsNotSplitSafe() {
    // The MV stores COUNT, the user requests COUNT, and the re-aggregation function is SUM —
    // hence findRule(COUNT, COUNT) returns the PassthroughEquivalence("COUNT","SUM") rule whose
    // isSplitSafe() must be false (base side returns COUNT intermediates, MV side returns SUM
    // intermediates after re-agg, and mixing them through one reducer would silently undercount).
    AggregationEquivalence rule = AggregationEquivalenceRegistry.findRule("COUNT", "COUNT");
    assertNotNull(rule, "COUNT/COUNT rule must be registered");
    assertFalse(rule.isSplitSafe(),
        "COUNT(user) over a COUNT-storing MV must NOT be split-safe (re-agg is SUM, intermediates diverge)");
  }

  @Test
  public void testDistributivePassthroughIsSplitSafe() {
    for (String fn : new String[] {"SUM", "MIN", "MAX"}) {
      AggregationEquivalence rule = AggregationEquivalenceRegistry.findRule(fn, fn);
      assertNotNull(rule, fn + "->" + fn + " rule must be registered");
      assertTrue(rule.isSplitSafe(), fn + "->" + fn + " must be split-safe (distributive)");
    }
  }

  @Test
  public void testSketchMergeRulesAreSplitSafe() {
    // For sketch families: base side returns the user function's intermediate (the raw sketch
    // bytes from the user agg); MV side stores DISTINCTCOUNTRAW* whose reducer also yields the
    // user function's intermediates after merge.  Both sides therefore match in the reducer.
    String[][] pairs = {
        {"DISTINCTCOUNTHLL", "DISTINCTCOUNTRAWHLL"},
        {"DISTINCTCOUNTHLLPLUS", "DISTINCTCOUNTRAWHLLPLUS"},
        {"DISTINCTCOUNTTHETASKETCH", "DISTINCTCOUNTRAWTHETASKETCH"}
    };
    for (String[] pair : pairs) {
      AggregationEquivalence rule = AggregationEquivalenceRegistry.findRule(pair[0], pair[1]);
      assertNotNull(rule, pair[0] + "->" + pair[1] + " rule must be registered");
      assertTrue(rule.isSplitSafe(),
          pair[0] + "->" + pair[1] + " must be split-safe (sketch merge preserves intermediate type)");
    }
  }

  @Test
  public void testNoUnregisteredFunctionsAreSilentlyMatched() {
    // Catch the regression where an over-broad matches() lets an unknown function fall through
    // and pick up a wrong rewrite rule.
    assertNull(AggregationEquivalenceRegistry.findRule("AVG", "AVG"));
    assertNull(AggregationEquivalenceRegistry.findRule("PERCENTILE", "PERCENTILE"));
    assertNull(AggregationEquivalenceRegistry.findRule("SUM", "AVG"));
  }

  @Test
  public void testMaterializedViewFunctionSupportSurface() {
    // The analyzer relies on this set to reject MV definitions whose aggregations have no
    // re-aggregation rule.  If the registry changes, this test forces an explicit update.
    String[] supported = {"SUM", "MIN", "MAX", "COUNT",
        "DISTINCTCOUNTRAWHLL", "DISTINCTCOUNTRAWHLLPLUS", "DISTINCTCOUNTRAWTHETASKETCH"};
    for (String fn : supported) {
      assertTrue(AggregationEquivalenceRegistry.isMaterializedViewFunctionSupported(fn),
          fn + " must be recognized as a supported MV-side function");
    }
    assertFalse(AggregationEquivalenceRegistry.isMaterializedViewFunctionSupported("AVG"));
    assertFalse(AggregationEquivalenceRegistry.isMaterializedViewFunctionSupported("PERCENTILE"));
  }

  @Test
  public void testPassthroughSplitSafetyTracksFunctionEquality() {
    // Defense in depth: assert the documented PassthroughEquivalence contract directly so a
    // future edit (e.g. a "SUM->LONG_SUM" alias) cannot regress the M2 invariant.
    assertTrue(new PassthroughEquivalence("SUM", "SUM").isSplitSafe());
    assertFalse(new PassthroughEquivalence("COUNT", "SUM").isSplitSafe());
    assertFalse(new PassthroughEquivalence("FOO", "BAR").isSplitSafe());
  }

  @Test
  public void testFindRuleIsSymmetricallyStrict() {
    // findRule rejects (user,mv) pairs that no registered rule supports.  This guards against
    // accidental over-broad matching where a future rule lets COUNT users be served from a SUM-
    // storing MV with no algebraic translation.
    assertNotNull(AggregationEquivalenceRegistry.findRule("COUNT", "COUNT"));
    assertNull(AggregationEquivalenceRegistry.findRule("SUM", "COUNT"));
    assertNull(AggregationEquivalenceRegistry.findRule("COUNT", "SUM"));
  }

  @Test
  public void testRegistryLookupIsCaseInsensitive() {
    assertNotNull(AggregationEquivalenceRegistry.findRule("sum", "sum"));
    assertNotNull(AggregationEquivalenceRegistry.findRule("Count", "Count"));
    assertNotNull(AggregationEquivalenceRegistry.findRule("distinctcounthll", "distinctcountrawhll"));
  }

  @Test
  public void testSupportedMaterializedViewFunctionsReturnsExpectedSet() {
    assertEquals(AggregationEquivalenceRegistry.supportedMaterializedViewFunctions().size(), 7);
  }
}
