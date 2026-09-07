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

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Behavioral coverage for [SketchMergeEquivalence]: the precision-compatibility gate and the
/// generated re-aggregation expression.
public class SketchMergeEquivalenceTest {

  private static Expression id(String column) {
    return RequestUtils.getIdentifierExpression(column);
  }

  private static Expression lit(int value) {
    return RequestUtils.getLiteralExpression(value);
  }

  private static Expression lit(String value) {
    return RequestUtils.getLiteralExpression(value);
  }

  private static List<Expression> ops(Expression... operands) {
    return Arrays.asList(operands);
  }

  private static Expression agg(String functionName, Expression... operands) {
    return RequestUtils.getFunctionExpression(functionName.toLowerCase(), operands);
  }

  private static AggregationEquivalence rule(String userFunction, String mvFunction) {
    AggregationEquivalence rule = AggregationEquivalenceRegistry.findRule(userFunction, mvFunction);
    assertNotNull(rule, userFunction + "->" + mvFunction + " rule must be registered");
    return rule;
  }

  /// A query requesting higher precision than the stored sketch must be rejected (merging cannot
  /// recover discarded precision); equal or lower precision is accepted.
  @Test
  public void testCpcPrecisionGate() {
    AggregationEquivalence cpc = rule("DISTINCTCOUNTCPCSKETCH", "DISTINCTCOUNTRAWCPCSKETCH");
    // MV stored at default lgK=12 (nominalEntries 4096).
    assertFalse(cpc.operandsCompatible(ops(id("c"), lit(16)), ops(id("c"))), "lgK 16 > default 12 must reject");
    assertTrue(cpc.operandsCompatible(ops(id("c"), lit(12)), ops(id("c"))), "lgK 12 == default must accept");
    assertTrue(cpc.operandsCompatible(ops(id("c")), ops(id("c"), lit(16))), "default < stored lgK 16 must accept");
  }

  /// CPC precision is numeric lgK OR a string `nominalEntries=` form; both must normalize to the
  /// same comparable value.
  @Test
  public void testCpcNumericAndStringFormsAreEquivalent() {
    AggregationEquivalence cpc = rule("DISTINCTCOUNTCPCSKETCH", "DISTINCTCOUNTRAWCPCSKETCH");
    assertTrue(cpc.operandsCompatible(ops(id("c"), lit("nominalEntries=4096")), ops(id("c"), lit(12))),
        "nominalEntries=4096 and lgK=12 are the same precision");
    assertFalse(cpc.operandsCompatible(ops(id("c"), lit("nominalEntries=65536")), ops(id("c"), lit(12))),
        "nominalEntries=65536 (lgK 16) > lgK 12 must reject");
  }

  @Test
  public void testThetaPrecisionGate() {
    AggregationEquivalence theta = rule("DISTINCTCOUNTTHETASKETCH", "DISTINCTCOUNTRAWTHETASKETCH");
    // MV stored at the theta aggregation default (nominalEntries 4096).
    assertFalse(theta.operandsCompatible(ops(id("c"), lit("nominalEntries=16384")), ops(id("c"))),
        "16384 > default 4096 must reject");
    assertTrue(theta.operandsCompatible(ops(id("c"), lit("nominalEntries=4096")), ops(id("c"))),
        "4096 == default must accept");
    assertTrue(theta.operandsCompatible(ops(id("c")), ops(id("c"), lit("nominalEntries=16384"))),
        "default < stored 16384 must accept");
  }

  @Test
  public void testTuplePrecisionGate() {
    AggregationEquivalence tuple = rule("DISTINCTCOUNTTUPLESKETCH", "DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH");
    // MV stored at the tuple aggregation default (nominalEntries 2^14 = 16384).
    assertFalse(tuple.operandsCompatible(ops(id("c"), lit("nominalEntries=32768")), ops(id("c"))),
        "32768 > default 16384 must reject");
    assertTrue(tuple.operandsCompatible(ops(id("c"), lit("nominalEntries=16384")), ops(id("c"))),
        "16384 == default must accept");
    assertTrue(tuple.operandsCompatible(ops(id("c"), lit("nominalEntries=4096")), ops(id("c"))),
        "4096 < default 16384 must accept");
  }

  /// The raw-self rules (user wants the merged sketch itself) must apply the same precision gate.
  @Test
  public void testRawSelfPrecisionGate() {
    AggregationEquivalence rawTheta = rule("DISTINCTCOUNTRAWTHETASKETCH", "DISTINCTCOUNTRAWTHETASKETCH");
    assertFalse(rawTheta.operandsCompatible(
        ops(id("c"), lit("nominalEntries=16384")), ops(id("c"), lit("nominalEntries=4096"))));
    assertTrue(rawTheta.operandsCompatible(
        ops(id("c"), lit("nominalEntries=4096")), ops(id("c"), lit("nominalEntries=16384"))));

    AggregationEquivalence rawCpc = rule("DISTINCTCOUNTRAWCPCSKETCH", "DISTINCTCOUNTRAWCPCSKETCH");
    assertFalse(rawCpc.operandsCompatible(ops(id("c"), lit(16)), ops(id("c"), lit(12))));
    assertTrue(rawCpc.operandsCompatible(ops(id("c"), lit(12)), ops(id("c"), lit(16))));
  }

  /// Each family's gate uses its own aggregation default, so the same requested precision can be
  /// accepted for one family and rejected for another. This is what keeps a query mixing families
  /// correct: each aggregation is gated independently by its own rule.
  @Test
  public void testGateIsPerFamily() {
    AggregationEquivalence theta = rule("DISTINCTCOUNTTHETASKETCH", "DISTINCTCOUNTRAWTHETASKETCH");
    AggregationEquivalence tuple = rule("DISTINCTCOUNTTUPLESKETCH", "DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH");
    // nominalEntries=16384 against a default-precision MV: rejected for theta (default 4096),
    // accepted for tuple (default 16384).
    assertFalse(theta.operandsCompatible(ops(id("c"), lit("nominalEntries=16384")), ops(id("c"))));
    assertTrue(tuple.operandsCompatible(ops(id("c"), lit("nominalEntries=16384")), ops(id("c"))));
  }

  /// HLL is not precision-gated (its default log2m is not modeled here); behavior is unchanged.
  @Test
  public void testHllIsNotGated() {
    AggregationEquivalence hll = rule("DISTINCTCOUNTHLL", "DISTINCTCOUNTRAWHLL");
    assertTrue(hll.operandsCompatible(ops(id("c"), lit(20)), ops(id("c"))));
  }

  /// rewrite() must target the MV column, use the re-aggregation function, and preserve trailing
  /// literal parameters.
  @Test
  public void testRewriteResultRules() {
    assertRewrite("DISTINCTCOUNTCPCSKETCH", "DISTINCTCOUNTRAWCPCSKETCH", "distinctcountcpcsketch",
        agg("DISTINCTCOUNTCPCSKETCH", id("users"), lit(12)), "mv_users_cpc", 1);
    assertRewrite("DISTINCTCOUNTTUPLESKETCH", "DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH", "distinctcounttuplesketch",
        agg("DISTINCTCOUNTTUPLESKETCH", id("users")), "mv_users_tuple", 0);
    assertRewrite("SUMVALUESINTEGERSUMTUPLESKETCH", "DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH",
        "sumvaluesintegersumtuplesketch", agg("SUMVALUESINTEGERSUMTUPLESKETCH", id("users")), "mv_users_tuple", 0);
    assertRewrite("AVGVALUEINTEGERSUMTUPLESKETCH", "DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH",
        "avgvalueintegersumtuplesketch", agg("AVGVALUEINTEGERSUMTUPLESKETCH", id("users")), "mv_users_tuple", 0);
  }

  @Test
  public void testRewriteRawSelfRules() {
    assertRewrite("DISTINCTCOUNTRAWTHETASKETCH", "DISTINCTCOUNTRAWTHETASKETCH", "distinctcountrawthetasketch",
        agg("DISTINCTCOUNTRAWTHETASKETCH", id("users")), "mv_users_theta", 0);
    assertRewrite("DISTINCTCOUNTRAWCPCSKETCH", "DISTINCTCOUNTRAWCPCSKETCH", "distinctcountrawcpcsketch",
        agg("DISTINCTCOUNTRAWCPCSKETCH", id("users")), "mv_users_cpc", 0);
    assertRewrite("DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH", "DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH",
        "distinctcountrawintegersumtuplesketch", agg("DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH", id("users")),
        "mv_users_tuple", 0);
  }

  private static void assertRewrite(String userFunction, String mvFunction, String expectedReAggOperator,
      Expression userExpression, String mvColumn, int expectedTrailingLiterals) {
    Expression rewritten = rule(userFunction, mvFunction).rewrite(userExpression, mvColumn);
    assertNotNull(rewritten);
    Function function = rewritten.getFunctionCall();
    assertNotNull(function);
    assertEquals(function.getOperator(), expectedReAggOperator);
    assertEquals(function.getOperands().get(0).getIdentifier().getName(), mvColumn);
    assertEquals(function.getOperands().size(), 1 + expectedTrailingLiterals);
  }
}
