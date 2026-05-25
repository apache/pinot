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
package org.apache.pinot.query.validate;

import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link PinotTypeCoercion} — in particular, the rule that prefers casting the non-column-reference operand
 * in TIMESTAMP-vs-BIGINT binary comparisons. This avoids wrapping a column in a per-row CAST when the other side is a
 * literal or constant subexpression, which is both faster on the query path and preserves index applicability.
 */
public class PinotTypeCoercionTest extends QueryEnvironmentTestBase {

  // 1746022135000 ms == 2025-04-30 14:08:55 UTC (used for deterministic literal-vs-TIMESTAMP comparisons).
  private static final long TS_LITERAL_MS = 1746022135000L;
  private static final String TS_LITERAL_RENDERED = "2025-04-30 14:08:55";
  // RelNode digest uses positional ordinals ($N). Column ordinals depend on the test schema in
  // QueryEnvironmentTestBase#getSchemaBuilder, so pin down the ones we assert on. If the schema
  // changes, this constant fails loudly with a clear message rather than letting cascade failures
  // make the assertions inscrutable.
  private static final String TS_TIMESTAMP_ORD = "$8";
  private static final String COL7_ORD = "$6";

  private String explain(String query) {
    try (QueryEnvironment.CompiledQuery compiled = _queryEnvironment.compile("EXPLAIN PLAN FOR " + query)) {
      return compiled.explain(RANDOM_REQUEST_ID_GEN.nextLong(), null).getExplainPlan();
    }
  }

  /**
   * Sanity: the test schema ordinals we hard-code below match the actual columns. If
   * {@code QueryEnvironmentTestBase#getSchemaBuilder} changes column order, this test fails first
   * with a clear message instead of cascading failures across the suite.
   */
  @Test
  public void testSchemaOrdinalsAreStable() {
    String plan = explain("SELECT ts_timestamp, col7 FROM a");
    assertTrue(plan.contains("ts_timestamp=[" + TS_TIMESTAMP_ORD + "]"),
        "Expected ts_timestamp to project from " + TS_TIMESTAMP_ORD + ". Got:\n" + plan);
    assertTrue(plan.contains("col7=[" + COL7_ORD + "]"),
        "Expected col7 to project from " + COL7_ORD + ". Got:\n" + plan);
  }

  /**
   * When a TIMESTAMP column is compared to a BIGINT literal, the cast must land on the literal side so that constant
   * folding produces a TIMESTAMP literal and the column is not wrapped in a per-row CAST.
   */
  @Test
  public void testTimestampColumnVsBigintLiteralKeepsColumnUnwrapped() {
    // Cover all binary comparison operators in both orientations. Calcite renders != as <>.
    String[] sqlOps = {">", "<", ">=", "<=", "=", "!="};
    for (String op : sqlOps) {
      String rendered = "!=".equals(op) ? "<>" : op;

      // Column on the left.
      String q1 = "SELECT ts_timestamp FROM a WHERE ts_timestamp " + op + " " + TS_LITERAL_MS;
      String plan1 = explain(q1);
      assertTrue(plan1.contains(rendered + "(" + TS_TIMESTAMP_ORD + ", " + TS_LITERAL_RENDERED + ")"),
          "Expected column to remain unwrapped and literal cast to TIMESTAMP for query: " + q1 + "\nGot:\n" + plan1);
      assertFalse(plan1.contains("CAST(" + TS_TIMESTAMP_ORD + ")"),
          "TIMESTAMP column should not be wrapped in CAST for query: " + q1 + "\nGot:\n" + plan1);

      // Column on the right.
      String q2 = "SELECT ts_timestamp FROM a WHERE " + TS_LITERAL_MS + " " + op + " ts_timestamp";
      String plan2 = explain(q2);
      assertTrue(plan2.contains(rendered + "(" + TS_LITERAL_RENDERED + ", " + TS_TIMESTAMP_ORD + ")"),
          "Expected literal cast to TIMESTAMP and column unwrapped for query: " + q2 + "\nGot:\n" + plan2);
      assertFalse(plan2.contains("CAST(" + TS_TIMESTAMP_ORD + ")"),
          "TIMESTAMP column should not be wrapped in CAST for query: " + q2 + "\nGot:\n" + plan2);
    }
  }

  /**
   * Symmetric case: when a BIGINT column is compared to a TIMESTAMP literal, the cast must land on the literal side
   * so that constant folding produces a BIGINT literal and the column is not wrapped in a per-row CAST.
   */
  @Test
  public void testBigintColumnVsTimestampLiteralKeepsColumnUnwrapped() {
    String plan =
        explain("SELECT col7 FROM a WHERE col7 < TIMESTAMP '" + TS_LITERAL_RENDERED + "'");
    assertFalse(plan.contains("CAST(" + COL7_ORD + ")"),
        "BIGINT column should not be wrapped in CAST. Got:\n" + plan);
    assertTrue(plan.contains("<(" + COL7_ORD + ", " + TS_LITERAL_MS + ")"),
        "Expected TIMESTAMP literal folded to BIGINT and column unwrapped. Got:\n" + plan);
  }

  /**
   * When a TIMESTAMP column is compared to a non-column TIMESTAMP-typed expression (e.g. {@code NOW() - 1000}, which
   * after binary-arithmetic coercion is BIGINT-typed and then folded back to TIMESTAMP for the comparison), the
   * resulting plan must keep the column unwrapped and constant-fold the right-hand side to a TIMESTAMP literal.
   */
  @Test
  public void testTimestampColumnVsConstantSubexpressionKeepsColumnUnwrapped() {
    // NOW() - 1000 is a constant for the lifetime of the query: arithmetic coercion casts NOW() to BIGINT, the
    // subtraction is BIGINT - INT = BIGINT, and our comparison rule then casts that BIGINT result to TIMESTAMP.
    // After constant folding, the right-hand side is rendered as a single TIMESTAMP literal.
    String plan = explain("SELECT ts_timestamp FROM a WHERE ts_timestamp > NOW() - 1000");
    assertFalse(plan.contains("CAST(" + TS_TIMESTAMP_ORD + ")"),
        "TIMESTAMP column should not be wrapped in CAST when right-hand side is constant. Got:\n" + plan);
    assertTrue(plan.matches("(?s).*>\\(\\" + TS_TIMESTAMP_ORD + ", \\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\).*"),
        "Right-hand side should be constant-folded to a TIMESTAMP literal. Got:\n" + plan);
  }

  /**
   * When a BIGINT column is compared to a non-column TIMESTAMP-typed expression (e.g. {@code NOW()}), the existing
   * behavior of casting the TIMESTAMP side to BIGINT must be preserved: the BIGINT column stays unwrapped, and the
   * TIMESTAMP function is folded to a BIGINT literal.
   */
  @Test
  public void testBigintColumnVsTimestampFunctionKeepsColumnUnwrapped() {
    String plan = explain("SELECT col7 FROM a WHERE col7 < NOW()");
    assertFalse(plan.contains("CAST(" + COL7_ORD + ")"),
        "BIGINT column should not be wrapped in CAST. Got:\n" + plan);
    assertTrue(plan.matches("(?s).*<\\(\\" + COL7_ORD + ", \\d+\\).*"),
        "Right-hand side should be folded to a BIGINT literal. Got:\n" + plan);
  }

  /**
   * When both operands are column references (TIMESTAMP vs BIGINT), neither side wins the "non-column" tie-breaker, so
   * we fall back to the long-standing default of casting the TIMESTAMP side to BIGINT.
   */
  @Test
  public void testBothColumnReferencesFallsBackToCastTimestampToBigint() {
    String plan = explain("SELECT ts_timestamp FROM a WHERE ts_timestamp > col7");
    assertTrue(plan.contains(">(CAST(" + TS_TIMESTAMP_ORD + "):BIGINT"),
        "Expected fallback to cast TIMESTAMP column to BIGINT when both operands are columns. Got:\n" + plan);
  }

  /**
   * TIMESTAMP-column vs TIMESTAMP-function shouldn't trigger any CAST — both sides are already TIMESTAMP, so the
   * coercion rule should not fire and Calcite should compare directly.
   */
  @Test
  public void testTimestampColumnVsTimestampFunctionHasNoCast() {
    String plan = explain("SELECT ts_timestamp FROM a WHERE ts_timestamp > NOW()");
    assertFalse(plan.contains("CAST(" + TS_TIMESTAMP_ORD + ")"),
        "TIMESTAMP column should not be wrapped in CAST when compared to a TIMESTAMP function. Got:\n" + plan);
    assertEquals(plan.lines().filter(line -> line.contains("CAST(")).count(), 0L,
        "No CAST should appear in the plan. Got:\n" + plan);
  }

  /**
   * Regression for the {@code ago()}-style use case: {@code __time > ago('PT5M')} should keep the column unwrapped.
   * {@code ago(String)} is a scalar function that returns {@code long} (rendered as BIGINT in SQL), so the comparison
   * is TIMESTAMP-vs-BIGINT and the new rule applies. Before this rule, the column was wrapped in {@code CAST(.. AS
   * BIGINT)} per row, which made the query significantly slower than the workaround {@code __time > concat(ago(...),
   * '')} that happened to push the cast onto the literal side via the VARCHAR coercion path.
   */
  @Test
  public void testTimestampColumnVsAgoFunctionKeepsColumnUnwrapped() {
    String plan = explain("SELECT ts_timestamp FROM a WHERE ts_timestamp > ago('PT5M')");
    assertFalse(plan.contains("CAST(" + TS_TIMESTAMP_ORD + ")"),
        "TIMESTAMP column should not be wrapped in CAST when compared to ago(...). Got:\n" + plan);
    assertTrue(plan.matches("(?s).*>\\(\\" + TS_TIMESTAMP_ORD + ", \\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\).*"),
        "Right-hand side should be constant-folded to a TIMESTAMP literal. Got:\n" + plan);
  }

  /**
   * Binary-arithmetic coercion (TIMESTAMP +/- BIGINT) is unchanged: the result must be BIGINT (long arithmetic), so the
   * TIMESTAMP side is always cast to BIGINT regardless of which side is a column.
   */
  @Test
  public void testBinaryArithmeticStillCastsTimestampToBigint() {
    String plan = explain("SELECT ts_timestamp FROM a WHERE ts_timestamp + 1000 > 0");
    // Column ts_timestamp is wrapped in CAST(..):BIGINT for the arithmetic, then the result is compared to the BIGINT
    // literal. We do not change arithmetic coercion here.
    assertTrue(plan.contains("CAST(" + TS_TIMESTAMP_ORD + "):BIGINT"),
        "TIMESTAMP column must be cast to BIGINT for binary arithmetic. Got:\n" + plan);
  }
}
