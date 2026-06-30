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
package org.apache.pinot.materializedview.analysis.timeexpr;

import org.apache.pinot.common.request.Expression;
import org.apache.pinot.materializedview.analysis.timeexpr.TimeExprValidator.ArithmeticScaling;
import org.apache.pinot.materializedview.analysis.timeexpr.TimeExprValidator.DateTrunc;
import org.apache.pinot.materializedview.analysis.timeexpr.TimeExprValidator.IdentityPassthrough;
import org.apache.pinot.materializedview.analysis.timeexpr.TimeExprValidator.ParseException;
import org.apache.pinot.materializedview.analysis.timeexpr.TimeExprValidator.TimeExpression;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Structural tests for {@link TimeExprValidator#parse}.  Pin both the
/// happy paths (each accepted shape parses to the expected ADT variant with the right
/// literal arguments) and the error paths (each rejected construct surfaces a
/// {@link ParseException} with a message that names the
/// offending construct).  No policy is asserted here — TIMESTAMP-ness, base-column-name
/// matching, and bucket alignment are tested at the consumer (validator / inferer) level.
public class TimeExprParserTest {

  /// Parses a SELECT-list item from the SQL `SELECT <item> FROM t` and returns the
  /// alias-stripped Thrift {@link Expression}.  The SQL helper is the same shape the MV
  /// validator and inferer use in production, so tests exercise the parser against
  /// realistic Calcite output rather than hand-built Expression trees that might drift
  /// from what Pinot's parser actually emits.
  private static Expression parseSelectExpr(String selectListItem) {
    String sql = "SELECT " + selectListItem + " FROM t";
    return CalciteSqlParser.compileToPinotQuery(sql).getSelectList().get(0);
  }

  // ---------------------------------------------------------------------------------------------
  // Identity passthrough
  // ---------------------------------------------------------------------------------------------

  @Test
  public void identityPassthroughParses() {
    TimeExpression expr = TimeExprValidator.parse(parseSelectExpr("ts"));
    assertTrue(expr instanceof IdentityPassthrough);
    assertEquals(expr.baseTimeColumnName(), "ts");
  }

  @Test
  public void identityPassthroughPreservesUserCasing() {
    // The parser does not canonicalise identifier casing — it surfaces what the user
    // wrote so the consumer can compare against the table's case-correct column name.
    TimeExpression expr = TimeExprValidator.parse(parseSelectExpr("DaysSinceEpoch"));
    assertEquals(expr.baseTimeColumnName(), "DaysSinceEpoch");
  }

  // ---------------------------------------------------------------------------------------------
  // DATETRUNC
  // ---------------------------------------------------------------------------------------------

  @Test
  public void dateTruncTwoArgsParses() {
    TimeExpression expr =
        TimeExprValidator.parse(parseSelectExpr("DATETRUNC('DAY', ts)"));
    assertTrue(expr instanceof DateTrunc);
    DateTrunc dt = (DateTrunc) expr;
    assertEquals(dt.baseTimeColumnName(), "ts");
    assertEquals(dt.unit(), "DAY");
  }

  @Test
  public void dateTruncWithDefaultOptionalArgsParses() {
    // Each optional arg, when present, must equal its default.  Three / four / five
    // operand variants all parse so long as the optionals are at their defaults.
    TimeExpression three =
        TimeExprValidator.parse(parseSelectExpr("DATETRUNC('HOUR', ts, 'MILLISECONDS')"));
    assertEquals(((DateTrunc) three).unit(), "HOUR");

    TimeExpression four = TimeExprValidator.parse(
        parseSelectExpr("DATETRUNC('HOUR', ts, 'MILLISECONDS', 'UTC')"));
    assertEquals(((DateTrunc) four).unit(), "HOUR");

    TimeExpression five = TimeExprValidator.parse(
        parseSelectExpr("DATETRUNC('HOUR', ts, 'MILLISECONDS', 'UTC', 'MILLISECONDS')"));
    assertEquals(((DateTrunc) five).unit(), "HOUR");
  }

  @Test
  public void dateTruncPreservesUnitCasing() {
    // The parser surfaces the user's casing so the consumer can echo it in error
    // messages or normalise as needed.  Both lower-case and upper-case forms must reach
    // the consumer unchanged.
    TimeExpression upper =
        TimeExprValidator.parse(parseSelectExpr("DATETRUNC('DAY', ts)"));
    assertEquals(((DateTrunc) upper).unit(), "DAY");
    TimeExpression lower =
        TimeExprValidator.parse(parseSelectExpr("DATETRUNC('day', ts)"));
    assertEquals(((DateTrunc) lower).unit(), "day");
  }

  @Test
  public void dateTruncRejectsNonDefaultInputTimeUnit() {
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("DATETRUNC('DAY', ts, 'SECONDS')")));
    assertTrue(ex.getMessage().contains("inputTimeUnit"), ex.getMessage());
  }

  @Test
  public void dateTruncRejectsNonDefaultTimeZone() {
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(
            parseSelectExpr("DATETRUNC('DAY', ts, 'MILLISECONDS', 'America/Los_Angeles')")));
    assertTrue(ex.getMessage().contains("UTC"), ex.getMessage());
  }

  @Test
  public void dateTruncRejectsNonDefaultOutputTimeUnit() {
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(
            parseSelectExpr("DATETRUNC('DAY', ts, 'MILLISECONDS', 'UTC', 'SECONDS')")));
    assertTrue(ex.getMessage().contains("outputTimeUnit"), ex.getMessage());
  }

  @Test
  public void dateTruncRejectsBadArity() {
    ParseException one = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("DATETRUNC('DAY')")));
    assertTrue(one.getMessage().contains("2 to 5 arguments"), one.getMessage());
  }

  @Test
  public void dateTruncRejectsNonIdentifierSecondArg() {
    // Nested expression as DATETRUNC's column position would mix shape recognition with
    // arithmetic and break the consumer's bucket-alignment check.  Reject up front with
    // a clear message instead of letting the consumer chase a bogus mismatch.
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("DATETRUNC('DAY', ts * 1000)")));
    assertTrue(ex.getMessage().contains("bare base-table time-column identifier"), ex.getMessage());
  }

  @Test
  public void dateTruncRejectsNonStringUnit() {
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("DATETRUNC(86400000, ts)")));
    assertTrue(ex.getMessage().contains("string literal"), ex.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // Arithmetic scaling
  // ---------------------------------------------------------------------------------------------

  @Test
  public void arithmeticScalingFlatParses() {
    TimeExpression expr =
        TimeExprValidator.parse(parseSelectExpr("ts * 86400000"));
    assertTrue(expr instanceof ArithmeticScaling);
    ArithmeticScaling arith =
        (ArithmeticScaling) expr;
    assertEquals(arith.baseTimeColumnName(), "ts");
    assertEquals(arith.scaleFactor(), 86400000L);
  }

  @Test
  public void arithmeticScalingChainedParses() {
    // Chained multiplication folds left-to-right: `ts * 24 * 60 * 60 * 1000` → product
    // 86_400_000.  The parser collapses the entire chain into a single ArithmeticScaling
    // record with the multiplied value, so consumers don't need to walk further.
    TimeExpression expr =
        TimeExprValidator.parse(parseSelectExpr("ts * 24 * 60 * 60 * 1000"));
    ArithmeticScaling arith =
        (ArithmeticScaling) expr;
    assertEquals(arith.baseTimeColumnName(), "ts");
    assertEquals(arith.scaleFactor(), 86400000L);
  }

  @Test
  public void arithmeticScalingLiteralFirstStillParses() {
    // Order of operands is irrelevant — `1000 * ts` is the same shape as `ts * 1000`.
    // The parser uses identifier-vs-literal type inspection rather than positional rules.
    TimeExpression expr =
        TimeExprValidator.parse(parseSelectExpr("1000 * ts"));
    ArithmeticScaling arith =
        (ArithmeticScaling) expr;
    assertEquals(arith.baseTimeColumnName(), "ts");
    assertEquals(arith.scaleFactor(), 1000L);
  }

  @Test
  public void arithmeticScalingRejectsNoIdentifier() {
    // Calcite constant-folds `100 * 200 * 300` to a single literal `6000000` before the
    // expression reaches the parser, so the rejection surfaces as "must be an identifier"
    // (the top-level shape) rather than the chain-walker's "exactly one identifier"
    // message. Either is correct — the literal IS not a recognised shape — and pinning
    // the test to the actual error path documents the constant-folding interaction.
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("100 * 200 * 300")));
    assertTrue(ex.getMessage().contains("must be an identifier")
            || ex.getMessage().contains("exactly one base-table identifier"),
        ex.getMessage());
  }

  @Test
  public void arithmeticScalingRejectsTwoIdentifiers() {
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("ts * other_col")));
    assertTrue(ex.getMessage().contains("exactly one base-table identifier"), ex.getMessage());
  }

  @Test
  public void arithmeticScalingRejectsZeroLiteral() {
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("ts * 0")));
    assertTrue(ex.getMessage().contains("positive"), ex.getMessage());
  }

  @Test
  public void arithmeticScalingRejectsNegativeLiteral() {
    // Negative literal is parsed by Calcite as `-N` (a function call to negate), so the
    // walker hits an unsupported function leaf rather than a Literal.  The error message
    // names the operator so the user can correct it.
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("ts * (-1000)")));
    assertTrue(ex.getMessage().contains("chained multiplication") || ex.getMessage().contains("positive"),
        ex.getMessage());
  }

  @Test
  public void arithmeticScalingRejectsAddition() {
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("ts + 86400000")));
    // `+` is `plus`, not `times`, so the parser treats the top-level call as an
    // unsupported function.  The error message names the shape requirement.
    assertTrue(ex.getMessage().contains("unsupported function") || ex.getMessage().contains("plus"),
        ex.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // Unsupported shapes
  // ---------------------------------------------------------------------------------------------

  @Test
  public void rejectsDateTimeConvert() {
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(
            parseSelectExpr("dateTimeConvert(ts, '1:MILLISECONDS:EPOCH', '1:DAYS:EPOCH', '1:DAYS')")));
    assertTrue(ex.getMessage().contains("unsupported function"), ex.getMessage());
  }

  @Test
  public void rejectsToDateTime() {
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("toDateTime(ts, 'yyyy-MM-dd')")));
    assertTrue(ex.getMessage().contains("unsupported function"), ex.getMessage());
  }

  @Test
  public void rejectsLiteralExpression() {
    // A bare literal is not even a possible time-column shape; the SELECT-list is
    // expected to project an expression.  Pin the rejection so a future refactor can't
    // silently start emitting a constant TIMESTAMP for the MV time column.
    ParseException ex = expectThrows(
        ParseException.class,
        () -> TimeExprValidator.parse(parseSelectExpr("12345")));
    assertTrue(ex.getMessage().contains("must be an identifier"), ex.getMessage());
  }
}
