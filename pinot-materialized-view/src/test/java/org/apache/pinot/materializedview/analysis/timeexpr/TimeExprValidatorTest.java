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

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;


public class TimeExprValidatorTest {

  private static final DateTimeFieldSpec TIMESTAMP_FIELD = new DateTimeFieldSpec(
      "ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS");
  private static final DateTimeFieldSpec LONG_EPOCH_DAYS_FIELD = new DateTimeFieldSpec(
      "ts", DataType.LONG, "1:DAYS:EPOCH", "1:DAYS");

  private static Expression parseSelectExpr(String selectListItem) {
    String sql = "SELECT " + selectListItem + " FROM t";
    return CalciteSqlParser.compileToPinotQuery(sql).getSelectList().get(0);
  }

  @Test
  public void testIdentityPassthroughAccepted() {
    TimeExprValidator.validate(
        parseSelectExpr("ts"),
        "ts", TIMESTAMP_FIELD,
        "tsMv", TIMESTAMP_FIELD,
        TimeUnit.MILLISECONDS.toMillis(1));
  }

  @Test
  public void testIdentityRejectedWhenMismatchedBaseColumn() {
    IllegalStateException ex = expectThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("other_col"),
            "ts", TIMESTAMP_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.MILLISECONDS.toMillis(1)));
    String msg = ex.getMessage();
    assert msg.contains("derive from base time column 'ts'") : msg;
  }

  @Test
  public void testDatetruncAcceptedWhenUnitMatchesBucket() {
    TimeExprValidator.validate(
        parseSelectExpr("DATETRUNC('DAY', ts)"),
        "ts", TIMESTAMP_FIELD,
        "tsMv", TIMESTAMP_FIELD,
        TimeUnit.DAYS.toMillis(1));
  }

  @Test
  public void testDatetruncRejectedWhenUnitMismatchesBucket() {
    IllegalStateException ex = expectThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("DATETRUNC('HOUR', ts)"),
            "ts", TIMESTAMP_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
    assert ex.getMessage().contains("does not match the declared bucketTimePeriod");
  }

  @Test
  public void testDatetruncRejectedForCalendarUnit() {
    assertThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("DATETRUNC('WEEK', ts)"),
            "ts", TIMESTAMP_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(7)));
  }

  @Test
  public void testDatetruncRejectedWhenSecondArgIsNotBaseColumn() {
    assertThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("DATETRUNC('DAY', other_col)"),
            "ts", TIMESTAMP_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
  }

  @Test
  public void testRejectsNonTimestampBaseColumn() {
    IllegalStateException ex = expectThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("ts"),
            "ts", LONG_EPOCH_DAYS_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
    assert ex.getMessage().contains("TIMESTAMP");
  }

  @Test
  public void testRejectsNonTimestampViewColumn() {
    assertThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("ts"),
            "ts", TIMESTAMP_FIELD,
            "tsMv", LONG_EPOCH_DAYS_FIELD,
            TimeUnit.MILLISECONDS.toMillis(1)));
  }

  @Test
  public void testRejectsDateTimeConvert() {
    IllegalStateException ex = expectThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("dateTimeConvert(ts, '1:MILLISECONDS:EPOCH', '1:DAYS:EPOCH', '1:DAYS')"),
            "ts", TIMESTAMP_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
    assert ex.getMessage().contains("unsupported function");
  }

  @Test
  public void testRejectsToDateTime() {
    assertThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("toDateTime(ts, 'yyyy-MM-dd')"),
            "ts", TIMESTAMP_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
  }

  @Test
  public void testDatetruncRejectsNonDefaultTimezone() {
    assertThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("DATETRUNC('DAY', ts, 'MILLISECONDS', 'America/Los_Angeles')"),
            "ts", TIMESTAMP_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
  }

  @Test
  public void testArithmeticScalingAcceptedFromNonTimestampBase() {
    // Base is INT-days, MV is TIMESTAMP; SELECT multiplies days by ms-per-day to produce millis.
    TimeExprValidator.validate(
        parseSelectExpr("ts * 86400000"),
        "ts", LONG_EPOCH_DAYS_FIELD,
        "tsMv", TIMESTAMP_FIELD,
        TimeUnit.DAYS.toMillis(1));
  }

  @Test
  public void testArithmeticScalingAcceptedWithChainedMultiplication() {
    // Same as above but the user broke the scale factor into a chain (24*60*60*1000).
    TimeExprValidator.validate(
        parseSelectExpr("ts * 24 * 60 * 60 * 1000"),
        "ts", LONG_EPOCH_DAYS_FIELD,
        "tsMv", TIMESTAMP_FIELD,
        TimeUnit.DAYS.toMillis(1));
  }

  @Test
  public void testArithmeticScalingRejectedWhenBaseColumnNotReferenced() {
    IllegalStateException ex = expectThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("other_col * 86400000"),
            "ts", LONG_EPOCH_DAYS_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
    assert ex.getMessage().contains("base time column 'ts'") : ex.getMessage();
  }

  @Test
  public void testArithmeticScalingRejectedWhenBaseColumnReferencedTwice() {
    assertThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("ts * ts"),
            "ts", LONG_EPOCH_DAYS_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
  }

  @Test
  public void testArithmeticScalingRejectsAddition() {
    assertThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("ts + 86400000"),
            "ts", LONG_EPOCH_DAYS_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
  }

  @Test
  public void testArithmeticScalingRejectsNonPositiveLiteral() {
    assertThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("ts * 0"),
            "ts", LONG_EPOCH_DAYS_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
  }

  @Test
  public void testIdentityRejectedWhenBaseIsNonTimestamp() {
    IllegalStateException ex = expectThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("ts"),
            "ts", LONG_EPOCH_DAYS_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
    assert ex.getMessage().contains("Identity passthrough requires") : ex.getMessage();
  }

  @Test
  public void testDatetruncRejectsNonDefaultInputTimeUnit() {
    assertThrows(IllegalStateException.class, () ->
        TimeExprValidator.validate(
            parseSelectExpr("DATETRUNC('DAY', ts, 'SECONDS')"),
            "ts", TIMESTAMP_FIELD,
            "tsMv", TIMESTAMP_FIELD,
            TimeUnit.DAYS.toMillis(1)));
  }
}
