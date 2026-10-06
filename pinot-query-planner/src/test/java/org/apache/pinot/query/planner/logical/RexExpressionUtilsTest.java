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
package org.apache.pinot.query.planner.logical;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/// Tests for RexExpressionUtils, focusing on the handleSearch method and null handling.
public class RexExpressionUtilsTest {
  private RexBuilder _rexBuilder;
  private RelDataTypeFactory _typeFactory;

  @BeforeClass
  public void setup() {
    _typeFactory = new TypeFactory();
    _rexBuilder = new RexBuilder(_typeFactory);
  }

  @Test
  public void testUuidLiteralRoundTrip() {
    UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    RelBuilder relBuilder = RelBuilder.create(Frameworks.newConfigBuilder().build());

    RexExpression.Literal literal = RexExpressionUtils.fromRexLiteral(_rexBuilder.makeUuidLiteral(uuid));
    Assert.assertEquals(literal.getDataType(), ColumnDataType.UUID);
    Assert.assertEquals(literal.getValue(), new ByteArray(UuidUtils.toBytes(uuid)));

    RexLiteral roundTrip = RexExpressionUtils.toRexLiteral(relBuilder, literal);
    Assert.assertEquals(roundTrip.getTypeName(), SqlTypeName.UUID);
    Assert.assertEquals(roundTrip.getValue(), uuid);
  }

  @Test
  public void testHandleSearchNullLiteralInWithNullAsUnknown() {
    // Test: NULL IN (1, 2, 3) (when nullAs = UNKNOWN)
    RexLiteral literal = _rexBuilder.makeNullLiteral(_typeFactory.createSqlType(SqlTypeName.INTEGER));

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(3)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, rangeSetBuilder.build());

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, literal, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be FALSE
    Assert.assertEquals(result, RexExpression.Literal.FALSE);
  }

  @Test
  public void testHandleSearchNullLiteralInWithNullAsTrue() {
    // Test: NULL IN (1, 2, 3) (when nullAs = TRUE)
    RexLiteral literal = _rexBuilder.makeNullLiteral(_typeFactory.createSqlType(SqlTypeName.INTEGER));

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(3)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.TRUE, rangeSetBuilder.build());

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, literal, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be TRUE
    Assert.assertEquals(result, RexExpression.Literal.TRUE);
  }

  @Test
  public void testHandleSearchNullLiteralInWithNullAsFalse() {
    // Test: NULL IN (1, 2, 3) (when nullAs = FALSE)
    RexLiteral literal = _rexBuilder.makeNullLiteral(_typeFactory.createSqlType(SqlTypeName.INTEGER));

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(3)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.FALSE, rangeSetBuilder.build());

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, literal, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be FALSE
    Assert.assertEquals(result, RexExpression.Literal.FALSE);
  }

  @Test
  public void testHandleSearchInWithNullAsUnknown() {
    // Test: col IN (1, 2, 3) (when nullAs = UNKNOWN)
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(3)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, rangeSetBuilder.build());

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be a simple IN expression without null check
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall funcCall = (RexExpression.FunctionCall) result;
    Assert.assertEquals(funcCall.getFunctionName(), SqlKind.IN.name());
    Assert.assertEquals(funcCall.getFunctionOperands().size(), 4); // col + 3 values
  }

  @Test
  public void testHandleSearchInWithNullAsTrue() {
    // Test: col IN (1, 2, 3) OR col IS NULL (when nullAs = TRUE)
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(3)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.TRUE, rangeSetBuilder.build());

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be: (col IN (1, 2, 3)) OR (col IS NULL)
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall funcCall = (RexExpression.FunctionCall) result;
    Assert.assertEquals(funcCall.getFunctionName(), SqlKind.OR.name());
    Assert.assertEquals(funcCall.getFunctionOperands().size(), 2);

    // First operand should be the IN expression
    RexExpression firstOperand = funcCall.getFunctionOperands().get(0);
    Assert.assertTrue(firstOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) firstOperand).getFunctionName(), SqlKind.IN.name());

    // Second operand should be IS NULL
    RexExpression secondOperand = funcCall.getFunctionOperands().get(1);
    Assert.assertTrue(secondOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) secondOperand).getFunctionName(), SqlKind.IS_NULL.name());
  }

  @Test
  public void testHandleSearchInWithNullAsFalse() {
    // Test: col IN (1, 2) AND col IS NOT NULL (when nullAs = FALSE)
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.FALSE, rangeSetBuilder.build());

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be: (col IN (1, 2)) AND (col IS NOT NULL)
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall funcCall = (RexExpression.FunctionCall) result;
    Assert.assertEquals(funcCall.getFunctionName(), SqlKind.AND.name());
    Assert.assertEquals(funcCall.getFunctionOperands().size(), 2);

    // First operand should be the IN expression
    RexExpression firstOperand = funcCall.getFunctionOperands().get(0);
    Assert.assertTrue(firstOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) firstOperand).getFunctionName(), SqlKind.IN.name());

    // Second operand should be IS NOT NULL
    RexExpression secondOperand = funcCall.getFunctionOperands().get(1);
    Assert.assertTrue(secondOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) secondOperand).getFunctionName(), SqlKind.IS_NOT_NULL.name());
  }

  @Test
  public void testHandleSearchNullLiteralNotInWithNullAsUnknown() {
    // Test: NULL NOT IN (1, 2, 3) (when nullAs = UNKNOWN)
    RexLiteral literal = _rexBuilder.makeNullLiteral(_typeFactory.createSqlType(SqlTypeName.INTEGER));

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(3)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, rangeSetBuilder.build()).negate();

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, literal, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // This should be UNKNOWN, not TRUE, since we cannot evaluate an unknown value, but this tests the current
    // behavior. It shouldn't matter though, because Calcite folds these away before reaching this code
    Assert.assertEquals(result, RexExpression.Literal.TRUE);
  }

  @Test
  public void testHandleSearchNullLiteralNotInWithNullAsTrue() {
    // Test: NULL NOT IN (1, 2, 3) (when nullAs = TRUE)
    RexLiteral literal = _rexBuilder.makeNullLiteral(_typeFactory.createSqlType(SqlTypeName.INTEGER));

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(3)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.FALSE, rangeSetBuilder.build()).negate();

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, literal, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be TRUE
    Assert.assertEquals(result, RexExpression.Literal.TRUE);
  }

  @Test
  public void testHandleSearchNullLiteralNotInWithNullAsFalse() {
    // Test: NULL NOT IN (1, 2, 3) (when nullAs = FALSE)
    RexLiteral literal = _rexBuilder.makeNullLiteral(_typeFactory.createSqlType(SqlTypeName.INTEGER));

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(3)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.TRUE, rangeSetBuilder.build()).negate();

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, literal, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be FALSE
    Assert.assertEquals(result, RexExpression.Literal.FALSE);
  }

  @Test
  public void testHandleSearchNotInWithNullAsTrue() {
    // Test: col NOT IN (1, 2) OR col IS NULL
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.FALSE, rangeSetBuilder.build()).negate();

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be: (col NOT IN (1, 2)) OR (col IS NULL)
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall funcCall = (RexExpression.FunctionCall) result;
    Assert.assertEquals(funcCall.getFunctionName(), SqlKind.OR.name());
    Assert.assertEquals(funcCall.getFunctionOperands().size(), 2);

    // First operand should be the NOT IN expression
    RexExpression firstOperand = funcCall.getFunctionOperands().get(0);
    Assert.assertTrue(firstOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) firstOperand).getFunctionName(), SqlKind.NOT_IN.name());

    // Second operand should be IS NULL
    RexExpression secondOperand = funcCall.getFunctionOperands().get(1);
    Assert.assertTrue(secondOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) secondOperand).getFunctionName(), SqlKind.IS_NULL.name());
  }

  @Test
  public void testHandleSearchNotInWithNullAsFalse() {
    // Test: col NOT IN (1, 2) AND col IS NOT NULL
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.TRUE, rangeSetBuilder.build()).negate();

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be: (col NOT IN (1, 2)) AND (col IS NOT NULL)
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall funcCall = (RexExpression.FunctionCall) result;
    Assert.assertEquals(funcCall.getFunctionName(), SqlKind.AND.name());
    Assert.assertEquals(funcCall.getFunctionOperands().size(), 2);

    // First operand should be the NOT IN expression
    RexExpression firstOperand = funcCall.getFunctionOperands().get(0);
    Assert.assertTrue(firstOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) firstOperand).getFunctionName(), SqlKind.NOT_IN.name());

    // Second operand should be IS NOT NULL
    RexExpression secondOperand = funcCall.getFunctionOperands().get(1);
    Assert.assertTrue(secondOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) secondOperand).getFunctionName(), SqlKind.IS_NOT_NULL.name());
  }

  @Test
  public void testHandleSearchNullLiteralRangeWithNullAsUnknown() {
    // Test: NULL > 10 with RexUnknownAs.UNKNOWN
    RexLiteral literal = _rexBuilder.makeNullLiteral(_typeFactory.createSqlType(SqlTypeName.INTEGER));

    Range<BigDecimal> range = Range.greaterThan(BigDecimal.valueOf(10));
    ImmutableRangeSet<BigDecimal> rangeSet = ImmutableRangeSet.of(range);
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, rangeSet);

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, literal, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be FALSE
    Assert.assertEquals(result, RexExpression.Literal.FALSE);
  }

  @Test
  public void testHandleSearchNullLiteralRangeWithNullAsTrue() {
    // Test: NULL > 10 (when nullAs = TRUE)
    RexLiteral literal = _rexBuilder.makeNullLiteral(_typeFactory.createSqlType(SqlTypeName.INTEGER));

    Range<BigDecimal> range = Range.greaterThan(BigDecimal.valueOf(10));
    ImmutableRangeSet<BigDecimal> rangeSet = ImmutableRangeSet.of(range);
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.TRUE, rangeSet);

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, literal, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be TRUE
    Assert.assertEquals(result, RexExpression.Literal.TRUE);
  }

  @Test
  public void testHandleSearchNullLiteralRangeWithNullAsFalse() {
    // Test: NULL > 10 (when nullAs = FALSE)
    RexLiteral literal = _rexBuilder.makeNullLiteral(_typeFactory.createSqlType(SqlTypeName.INTEGER));

    Range<BigDecimal> range = Range.greaterThan(BigDecimal.valueOf(10));
    ImmutableRangeSet<BigDecimal> rangeSet = ImmutableRangeSet.of(range);
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.FALSE, rangeSet);

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, literal, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be FALSE
    Assert.assertEquals(result, RexExpression.Literal.FALSE);
  }

  @Test
  public void testHandleSearchRangeWithNullAsTrue() {
    // Test: col > 10 OR col IS NULL (when nullAs = TRUE)
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    Range<BigDecimal> range = Range.greaterThan(BigDecimal.valueOf(10));
    ImmutableRangeSet<BigDecimal> rangeSet = ImmutableRangeSet.of(range);
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.TRUE, rangeSet);

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be: (col > 10) OR (col IS NULL)
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall funcCall = (RexExpression.FunctionCall) result;
    Assert.assertEquals(funcCall.getFunctionName(), SqlKind.OR.name());
    Assert.assertEquals(funcCall.getFunctionOperands().size(), 2);

    // First operand should be the range expression (GREATER_THAN)
    RexExpression firstOperand = funcCall.getFunctionOperands().get(0);
    Assert.assertTrue(firstOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) firstOperand).getFunctionName(), SqlKind.GREATER_THAN.name());

    // Second operand should be IS NULL
    RexExpression secondOperand = funcCall.getFunctionOperands().get(1);
    Assert.assertTrue(secondOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) secondOperand).getFunctionName(), SqlKind.IS_NULL.name());
  }

  @Test
  public void testHandleSearchRangeWithNullAsFalse() {
    // Test: col BETWEEN 10 AND 20 AND col IS NOT NULL (when nullAs = FALSE)
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    Range<BigDecimal> range = Range.closed(BigDecimal.valueOf(10), BigDecimal.valueOf(20));
    ImmutableRangeSet<BigDecimal> rangeSet = ImmutableRangeSet.of(range);
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.FALSE, rangeSet);

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be: (col >= 10 AND col <= 20) AND (col IS NOT NULL)
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall funcCall = (RexExpression.FunctionCall) result;
    Assert.assertEquals(funcCall.getFunctionName(), SqlKind.AND.name());
    Assert.assertEquals(funcCall.getFunctionOperands().size(), 2);

    // First operand should be the range expression (another AND with GREATER_THAN_OR_EQUAL and LESS_THAN_OR_EQUAL)
    RexExpression firstOperand = funcCall.getFunctionOperands().get(0);
    Assert.assertTrue(firstOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) firstOperand).getFunctionName(), SqlKind.AND.name());

    // Second operand should be IS NOT NULL
    RexExpression secondOperand = funcCall.getFunctionOperands().get(1);
    Assert.assertTrue(secondOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) secondOperand).getFunctionName(), SqlKind.IS_NOT_NULL.name());
  }

  @Test
  public void testHandleSearchMultipleRangesWithNullAsTrue() {
    // Test: (col < 5 OR col > 20) OR col IS NULL (when nullAs = TRUE)
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.lessThan(BigDecimal.valueOf(5)));
    rangeSetBuilder.add(Range.greaterThan(BigDecimal.valueOf(20)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.TRUE, rangeSetBuilder.build());

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be: ((col < 5) OR (col > 20)) OR (col IS NULL)
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall funcCall = (RexExpression.FunctionCall) result;
    Assert.assertEquals(funcCall.getFunctionName(), SqlKind.OR.name());
    Assert.assertEquals(funcCall.getFunctionOperands().size(), 2);

    // First operand should be an OR of the two ranges
    RexExpression firstOperand = funcCall.getFunctionOperands().get(0);
    Assert.assertTrue(firstOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) firstOperand).getFunctionName(), SqlKind.OR.name());

    // Second operand should be IS NULL
    RexExpression secondOperand = funcCall.getFunctionOperands().get(1);
    Assert.assertTrue(secondOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) secondOperand).getFunctionName(), SqlKind.IS_NULL.name());
  }

  @Test
  public void testHandleSearchWithStringType() {
    // Test: col IN ('a', 'b', 'c') OR col IS NULL
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);

    ImmutableRangeSet.Builder<NlsString> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(new NlsString("a", "UTF-8", SqlCollation.COERCIBLE)));
    rangeSetBuilder.add(Range.singleton(new NlsString("b", "UTF-8", SqlCollation.COERCIBLE)));
    rangeSetBuilder.add(Range.singleton(new NlsString("c", "UTF-8", SqlCollation.COERCIBLE)));
    Sarg<NlsString> sarg = Sarg.of(RexUnknownAs.TRUE, rangeSetBuilder.build());

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.VARCHAR));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be: (col IN ('a', 'b', 'c')) OR (col IS NULL)
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall funcCall = (RexExpression.FunctionCall) result;
    Assert.assertEquals(funcCall.getFunctionName(), SqlKind.OR.name());
    Assert.assertEquals(funcCall.getFunctionOperands().size(), 2);

    // First operand should be the IN expression
    RexExpression firstOperand = funcCall.getFunctionOperands().get(0);
    Assert.assertTrue(firstOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) firstOperand).getFunctionName(), SqlKind.IN.name());

    // Second operand should be IS NULL
    RexExpression secondOperand = funcCall.getFunctionOperands().get(1);
    Assert.assertTrue(secondOperand instanceof RexExpression.FunctionCall);
    Assert.assertEquals(((RexExpression.FunctionCall) secondOperand).getFunctionName(), SqlKind.IS_NULL.name());
  }

  @Test
  public void testHandleSearchLiteralInEvaluation() {
    // Test: 5 IN (1, 2, 3) should evaluate to FALSE
    RexLiteral leftLiteral = _rexBuilder.makeExactLiteral(BigDecimal.valueOf(5));

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(3)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, rangeSetBuilder.build());

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, leftLiteral, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be evaluated to FALSE literal
    Assert.assertTrue(result instanceof RexExpression.Literal);
    Assert.assertEquals(result, RexExpression.Literal.FALSE);
  }

  @Test
  public void testHandleSearchLiteralInMatchEvaluation() {
    // Test: 2 IN (1, 2, 3) should evaluate to TRUE
    RexLiteral leftLiteral = _rexBuilder.makeExactLiteral(BigDecimal.valueOf(2));

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(3)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, rangeSetBuilder.build());

    RexLiteral searchLiteral = _rexBuilder.makeSearchArgumentLiteral(sarg,
        _typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall searchCall = (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, leftLiteral, searchLiteral);

    RexExpression result = RexExpressionUtils.fromRexCall(searchCall);

    // Should be evaluated to TRUE literal
    Assert.assertTrue(result instanceof RexExpression.Literal);
    Assert.assertEquals(result, RexExpression.Literal.TRUE);
  }

  /// `x IN (<20 values>) OR x > 100`: the points become one IN instead of one pair of comparisons each.
  @Test
  public void testHandleSearchPointsAndRange() {
    RexExpression result = convertIntSearch(RexUnknownAs.UNKNOWN,
        withRanges(points(1, 20), Range.greaterThan(BigDecimal.valueOf(100))));
    Assert.assertEquals(result,
        call(SqlKind.OR, in(SqlKind.IN, 1, 20), call(SqlKind.GREATER_THAN, ref(), intLiteral(100))));
  }

  /// `x NOT IN (<20 values>) AND x > 0`: the complement holds the points, so they become one NOT_IN.
  @Test
  public void testHandleSearchComplementedPointsAndRange() {
    ImmutableRangeSet<BigDecimal> rangeSet =
        ImmutableRangeSet.copyOf(withRanges(points(1, 20), Range.atMost(BigDecimal.ZERO)).complement());
    RexExpression result = convertIntSearch(RexUnknownAs.UNKNOWN, rangeSet);
    Assert.assertEquals(result, call(SqlKind.AND, in(SqlKind.NOT_IN, 1, 20),
        call(SqlKind.GREATER_THAN, ref(), intLiteral(0))));
  }

  /// `x BETWEEN 0 AND 1000 AND x NOT IN (<25 values>)`: a complement range on each side becomes one comparison each.
  @Test
  public void testHandleSearchComplementedPointsInBoundedRange() {
    ImmutableRangeSet<BigDecimal> rangeSet = ImmutableRangeSet.copyOf(withRanges(points(1, 25),
        Range.lessThan(BigDecimal.ZERO), Range.greaterThan(BigDecimal.valueOf(1000))).complement());
    RexExpression result = convertIntSearch(RexUnknownAs.UNKNOWN, rangeSet);
    Assert.assertEquals(result, call(SqlKind.AND, in(SqlKind.NOT_IN, 1, 25),
        call(SqlKind.GREATER_THAN_OR_EQUAL, ref(), intLiteral(0)),
        call(SqlKind.LESS_THAN_OR_EQUAL, ref(), intLiteral(1000))));
  }

  /// `(x IN (<20 values>) OR x BETWEEN 100 AND 200) OR x IS NULL`, and the same with `AND x IS NOT NULL`.
  @Test
  public void testHandleSearchPointsAndClosedRangeWithNullCheck() {
    ImmutableRangeSet<BigDecimal> rangeSet =
        withRanges(points(1, 20), Range.closed(BigDecimal.valueOf(100), BigDecimal.valueOf(200)));
    RexExpression ranges = call(SqlKind.OR, in(SqlKind.IN, 1, 20),
        call(SqlKind.AND, call(SqlKind.GREATER_THAN_OR_EQUAL, ref(), intLiteral(100)),
            call(SqlKind.LESS_THAN_OR_EQUAL, ref(), intLiteral(200))));
    Assert.assertEquals(convertIntSearch(RexUnknownAs.TRUE, rangeSet),
        call(SqlKind.OR, ranges, call(SqlKind.IS_NULL, ref())));
    Assert.assertEquals(convertIntSearch(RexUnknownAs.FALSE, rangeSet),
        call(SqlKind.AND, ranges, call(SqlKind.IS_NOT_NULL, ref())));
  }

  /// A few points next to a range keep one pair of comparisons per point, as before.
  @Test
  public void testHandleSearchFewPointsAndRange() {
    RexExpression result = convertIntSearch(RexUnknownAs.UNKNOWN,
        withRanges(points(1, RexExpressionUtils.MIN_POINTS_FOR_IN - 1), Range.greaterThan(BigDecimal.valueOf(100))));
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall or = (RexExpression.FunctionCall) result;
    Assert.assertEquals(or.getFunctionName(), SqlKind.OR.name());
    Assert.assertEquals(or.getFunctionOperands().size(), RexExpressionUtils.MIN_POINTS_FOR_IN);
    Assert.assertEquals(or.getFunctionOperands().get(0), call(SqlKind.AND,
        call(SqlKind.GREATER_THAN_OR_EQUAL, ref(), intLiteral(1)),
        call(SqlKind.LESS_THAN_OR_EQUAL, ref(), intLiteral(1))));
  }

  /// `x BETWEEN 0 AND 10 AND x NOT IN (3, 5)`: no points and 2 complement points. This must not become an IN without
  /// values.
  @Test
  public void testHandleSearchRangesWithoutPoints() {
    RexExpression result = convertIntSearch(RexUnknownAs.UNKNOWN, withRanges(
        Range.closedOpen(BigDecimal.ZERO, BigDecimal.valueOf(3)),
        Range.open(BigDecimal.valueOf(3), BigDecimal.valueOf(5)),
        Range.openClosed(BigDecimal.valueOf(5), BigDecimal.valueOf(10))));
    Assert.assertEquals(result, call(SqlKind.OR,
        call(SqlKind.AND, call(SqlKind.GREATER_THAN_OR_EQUAL, ref(), intLiteral(0)),
            call(SqlKind.LESS_THAN, ref(), intLiteral(3))),
        call(SqlKind.AND, call(SqlKind.GREATER_THAN, ref(), intLiteral(3)),
            call(SqlKind.LESS_THAN, ref(), intLiteral(5))),
        call(SqlKind.AND, call(SqlKind.GREATER_THAN, ref(), intLiteral(5)),
            call(SqlKind.LESS_THAN_OR_EQUAL, ref(), intLiteral(10)))));
  }

  /// BIG_DECIMAL points next to a range stay comparisons, because intermediate stages match IN values with `equals`.
  @Test
  public void testHandleSearchBigDecimalPointsAndRange() {
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.DECIMAL, 38, 2), 0);
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN,
        withRanges(points(1, 25), Range.greaterThan(BigDecimal.valueOf(100))));
    RexLiteral searchLiteral =
        _rexBuilder.makeSearchArgumentLiteral(sarg, _typeFactory.createSqlType(SqlTypeName.DECIMAL, 38, 2));
    RexExpression result = RexExpressionUtils.fromRexCall(
        (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral));
    Assert.assertTrue(result instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall or = (RexExpression.FunctionCall) result;
    Assert.assertEquals(or.getFunctionName(), SqlKind.OR.name());
    Assert.assertEquals(or.getFunctionOperands().size(), 26);
    for (RexExpression operand : or.getFunctionOperands()) {
      Assert.assertNotEquals(((RexExpression.FunctionCall) operand).getFunctionName(), SqlKind.IN.name());
    }
  }

  /// Point ranges for the values `from` to `to`, both included.
  private static ImmutableRangeSet<BigDecimal> points(int from, int to) {
    ImmutableRangeSet.Builder<BigDecimal> builder = ImmutableRangeSet.builder();
    for (int i = from; i <= to; i++) {
      builder.add(Range.singleton(BigDecimal.valueOf(i)));
    }
    return builder.build();
  }

  @SafeVarargs
  private static ImmutableRangeSet<BigDecimal> withRanges(Range<BigDecimal>... ranges) {
    return withRanges(ImmutableRangeSet.of(), ranges);
  }

  @SafeVarargs
  private static ImmutableRangeSet<BigDecimal> withRanges(ImmutableRangeSet<BigDecimal> rangeSet,
      Range<BigDecimal>... ranges) {
    ImmutableRangeSet.Builder<BigDecimal> builder = ImmutableRangeSet.<BigDecimal>builder().addAll(rangeSet);
    for (Range<BigDecimal> range : ranges) {
      builder.add(range);
    }
    return builder.build();
  }

  private static RexExpression in(SqlKind kind, int from, int to) {
    RexExpression[] operands = new RexExpression[to - from + 2];
    operands[0] = ref();
    for (int i = from; i <= to; i++) {
      operands[i - from + 1] = intLiteral(i);
    }
    return call(kind, operands);
  }

  private RexExpression convertIntSearch(RexUnknownAs nullAs, ImmutableRangeSet<BigDecimal> rangeSet) {
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    Sarg<BigDecimal> sarg = Sarg.of(nullAs, rangeSet);
    RexLiteral searchLiteral =
        _rexBuilder.makeSearchArgumentLiteral(sarg, _typeFactory.createSqlType(SqlTypeName.INTEGER));
    return RexExpressionUtils.fromRexCall(
        (RexCall) _rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, inputRef, searchLiteral));
  }

  private static RexExpression ref() {
    return new RexExpression.InputRef(0);
  }

  private static RexExpression intLiteral(int value) {
    return new RexExpression.Literal(ColumnDataType.INT, value);
  }

  private static RexExpression call(SqlKind kind, RexExpression... operands) {
    return new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, kind.name(), List.of(operands));
  }
}
