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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.query.type.TypeFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for RexExpressionUtils, focusing on the handleSearch method and null handling.
 */
public class RexExpressionUtilsTest {
  private RexBuilder _rexBuilder;
  private RelDataTypeFactory _typeFactory;

  @BeforeClass
  public void setup() {
    _typeFactory = new TypeFactory();
    _rexBuilder = new RexBuilder(_typeFactory);
  }

  @Test
  public void testHandleSearchInWithNullAsUnknown() {
    // Test: col IN (1, 2, 3) with RexUnknownAs.UNKNOWN
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
  public void testHandleSearchNotInWithNullAsTrue() {
    // Test: col NOT IN (1, 2) OR col IS NULL (when nullAs = TRUE)
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.TRUE, rangeSetBuilder.build()).negate();

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
    // Test: col NOT IN (1, 2) AND col IS NOT NULL (when nullAs = FALSE)
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

    ImmutableRangeSet.Builder<BigDecimal> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(1)));
    rangeSetBuilder.add(Range.singleton(BigDecimal.valueOf(2)));
    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.FALSE, rangeSetBuilder.build()).negate();

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
    // Test: col IN ('a', 'b', 'c') OR col IS NULL (when nullAs = TRUE)
    RexInputRef inputRef = _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);

    ImmutableRangeSet.Builder<String> rangeSetBuilder = ImmutableRangeSet.builder();
    rangeSetBuilder.add(Range.singleton("a"));
    rangeSetBuilder.add(Range.singleton("b"));
    rangeSetBuilder.add(Range.singleton("c"));
    Sarg<String> sarg = Sarg.of(RexUnknownAs.TRUE, rangeSetBuilder.build());

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
}
