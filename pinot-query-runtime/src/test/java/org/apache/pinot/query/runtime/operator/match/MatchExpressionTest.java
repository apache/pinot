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

import java.math.BigDecimal;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.runtime.operator.operands.ReferenceOperand;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Focused correctness and allocation regression tests for match-specific expression terms.
public class MatchExpressionTest {
  private static final int UNIVERSAL_SYMBOL_ORDINAL = RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL;
  private static final List<PatternSymbol> SYMBOLS = List.of(new PatternSymbol("A", null));
  private static final List<PatternSymbol> A_B_SYMBOLS =
      List.of(new PatternSymbol("A", null), new PatternSymbol("B", null));

  @Test
  public void testNavigationOffsetMustBeAnExactNonNegativeIntegerInRange() {
    assertNavigationCompileFails(new RexExpression.Literal(ColumnDataType.DOUBLE, 1.5), "exact integer");
    assertNavigationCompileFails(new RexExpression.Literal(ColumnDataType.LONG, 1L << 32),
        "between 0 and " + Integer.MAX_VALUE);
    assertNavigationCompileFails(new RexExpression.Literal(ColumnDataType.INT, -1), "must not be negative");
  }

  @Test
  public void testNestedNavigationOffsetOverflowIsRejected() {
    RexExpression inner = navigation("NEXT", patternRef(),
        new RexExpression.Literal(ColumnDataType.INT, Integer.MAX_VALUE), ColumnDataType.INT);
    RexExpression outer = navigation("NEXT", inner, new RexExpression.Literal(ColumnDataType.INT, 1),
        ColumnDataType.INT);

    QueryException exception = expectThrows(QueryException.class,
        () -> MatchExpression.compile(outer, schema(ColumnDataType.INT)));

    assertTrue(exception.getMessage().contains("combined PREV / NEXT offset"), exception.getMessage());
  }

  @Test
  public void testNavigationDoesNotReboxValuesAlreadyInTheDeclaredStoredType() {
    Long value = Long.valueOf(1_000);
    MatchExpression expression = MatchExpression.compile(patternRef(), schema(ColumnDataType.LONG));

    Object result = expression.evaluate(tape(List.<Object[]>of(new Object[]{value})));

    assertSame(result, value);
  }

  @Test
  public void testNavigationStillConvertsWhenDeclaredStoredTypeDiffers() {
    MatchExpression expression = MatchExpression.compile(
        navigation("PREV", patternRef(), new RexExpression.Literal(ColumnDataType.INT, 0), ColumnDataType.LONG),
        schema(ColumnDataType.INT));

    Object result = expression.evaluate(tape(List.<Object[]>of(new Object[]{1_000})));

    assertTrue(result instanceof Long, "Expected a LONG stored value, got: " + result.getClass());
    assertEquals(result, 1_000L);
  }

  @Test
  public void testNavigationDefaultsRunningWrapperAndLogicalBounds() {
    MatchTape tape = tape(List.<Object[]>of(new Object[]{10}, new Object[]{20}, new Object[]{30}));

    assertEquals(MatchExpression.compile(navigation("FIRST", patternRef(), ColumnDataType.INT),
        schema(ColumnDataType.INT)).evaluate(tape), 10);
    assertEquals(MatchExpression.compile(navigation("LAST", patternRef(), ColumnDataType.INT),
        schema(ColumnDataType.INT)).evaluate(tape), 30);
    assertEquals(MatchExpression.compile(navigation("PREV", patternRef(), ColumnDataType.INT),
        schema(ColumnDataType.INT)).evaluate(tape), 20);
    assertNull(MatchExpression.compile(navigation("NEXT", patternRef(), ColumnDataType.INT),
        schema(ColumnDataType.INT)).evaluate(tape));

    // This is the nested form produced for PREV(RUNNING LAST(value)): RUNNING does not change the designated row.
    RexExpression runningLast = new RexExpression.FunctionCall(ColumnDataType.INT, "RUNNING",
        List.of(navigation("LAST", patternRef(), ColumnDataType.INT)));
    assertEquals(MatchExpression.compile(navigation("PREV", runningLast, ColumnDataType.INT),
        schema(ColumnDataType.INT)).evaluate(tape), 20);

    // FIRST/LAST offsets are logical positions within the designated variable, not partition-relative positions.
    assertNull(MatchExpression.compile(
        navigation("FIRST", patternRef(), new RexExpression.Literal(ColumnDataType.INT, 3), ColumnDataType.INT),
        schema(ColumnDataType.INT)).evaluate(tape));
  }

  @Test
  public void testNestedLogicalAndPhysicalOffsetsCompose() {
    MatchTape tape = new MatchTape(A_B_SYMBOLS);
    tape.reset(List.<Object[]>of(
        new Object[]{0}, new Object[]{10}, new Object[]{20}, new Object[]{30}, new Object[]{40}), 0, 1);
    tape.push(0);
    tape.push(1);
    tape.push(0);
    tape.push(1);
    tape.push(0);

    RexExpression secondLastA = navigation("LAST", patternRef(), literal(1), ColumnDataType.INT);
    RexExpression secondA = navigation("FIRST", patternRef(), literal(1), ColumnDataType.INT);
    assertEquals(MatchExpression.compile(navigation("PREV", secondLastA, literal(1), ColumnDataType.INT),
        schema(ColumnDataType.INT)).evaluate(tape), 10);
    assertEquals(MatchExpression.compile(navigation("NEXT", secondA, literal(1), ColumnDataType.INT),
        schema(ColumnDataType.INT)).evaluate(tape), 30);
  }

  @Test
  public void testDefinePredicateAcceptsBooleanResults() {
    MatchTape tape = tape(List.<Object[]>of(new Object[]{1}));

    assertTrue(MatchExpression.compile(new RexExpression.Literal(ColumnDataType.BOOLEAN, true),
        schema(ColumnDataType.INT)).test(tape));
    assertFalse(MatchExpression.compile(new RexExpression.Literal(ColumnDataType.BOOLEAN, false),
        schema(ColumnDataType.INT)).test(tape));
    assertFalse(MatchExpression.compile(new RexExpression.Literal(ColumnDataType.BOOLEAN, null),
        schema(ColumnDataType.INT)).test(tape));
  }

  @Test
  public void testClassifierNavigationPreservesTheDesignatedRow() {
    MatchTape tape = new MatchTape(A_B_SYMBOLS);
    tape.reset(List.<Object[]>of(new Object[]{0}, new Object[]{1}, new Object[]{2}), 0, 1);
    tape.push(0);
    tape.push(1);
    tape.push(0);

    assertEquals(classifierNavigation("FIRST", 0).evaluate(tape), "A");
    assertEquals(classifierNavigation("FIRST", 1).evaluate(tape), "B");
    assertEquals(classifierNavigation("LAST", 1).evaluate(tape), "B");
    assertEquals(classifierNavigation("PREV", 1).evaluate(tape), "B");
    // A future partition row exists, but it has not been classified in the current candidate match.
    assertNull(classifierNavigation("NEXT", 1).evaluate(tape));
  }

  @Test
  public void testCountStarDoesNotVisitUniversalRows() {
    MatchTape tape = new MatchTape(SYMBOLS) {
      @Override
      public int rowAt(int symbolOrdinal, int logicalIndex) {
        throw new AssertionError("COUNT(*) must not visit individual rows");
      }
    };
    tape.reset(List.<Object[]>of(new Object[]{1}, new Object[]{2}), 0, 1);
    tape.push(0);
    tape.push(0);
    MatchTerm.Aggregate count =
        new MatchTerm.Aggregate(MatchTerm.Aggregate.Kind.COUNT, UNIVERSAL_SYMBOL_ORDINAL, null, ColumnDataType.LONG);

    assertEquals(count.evaluate(tape), 2L);
  }

  @Test
  public void testUniversalAggregatesTraverseOnlyTheContiguousMatchRange() {
    List<Object[]> partitionRows = List.of(
        new Object[]{100L}, new Object[]{2L}, new Object[]{null}, new Object[]{4L}, new Object[]{200L});
    MatchTape tape = new MatchTape(SYMBOLS);
    tape.reset(partitionRows, 1, 1);
    tape.push(0);
    tape.push(0);
    tape.push(0);

    assertEquals(aggregate(MatchTerm.Aggregate.Kind.COUNT, ColumnDataType.LONG, ColumnDataType.LONG).evaluate(tape),
        2L);
    assertEquals(aggregate(MatchTerm.Aggregate.Kind.SUM, ColumnDataType.LONG, ColumnDataType.LONG).evaluate(tape),
        6L);
    assertEquals(aggregate(MatchTerm.Aggregate.Kind.MIN, ColumnDataType.LONG, ColumnDataType.LONG).evaluate(tape),
        2L);
    assertEquals(aggregate(MatchTerm.Aggregate.Kind.MAX, ColumnDataType.LONG, ColumnDataType.LONG).evaluate(tape),
        4L);
    assertEquals(aggregate(MatchTerm.Aggregate.Kind.AVG, ColumnDataType.LONG, ColumnDataType.LONG).evaluate(tape),
        3L);
  }

  @Test
  public void testIntegralAndFloatingPointAggregates() {
    MatchTerm.Aggregate longSum = aggregate(MatchTerm.Aggregate.Kind.SUM, ColumnDataType.LONG, ColumnDataType.LONG);
    List<Object[]> longRows = List.of(new Object[]{9_007_199_254_740_993L}, new Object[]{2L}, new Object[]{null});
    assertEquals(longSum.evaluate(tape(longRows)), 9_007_199_254_740_995L);

    MatchTerm.Aggregate doubleAverage =
        aggregate(MatchTerm.Aggregate.Kind.AVG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE);
    List<Object[]> doubleRows = List.of(new Object[]{1.25}, new Object[]{2.75});
    assertEquals(doubleAverage.evaluate(tape(doubleRows)), 2.0);
  }

  @Test
  public void testAggregateEmptySetIdentitiesAndStoredResultTypes() {
    MatchTape nullTape = tape(List.<Object[]>of(new Object[]{null}));
    assertEquals(aggregate(MatchTerm.Aggregate.Kind.COUNT, ColumnDataType.LONG, ColumnDataType.LONG)
        .evaluate(nullTape), 0L);
    assertNull(aggregate(MatchTerm.Aggregate.Kind.MIN, ColumnDataType.LONG, ColumnDataType.LONG).evaluate(nullTape));
    assertNull(aggregate(MatchTerm.Aggregate.Kind.SUM, ColumnDataType.LONG, ColumnDataType.LONG).evaluate(nullTape));
    assertNull(aggregate(MatchTerm.Aggregate.Kind.AVG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE)
        .evaluate(nullTape));
    assertNull(aggregate(MatchTerm.Aggregate.Kind.AVG, ColumnDataType.BIG_DECIMAL, ColumnDataType.BIG_DECIMAL)
        .evaluate(nullTape));

    Object intSum = aggregate(MatchTerm.Aggregate.Kind.SUM, ColumnDataType.INT, ColumnDataType.INT)
        .evaluate(tape(List.<Object[]>of(new Object[]{1}, new Object[]{2})));
    assertTrue(intSum instanceof Integer);
    assertEquals(intSum, 3);

    Object floatAverage = aggregate(MatchTerm.Aggregate.Kind.AVG, ColumnDataType.FLOAT, ColumnDataType.FLOAT)
        .evaluate(tape(List.<Object[]>of(new Object[]{1.0F}, new Object[]{3.0F})));
    assertTrue(floatAverage instanceof Float);
    assertEquals(floatAverage, 2.0F);

    Object widenedMin = aggregate(MatchTerm.Aggregate.Kind.MIN, ColumnDataType.INT, ColumnDataType.LONG)
        .evaluate(tape(List.<Object[]>of(new Object[]{2}, new Object[]{1})));
    assertTrue(widenedMin instanceof Long);
    assertEquals(widenedMin, 1L);
  }

  @Test
  public void testBigDecimalSumAndAverageDoNotRoundThroughDouble() {
    BigDecimal first = new BigDecimal("9007199254740993.0000000000000001");
    BigDecimal second = new BigDecimal("9007199254740995.0000000000000003");
    List<Object[]> rows = List.of(new Object[]{first}, new Object[]{second});

    MatchTerm.Aggregate sum =
        aggregate(MatchTerm.Aggregate.Kind.SUM, ColumnDataType.BIG_DECIMAL, ColumnDataType.BIG_DECIMAL);
    MatchTerm.Aggregate average =
        aggregate(MatchTerm.Aggregate.Kind.AVG, ColumnDataType.BIG_DECIMAL, ColumnDataType.BIG_DECIMAL);

    assertEquals(((BigDecimal) sum.evaluate(tape(rows))).compareTo(
        new BigDecimal("18014398509481988.0000000000000004")), 0);
    assertEquals(((BigDecimal) average.evaluate(tape(rows))).compareTo(
        new BigDecimal("9007199254740994.0000000000000002")), 0);
  }

  @Test
  public void testBigDecimalAggregatesWidenIntegralInputsExactly() {
    List<Object[]> rows = List.of(new Object[]{9_007_199_254_740_993L}, new Object[]{1L});
    MatchTerm.Aggregate sum =
        aggregate(MatchTerm.Aggregate.Kind.SUM, ColumnDataType.LONG, ColumnDataType.BIG_DECIMAL);
    MatchTerm.Aggregate average =
        aggregate(MatchTerm.Aggregate.Kind.AVG, ColumnDataType.LONG, ColumnDataType.BIG_DECIMAL);

    assertEquals(sum.evaluate(tape(rows)), new BigDecimal("9007199254740994"));
    assertEquals(average.evaluate(tape(rows)), new BigDecimal("4503599627370497"));
  }

  @Test
  public void testMultiValueAggregateOperandFailsWithActionableError() {
    ReferenceOperand argument = new ReferenceOperand(0, schema(ColumnDataType.LONG_ARRAY));

    QueryException exception = expectThrows(QueryException.class,
        () -> new MatchTerm.Aggregate(MatchTerm.Aggregate.Kind.SUM, UNIVERSAL_SYMBOL_ORDINAL, argument,
            ColumnDataType.LONG));

    assertTrue(exception.getMessage().contains("Multi-value operand type 'LONG_ARRAY'"), exception.getMessage());
    assertTrue(exception.getMessage().contains("Reduce the array to a scalar"), exception.getMessage());
  }

  private static void assertNavigationCompileFails(RexExpression.Literal offset, String expectedMessage) {
    RexExpression expression = navigation("PREV", patternRef(), offset, ColumnDataType.INT);
    QueryException exception = expectThrows(QueryException.class,
        () -> MatchExpression.compile(expression, schema(ColumnDataType.INT)));
    assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
  }

  private static MatchTerm.Aggregate aggregate(MatchTerm.Aggregate.Kind kind, ColumnDataType inputType,
      ColumnDataType resultType) {
    return new MatchTerm.Aggregate(kind, UNIVERSAL_SYMBOL_ORDINAL, new ReferenceOperand(0, schema(inputType)),
        resultType);
  }

  private static MatchTape tape(List<Object[]> rows) {
    MatchTape tape = new MatchTape(SYMBOLS);
    tape.reset(rows, 0, 1);
    for (int i = 0; i < rows.size(); i++) {
      tape.push(0);
    }
    return tape;
  }

  private static DataSchema schema(ColumnDataType type) {
    return new DataSchema(new String[]{"value"}, new ColumnDataType[]{type});
  }

  private static RexExpression patternRef() {
    return new RexExpression.PatternFieldRef(0, 0, "A");
  }

  private static RexExpression.Literal literal(int value) {
    return new RexExpression.Literal(ColumnDataType.INT, value);
  }

  private static RexExpression navigation(String functionName, RexExpression operand, RexExpression offset,
      ColumnDataType resultType) {
    return new RexExpression.FunctionCall(resultType, functionName, List.of(operand, offset));
  }

  private static RexExpression navigation(String functionName, RexExpression operand, ColumnDataType resultType) {
    return new RexExpression.FunctionCall(resultType, functionName, List.of(operand));
  }

  private static MatchExpression classifierNavigation(String functionName, int offset) {
    RexExpression classifier = new RexExpression.FunctionCall(ColumnDataType.STRING, "CLASSIFIER", List.of());
    return MatchExpression.compile(navigation(functionName, classifier,
        new RexExpression.Literal(ColumnDataType.INT, offset), ColumnDataType.STRING), schema(ColumnDataType.INT));
  }
}
