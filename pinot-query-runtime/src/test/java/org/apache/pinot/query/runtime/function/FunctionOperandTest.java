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
package org.apache.pinot.query.runtime.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.runtime.operator.TransformOperator;
import org.apache.pinot.query.runtime.operator.operands.FunctionOperand;
import org.apache.pinot.spi.annotations.FunctionVolatility;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Tests lazy, per-operand caching. Fixtures live in a function package for registry discovery; invocation counters
/// are thread-local so parallel test methods do not interfere with each other.
public class FunctionOperandTest {
  private static final DataSchema INPUT_SCHEMA =
      new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final ThreadLocal<int[]> INVOCATIONS = ThreadLocal.withInitial(() -> new int[1]);

  @BeforeMethod
  public void setUp() {
    INVOCATIONS.get()[0] = 0;
  }

  @AfterMethod
  public void tearDown() {
    INVOCATIONS.remove();
  }

  @DataProvider
  public Object[][] cacheableFunctions() {
    return new Object[][]{
        {"operandCacheIdentity", List.of(literal(7)), 7},
        {"operandCacheNonFoldable", List.of(literal(7)), 7},
        {"operandCacheVarargs", List.of(literal(3), literal(4)), 7},
        {"operandCacheZeroArgs", List.of(), 7}
    };
  }

  @Test(dataProvider = "cacheableFunctions")
  public void testLazyCaching(String name, List<RexExpression> arguments, int expected) {
    FunctionOperand operand = operand(name, ColumnDataType.LONG, arguments);
    assertEquals(INVOCATIONS.get()[0], 0, "Construction must not evaluate the function");
    for (int i = 0; i < 3; i++) {
      assertEquals(operand.apply(List.of(i)), (long) expected, "Cache the converted result");
    }
    assertEquals(INVOCATIONS.get()[0], 1);
  }

  @Test
  public void testImmutableNonFoldableMetadata() {
    FunctionInfo info = FunctionRegistry.lookupFunctionInfo("operandcachenonfoldable", 1);
    assertFalse(info.isDeterministic());
    assertEquals(info.getVolatility(), FunctionVolatility.IMMUTABLE);
  }

  @Test
  public void testNullResultIsCached() {
    FunctionOperand operand = operand("operandCacheIdentity", ColumnDataType.INT,
        List.of(new RexExpression.Literal(ColumnDataType.INT, null)));
    assertNull(operand.apply(List.of(1)));
    assertNull(operand.apply(List.of(2)));
    assertEquals(INVOCATIONS.get()[0], 1, "A computed null must not mean uninitialized");
  }

  @DataProvider
  public Object[][] nonCacheableFunctions() {
    return new Object[][]{{"operandCacheStable"}, {"operandCacheVolatile"}};
  }

  @Test(dataProvider = "nonCacheableFunctions")
  public void testNonImmutableFunctionsAreNotCached(String name) {
    FunctionOperand operand = operand(name, ColumnDataType.INT, List.of());
    operand.apply(List.of(1));
    operand.apply(List.of(2));
    assertEquals(INVOCATIONS.get()[0], 2);
  }

  @Test
  public void testColumnArgumentIsNotCached() {
    FunctionOperand operand = operand("operandCacheIdentity", ColumnDataType.INT,
        List.of(new RexExpression.InputRef(0)));
    assertEquals(operand.apply(List.of(3)), 3);
    assertEquals(operand.apply(List.of(4)), 4);
    assertEquals(INVOCATIONS.get()[0], 2);
  }

  @Test
  public void testNestedFunctionIsNotTreatedAsLiteral() {
    RexExpression child = new RexExpression.FunctionCall(ColumnDataType.INT, "operandCacheVolatile", List.of());
    FunctionOperand operand = operand("operandCacheIdentity", ColumnDataType.INT, List.of(child));
    assertEquals(operand.apply(List.of()), 1);
    assertEquals(operand.apply(List.of()), 3);
    assertEquals(INVOCATIONS.get()[0], 4);
  }

  @Test
  public void testArgumentConversion() {
    FunctionOperand operand = operand("operandCacheIdentity", ColumnDataType.INT,
        List.of(new RexExpression.Literal(ColumnDataType.LONG, 7L)));
    assertEquals(operand.apply(List.of()), 7);
    assertEquals(operand.apply(List.of()), 7);
    assertEquals(INVOCATIONS.get()[0], 1);
  }

  @Test
  public void testFailureRemainsLazyAndIsNotCached() {
    FunctionOperand operand = operand("operandCacheIdentity", ColumnDataType.INT, List.of(literal(-1)));
    assertEquals(INVOCATIONS.get()[0], 0);
    for (int i = 0; i < 2; i++) {
      QueryException error = expectThrows(QueryException.class, () -> operand.apply(List.of()));
      assertEquals(error.getCause().getMessage(), "negative value: -1");
    }
    assertEquals(INVOCATIONS.get()[0], 2);
  }

  @Test
  public void testArrayResultAndCacheIsolation() {
    FunctionOperand first = operand("operandCacheArray", ColumnDataType.INT_ARRAY, List.of(literal(7)));
    FunctionOperand second = operand("operandCacheArray", ColumnDataType.INT_ARRAY, List.of(literal(8)));
    Object firstResult = first.apply(List.of());
    Object secondResult = second.apply(List.of());
    assertEquals((int[]) firstResult, new int[]{7});
    assertEquals((int[]) secondResult, new int[]{8});
    assertNotSame(firstResult, secondResult);
    assertSame(first.apply(List.of()), firstResult);
    assertSame(second.apply(List.of()), secondResult);
    assertEquals(INVOCATIONS.get()[0], 2);
  }

  @Test
  public void testCachingAcrossBlocks() {
    MultiStageOperator input = mock(MultiStageOperator.class);
    when(input.nextBlock()).thenReturn(OperatorTestUtil.block(INPUT_SCHEMA),
        OperatorTestUtil.block(INPUT_SCHEMA, new Object[]{1}, new Object[]{2}),
        OperatorTestUtil.block(INPUT_SCHEMA, new Object[]{3}));
    DataSchema resultSchema = new DataSchema(new String[]{"array"}, new ColumnDataType[]{ColumnDataType.INT_ARRAY});
    List<RexExpression> projects = List.of(
        new RexExpression.FunctionCall(ColumnDataType.INT_ARRAY, "operandCacheArray", List.of(literal(7))));
    TransformOperator operator = new TransformOperator(OperatorTestUtil.getTracingContext(), input, INPUT_SCHEMA,
        new ProjectNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), projects));
    assertEquals(INVOCATIONS.get()[0], 0);
    assertEquals(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows().size(), 0);
    assertEquals(INVOCATIONS.get()[0], 0, "Empty input must not evaluate the function");
    List<Object[]> first = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    List<Object[]> second = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(first.size(), 2);
    assertEquals(second.size(), 1);
    assertEquals((int[]) first.get(0)[0], new int[]{7});
    assertSame(first.get(1)[0], first.get(0)[0]);
    assertSame(second.get(0)[0], first.get(0)[0]);
    assertEquals(INVOCATIONS.get()[0], 1);
  }

  private static RexExpression literal(int value) {
    return new RexExpression.Literal(ColumnDataType.INT, value);
  }

  private static FunctionOperand operand(String name, ColumnDataType resultType, List<RexExpression> arguments) {
    return new FunctionOperand(new RexExpression.FunctionCall(resultType, name, arguments), INPUT_SCHEMA);
  }

  @ScalarFunction(nullableParameters = true)
  public static Integer operandCacheIdentity(Integer value) {
    INVOCATIONS.get()[0]++;
    if (value != null && value < 0) {
      throw new IllegalArgumentException("negative value: " + value);
    }
    return value;
  }

  @ScalarFunction
  public static int[] operandCacheArray(int value) {
    INVOCATIONS.get()[0]++;
    return new int[]{value};
  }

  @ScalarFunction(isVarArg = true)
  public static int operandCacheVarargs(Object... values) {
    INVOCATIONS.get()[0]++;
    return (Integer) values[0] + (Integer) values[1];
  }

  @ScalarFunction
  public static int operandCacheZeroArgs() {
    INVOCATIONS.get()[0]++;
    return 7;
  }

  @ScalarFunction(volatility = FunctionVolatility.STABLE)
  public static int operandCacheStable() {
    INVOCATIONS.get()[0]++;
    return 7;
  }

  @ScalarFunction(volatility = FunctionVolatility.VOLATILE)
  public static int operandCacheVolatile() {
    return ++INVOCATIONS.get()[0];
  }

  /// Registers an immutable function that opts out of planner folding, independently of the legacy annotation hint.
  @ScalarFunction
  public static class NonFoldableFunction extends FunctionRegistry.ArgumentCountBasedScalarFunction {
    public NonFoldableFunction()
        throws NoSuchMethodException {
      super("operandCacheNonFoldable", Map.of(1,
          new FunctionInfo(FunctionOperandTest.class.getMethod("operandCacheIdentity", Integer.class),
              FunctionOperandTest.class, true, false, FunctionVolatility.IMMUTABLE)));
    }
  }
}
