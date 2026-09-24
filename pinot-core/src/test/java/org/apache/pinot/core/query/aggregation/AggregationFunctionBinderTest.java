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
package org.apache.pinot.core.query.aggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.pinot.common.function.AggregationFunctionTypeResolver;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises where the binder attaches call bindings and how it reports failures. No aggregate requires a binding yet,
/// so most tests stub the per-call rule to require one for MODE, with the logical type of its first argument.
public class AggregationFunctionBinderTest {
  private static final String QUERY = "SELECT trim(MODE(stringValue)), "
      + "MODE(CAST(stringValue AS TIMESTAMP)) FILTER (WHERE flag), SUM(intValue) FROM testTable "
      + "WHERE intValue > 1 GROUP BY intValue HAVING MODE(intValue) > 1 ORDER BY MODE(flag)";

  @Test
  public void testCallsThatDoNotRequireBindingAreUnchanged() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT MODE(stringValue), ANY_VALUE(flag), "
        + "FIRST_WITH_TIME(stringValue, longValue, 'STRING'), ARRAY_AGG(timestampValue, 'LONG', true), "
        + "MODE(notRegistered(stringValue)), COUNT(*) FROM testTable GROUP BY intValue HAVING MODE(intValue) > 1");
    PinotQuery original = query.deepCopy();
    new QueryOptimizer().optimize(query, ExpressionTypeResolverTest.SCHEMA);
    AggregationFunctionBinder.bind(query, ExpressionTypeResolverTest.SCHEMA);
    assertTrue(aggregations(query).stream().noneMatch(Function::isSetAggregationBinding));
    assertEquals(query.getSelectList(), original.getSelectList());

    FunctionContext function = RequestContextUtils.getExpression("MODE(timestampValue)").getFunction();
    assertSame(AggregationFunctionBinder.bind(function, ExpressionTypeResolverTest.SCHEMA), function);
  }

  @Test
  public void testBindsEveryClauseWithoutChangingCallIdentity() {
    try (MockedStatic<AggregationFunctionTypeResolver> ignored = requireBindingForMode()) {
      PinotQuery query = CalciteSqlParser.compileToPinotQuery(QUERY);
      List<Function> functions = aggregations(query);
      List<Function> originals = functions.stream().map(Function::deepCopy).toList();
      new QueryOptimizer().optimize(query, ExpressionTypeResolverTest.SCHEMA);

      // Aggregates nested in a transform, under FILTER, in HAVING and in ORDER BY are all bound.
      assertEquals(functions.stream().map(ExpectedBinding::of).toList(), List.of(
          new ExpectedBinding("mode", "STRING"), new ExpectedBinding("mode", "TIMESTAMP"),
          new ExpectedBinding("sum", null), new ExpectedBinding("mode", "INT"),
          new ExpectedBinding("mode", "BOOLEAN")));
      for (int i = 0; i < functions.size(); i++) {
        Function function = functions.get(i);
        assertEquals(function.getOperator(), originals.get(i).getOperator());
        assertEquals(function.getOperands(), originals.get(i).getOperands());
        assertEquals(RequestContextUtils.getFunction(function), RequestContextUtils.getFunction(originals.get(i)));
      }
    }
  }

  @Test
  public void testExistingBindingIsAuthoritative() {
    try (MockedStatic<AggregationFunctionTypeResolver> ignored = requireBindingForMode()) {
      PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT MODE(stringValue) FROM testTable");
      AggregateCallBinding existing = new AggregateCallBinding(List.of(ColumnDataType.JSON), ColumnDataType.STRING);
      aggregations(query).get(0).setAggregationBinding(existing.toThrift());
      AggregationFunctionBinder.bind(query, ExpressionTypeResolverTest.SCHEMA);
      assertEquals(AggregateCallBinding.fromThrift(aggregations(query).get(0).getAggregationBinding()), existing);

      FunctionContext function = RequestContextUtils.getExpression("MODE(timestampValue)").getFunction();
      FunctionContext bound = AggregationFunctionBinder.bind(function, ExpressionTypeResolverTest.SCHEMA);
      assertNull(function.getAggregationBinding());
      assertEquals(bound.getAggregationBinding(),
          new AggregateCallBinding(List.of(ColumnDataType.TIMESTAMP), ColumnDataType.TIMESTAMP));
      assertEquals(bound, function);
      assertSame(AggregationFunctionBinder.bind(bound, ExpressionTypeResolverTest.SCHEMA), bound);
      assertSame(AggregationFunctionBinder.bind(function, null), function);
    }
  }

  @Test
  public void testBindingFailureIsBadQuery() {
    try (MockedStatic<AggregationFunctionTypeResolver> resolver = mockStatic(AggregationFunctionTypeResolver.class)) {
      resolver.when(() -> AggregationFunctionTypeResolver.bind(any(), anyList(), any()))
          .thenThrow(new IllegalArgumentException("Unsupported MODE input type"));
      PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT MODE(stringValue) FROM testTable");
      BadQueryRequestException error = expectThrows(BadQueryRequestException.class,
          () -> AggregationFunctionBinder.bind(query, ExpressionTypeResolverTest.SCHEMA));
      assertTrue(error.getMessage().contains("mode(stringValue)"), error.getMessage());
      assertTrue(error.getMessage().contains("Unsupported MODE input type"), error.getMessage());
      expectThrows(BadQueryRequestException.class, () -> AggregationFunctionBinder.bind(
          RequestContextUtils.getExpression("MODE(stringValue)").getFunction(), ExpressionTypeResolverTest.SCHEMA));
    }
  }

  @Test
  public void testUnbindRemovesEveryBinding() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(QUERY);
    PinotQuery original = query.deepCopy();
    try (MockedStatic<AggregationFunctionTypeResolver> ignored = requireBindingForMode()) {
      AggregationFunctionBinder.bind(query, ExpressionTypeResolverTest.SCHEMA);
    }
    assertFalse(query.equals(original));
    AggregationFunctionBinder.unbind(query);
    assertEquals(query, original);
  }

  /// Stubs the per-call rule so MODE requires a binding whose result type is its first argument's logical type.
  private static MockedStatic<AggregationFunctionTypeResolver> requireBindingForMode() {
    MockedStatic<AggregationFunctionTypeResolver> resolver = mockStatic(AggregationFunctionTypeResolver.class);
    resolver.when(() -> AggregationFunctionTypeResolver.bind(any(), anyList(), any())).thenAnswer(invocation -> {
      if (invocation.getArgument(0) != AggregationFunctionType.MODE) {
        return null;
      }
      IntFunction<ColumnDataType> argumentTypes = invocation.getArgument(2);
      ColumnDataType type = argumentTypes.apply(0);
      return new AggregateCallBinding(List.of(type), type);
    });
    return resolver;
  }

  private static List<Function> aggregations(PinotQuery query) {
    List<Function> functions = new ArrayList<>();
    query.getSelectList().forEach(expression -> collect(expression, functions));
    if (query.getHavingExpression() != null) {
      collect(query.getHavingExpression(), functions);
    }
    if (query.getOrderByList() != null) {
      query.getOrderByList().forEach(expression -> collect(expression, functions));
    }
    return functions;
  }

  private static void collect(Expression expression, List<Function> functions) {
    if (expression.isSetFunctionCall()) {
      Function function = expression.getFunctionCall();
      if (AggregationFunctionType.isAggregationFunction(function.getOperator())) {
        functions.add(function);
      }
      function.getOperands().forEach(operand -> collect(operand, functions));
    }
  }

  private record ExpectedBinding(String operator, String resultType) {
    static ExpectedBinding of(Function function) {
      return new ExpectedBinding(function.getOperator(),
          function.isSetAggregationBinding() ? function.getAggregationBinding().getResultType() : null);
    }
  }
}
