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
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Exercises binding across SQL clauses, preserving the public call identity and legacy explicit-type forms.
public class AggregationFunctionBinderTest {
  @Test
  public void testAllClausesAndOriginalExpressionIdentity() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT trim(MODE(stringValue)) AS topValue, "
        + "FIRST_WITH_TIME(timestampValue, longValue) FILTER (WHERE flag) FROM testTable GROUP BY intValue "
        + "HAVING MODE(intValue) > 1 ORDER BY LAST_WITH_TIME(stringValue, longValue)");
    List<Function> functions = aggregations(query);
    List<Function> originals = functions.stream().map(Function::deepCopy).toList();
    new QueryOptimizer().optimize(query, ExpressionTypeResolverTest.SCHEMA);
    assertEquals(functions.size(), 4);
    assertEquals(functions.stream().map(function -> function.getAggregationBinding().getResultType()).toList(),
        List.of("STRING", "TIMESTAMP", "DOUBLE", "STRING"));
    for (int i = 0; i < functions.size(); i++) {
      Function function = functions.get(i);
      assertNotNull(function.getAggregationBinding());
      assertEquals(function.getOperator(), originals.get(i).getOperator());
      assertEquals(function.getOperands(), originals.get(i).getOperands());
      assertEquals(RequestContextUtils.getFunction(function).toString(),
          RequestContextUtils.getFunction(originals.get(i)).toString());
    }
    assertEquals(query.getSelectList().size(), 2);
    PinotQuery bound = query.deepCopy();
    AggregationFunctionBinder.bind(query, ExpressionTypeResolverTest.SCHEMA);
    assertEquals(query, bound);
  }

  @Test
  public void testOptionsAndTransformInputTypes() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT MODE(add(intValue, longValue)), "
        + "MODE(intValue, 'AVG'), MODE(CAST(stringValue AS TIMESTAMP)), "
        + "FIRST_WITH_TIME(CASE WHEN flag THEN 'a' ELSE stringValue END, longValue) FROM testTable");
    AggregationFunctionBinder.bind(query, ExpressionTypeResolverTest.SCHEMA);
    assertEquals(aggregations(query).stream().map(call -> call.getAggregationBinding().getResultType()).toList(),
        List.of("DOUBLE", "DOUBLE", "TIMESTAMP", "STRING"));
  }

  @Test
  public void testDirectContextBindingAndLegacyCalls() {
    FunctionContext function = RequestContextUtils.getExpression("MODE(timestampValue)").getFunction();
    FunctionContext bound = AggregationFunctionBinder.bind(function, ExpressionTypeResolverTest.SCHEMA);
    assertNull(function.getAggregationBinding());
    assertEquals(bound.getAggregationBinding(),
        new AggregateCallBinding(List.of(ColumnDataType.TIMESTAMP), ColumnDataType.TIMESTAMP));
    assertEquals(bound, function);
    assertEquals(bound.toString(), function.toString());
    assertSame(AggregationFunctionBinder.bind(bound, ExpressionTypeResolverTest.SCHEMA), bound);

    PinotQuery legacy = CalciteSqlParser.compileToPinotQuery(
        "SELECT FIRST_WITH_TIME(stringValue, longValue, 'STRING'), "
            + "LAST_WITH_TIME(intValue, longValue, 'INT'), count(*) FROM testTable");
    PinotQuery original = legacy.deepCopy();
    AggregationFunctionBinder.bind(legacy, ExpressionTypeResolverTest.SCHEMA);
    assertEquals(legacy, original);
  }

  @Test
  public void testUnsupportedInputDoesNotFallBack() {
    for (String expression : List.of("MODE(mvValues)", "MODE(missingColumn)", "MODE(notRegistered(stringValue))",
        "MODE(stringValue, 'AVG')")) {
      PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT " + expression + " FROM testTable");
      expectThrows(IllegalArgumentException.class,
          () -> AggregationFunctionBinder.bind(query, ExpressionTypeResolverTest.SCHEMA));
    }
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
}
