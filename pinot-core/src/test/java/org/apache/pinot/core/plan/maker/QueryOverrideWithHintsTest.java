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
package org.apache.pinot.core.plan.maker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.reduce.ReducerDataSchemaUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;


public class QueryOverrideWithHintsTest {

  private IndexSegment _indexSegment;

  @BeforeClass
  public void setUp() {
    _indexSegment = mock(IndexSegment.class);
    Mockito.when(_indexSegment.getColumnNames()).thenReturn(Set.of("$ts$MONTH"));
  }

  @Test
  public void testExpressionContextHashcode() {
    ExpressionContext expressionContext1 = ExpressionContext.forIdentifier("abc");
    ExpressionContext expressionContext2 = ExpressionContext.forIdentifier("abc");
    assertEquals(expressionContext1, expressionContext2);
    assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forIdentifier("abcd");
    assertNotEquals(expressionContext1, expressionContext2);
    assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forIdentifier("");
    assertNotEquals(expressionContext1, expressionContext2);
    assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext1 = ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "abc");
    expressionContext2 = ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "abc");
    assertEquals(expressionContext1, expressionContext2);
    assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "abcd");
    assertNotEquals(expressionContext1, expressionContext2);
    assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "");
    assertNotEquals(expressionContext1, expressionContext2);
    assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());

    expressionContext1 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "func1",
        List.of(ExpressionContext.forIdentifier("abc"),
            ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "abc"))));
    expressionContext2 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "func1",
        List.of(ExpressionContext.forIdentifier("abc"),
            ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "abc"))));
    assertEquals(expressionContext1, expressionContext2);
    assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());

    expressionContext1 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "datetrunc",
        List.of(ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "DAY"),
            ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "event_time_ts"))));
    expressionContext2 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "datetrunc",
        List.of(ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "DAY"),
            ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "event_time_ts"))));
    assertEquals(expressionContext1, expressionContext2);
    assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
  }

  @Test
  public void testOverrideFilterWithExpressionOverrideHints() {
    ExpressionContext dateTruncFunctionExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "dateTrunc", new ArrayList<>(new ArrayList<>(
            List.of(ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "MONTH"),
                ExpressionContext.forIdentifier("ts"))))));
    ExpressionContext timestampIndexColumn = ExpressionContext.forIdentifier("$ts$MONTH");
    ExpressionContext equalsExpression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "EQUALS", new ArrayList<>(
            List.of(dateTruncFunctionExpr, ExpressionContext.forLiteral(FieldSpec.DataType.INT, 1000)))));
    FilterContext filter = RequestContextUtils.getFilter(equalsExpression);
    Map<ExpressionContext, ExpressionContext> hints = Map.of(dateTruncFunctionExpr, timestampIndexColumn);
    InstancePlanMakerImplV2.overrideWithExpressionHints(filter, _indexSegment, hints);
    assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    assertEquals(filter.getPredicate().getLhs(), timestampIndexColumn);
    assertEquals(((EqPredicate) filter.getPredicate()).getValue(), "1000");

    FilterContext andFilter = RequestContextUtils.getFilter(ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "AND",
            new ArrayList<>(List.of(equalsExpression, equalsExpression)))));
    InstancePlanMakerImplV2.overrideWithExpressionHints(andFilter, _indexSegment, hints);
    assertEquals(andFilter.getChildren().get(0).getPredicate().getLhs(), timestampIndexColumn);
    assertEquals(andFilter.getChildren().get(1).getPredicate().getLhs(), timestampIndexColumn);
  }

  @Test
  public void testOverrideWithExpressionOverrideHints() {
    ExpressionContext dateTruncFunctionExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "dateTrunc", new ArrayList<>(
            List.of(ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "MONTH"),
                ExpressionContext.forIdentifier("ts")))));
    ExpressionContext timestampIndexColumn = ExpressionContext.forIdentifier("$ts$MONTH");
    ExpressionContext equalsExpression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "EQUALS", new ArrayList<>(
            List.of(dateTruncFunctionExpr, ExpressionContext.forLiteral(FieldSpec.DataType.INT, 1000)))));
    Map<ExpressionContext, ExpressionContext> hints = Map.of(dateTruncFunctionExpr, timestampIndexColumn);
    ExpressionContext newEqualsExpression =
        InstancePlanMakerImplV2.overrideWithExpressionHints(equalsExpression, _indexSegment, hints);
    assertEquals(newEqualsExpression.getFunction().getFunctionName(), "equals");
    assertEquals(newEqualsExpression.getFunction().getArguments().get(0), timestampIndexColumn);
    assertEquals(newEqualsExpression.getFunction().getArguments().get(1),
        ExpressionContext.forLiteral(FieldSpec.DataType.INT, 1000));
  }

  @Test
  public void testNotOverrideWithExpressionOverrideHints() {
    ExpressionContext dateTruncFunctionExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "dateTrunc", new ArrayList<>(
            List.of(ExpressionContext.forLiteral(FieldSpec.DataType.STRING, "DAY"),
                ExpressionContext.forIdentifier("ts")))));
    ExpressionContext timestampIndexColumn = ExpressionContext.forIdentifier("$ts$DAY");
    ExpressionContext equalsExpression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "EQUALS", new ArrayList<>(
            List.of(dateTruncFunctionExpr, ExpressionContext.forLiteral(FieldSpec.DataType.INT, 1000)))));
    Map<ExpressionContext, ExpressionContext> hints = Map.of(dateTruncFunctionExpr, timestampIndexColumn);
    ExpressionContext newEqualsExpression =
        InstancePlanMakerImplV2.overrideWithExpressionHints(equalsExpression, _indexSegment, hints);
    assertEquals(newEqualsExpression.getFunction().getFunctionName(), "equals");
    // No override as the physical column is not in the index segment.
    assertEquals(newEqualsExpression.getFunction().getArguments().get(0), dateTruncFunctionExpr);
    assertEquals(newEqualsExpression.getFunction().getArguments().get(1),
        ExpressionContext.forLiteral(FieldSpec.DataType.INT, 1000));
  }

  @Test
  public void testRewriteExpressionsWithHints() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT datetrunc('MONTH', ts), count(*), sum(datetrunc('MONTH', ts)) from myTable"
            + " group by datetrunc('MONTH', ts)");
    Expression dateTruncFunctionExpr =
        RequestUtils.getFunctionExpression("datetrunc", RequestUtils.getLiteralExpression("MONTH"),
            RequestUtils.getIdentifierExpression("ts"));
    Expression timestampIndexColumn = RequestUtils.getIdentifierExpression("$ts$MONTH");
    pinotQuery.setExpressionOverrideHints(Map.of(dateTruncFunctionExpr, timestampIndexColumn));
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    AggregationFunction count = queryContext.getAggregationFunctions()[0];
    InstancePlanMakerImplV2.rewriteQueryContextWithHints(queryContext, List.of(new SegmentContext(_indexSegment)));
    assertSame(queryContext.getAggregationFunctions()[0], count);
    assertEquals(queryContext.getSelectExpressions().get(0).getIdentifier(), "$ts$MONTH");
    assertEquals(queryContext.getGroupByExpressions().get(0).getIdentifier(), "$ts$MONTH");
    assertEquals(queryContext.getAggregationFunctions()[1].getInputExpressions(),
        List.of(ExpressionContext.forIdentifier("$ts$MONTH")));
  }

  @Test
  public void testNotRewriteExpressionsWithHints() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT datetrunc('DAY', ts), count(*), sum(abc) from myTable group by datetrunc('DAY', ts)");
    Expression dateTruncFunctionExpr =
        RequestUtils.getFunctionExpression("datetrunc", RequestUtils.getLiteralExpression("DAY"),
            RequestUtils.getIdentifierExpression("ts"));
    Expression timestampIndexColumn = RequestUtils.getIdentifierExpression("$ts$DAY");
    pinotQuery.setExpressionOverrideHints(Map.of(dateTruncFunctionExpr, timestampIndexColumn));
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    InstancePlanMakerImplV2.rewriteQueryContextWithHints(queryContext, List.of(new SegmentContext(_indexSegment)));
    assertEquals(queryContext.getSelectExpressions().get(0).getFunction(),
        queryContext.getExpressionOverrideHints().keySet().iterator().next().getFunction());
  }

  @DataProvider
  public Object[][] aggregationRewriteCases() {
    List<Object[]> cases = new ArrayList<>();
    for (boolean nullHandling : new boolean[]{false, true}) {
      for (boolean nested : new boolean[]{false, true}) {
        for (String predicate : List.of("", "DATETRUNC('MONTH', ts, 'MILLISECONDS') > 0", "TRUE", "FALSE")) {
          cases.add(new Object[]{nullHandling, predicate, nested});
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "aggregationRewriteCases")
  public void testRewriteCompiledAggregation(boolean nullHandlingEnabled, String predicate, boolean nested) {
    boolean filtered = !predicate.isEmpty();
    String filter = filtered ? " FILTER(WHERE " + predicate + ")" : "";
    String input = "DATETRUNC('MONTH', ts, 'MILLISECONDS')";
    if (nested) {
      input = "ADD(" + input + ", 1)";
    }
    QueryContext queryContext = aggregationQuery(
        "SELECT DISTINCTCOUNT(" + input + ")" + filter + " AS active_days FROM myTable", "MONTH", nullHandlingEnabled);
    if (predicate.equals("TRUE") || predicate.equals("FALSE")) {
      assertEquals(queryContext.getFilteredAggregationFunctions().get(0).getRight().isConstant(), true);
    }
    InstancePlanMakerImplV2.rewriteQueryContextWithHints(queryContext, List.of(new SegmentContext(_indexSegment)));

    ExpressionContext derived = ExpressionContext.forIdentifier("$ts$MONTH");
    ExpressionContext expectedInput = nested
        ? RequestContextUtils.getExpression(CalciteSqlParser.compileToExpression("ADD(\"$ts$MONTH\", 1)")) : derived;
    assertEquals(queryContext.getAggregationFunctions()[0].getInputExpressions(), List.of(expectedInput));
    assertEquals(queryContext.getColumns(), Set.of("$ts$MONTH"));
    assertEquals(queryContext.getAliasList(), List.of("active_days"));
    assertEquals(queryContext.isNullHandlingEnabled(), nullHandlingEnabled);
    assertEquals(queryContext.hasFilteredAggregations(), filtered);
    var pair = queryContext.getFilteredAggregationFunctions().get(0);
    if (filtered) {
      if (predicate.equals("TRUE") || predicate.equals("FALSE")) {
        assertEquals(pair.getRight().isConstant(), true);
        assertEquals(pair.getRight().isConstantTrue(), Boolean.parseBoolean(predicate));
        assertEquals(pair.getRight().isConstantFalse(), !Boolean.parseBoolean(predicate));
      } else {
        assertEquals(pair.getRight().getPredicate().getLhs(), derived);
      }
    }
    FunctionContext function = queryContext.getSelectExpressions().get(0).getFunction();
    if (filtered) {
      function = function.getArguments().get(0).getFunction();
    }
    assertEquals(queryContext.getFilteredAggregationsIndexMap().get(Pair.of(function, pair.getRight())),
        Integer.valueOf(0));

    AggregationFunction compiled = queryContext.getAggregationFunctions()[0];
    InstancePlanMakerImplV2.rewriteQueryContextWithHints(queryContext, List.of(new SegmentContext(_indexSegment)));
    assertSame(queryContext.getAggregationFunctions()[0], compiled);
  }

  @DataProvider
  public Object[][] orderByRewriteCases() {
    List<Object[]> cases = new ArrayList<>();
    for (String expression : List.of("ts", "DATETRUNC('MONTH', ts, 'MILLISECONDS')",
        "ADD(DATETRUNC('MONTH', ts, 'MILLISECONDS'), 1)")) {
      for (String ordering : List.of("ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST")) {
        cases.add(new Object[]{expression, ordering});
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "orderByRewriteCases")
  public void testRewritePreservesNullOrdering(String expression, String ordering) {
    QueryContext context = aggregationQuery(
        "SELECT DATETRUNC('MONTH', ts, 'MILLISECONDS') FROM myTable ORDER BY " + expression + " " + ordering,
        "MONTH", true);
    OrderByExpressionContext original = context.getOrderByExpressions().get(0);
    InstancePlanMakerImplV2.rewriteQueryContextWithHints(context, List.of(new SegmentContext(_indexSegment)));
    OrderByExpressionContext rewritten = context.getOrderByExpressions().get(0);
    assertEquals(rewritten.isAsc(), original.isAsc());
    assertEquals(rewritten.isNullsLast(), original.isNullsLast());
    if (expression.startsWith("DATETRUNC")) {
      assertEquals(rewritten.getExpression(), ExpressionContext.forIdentifier("$ts$MONTH"));
      assertNotSame(rewritten, original);
    } else {
      assertSame(rewritten, original);
      if (expression.startsWith("ADD")) {
        assertEquals(rewritten.getExpression().getFunction().getArguments().get(0),
            ExpressionContext.forIdentifier("$ts$MONTH"));
      }
    }
  }

  @Test
  public void testPreparationWithoutHints() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*), SUM(ts) FROM myTable");
    AggregationFunction count = queryContext.getAggregationFunctions()[0];
    AggregationFunction sum = queryContext.getAggregationFunctions()[1];
    FunctionContext sumContext = queryContext.getSelectExpressions().get(1).getFunction();
    sumContext.getArguments().set(0, ExpressionContext.forIdentifier("other"));
    Set<FunctionContext> changed = Collections.newSetFromMap(new IdentityHashMap<>());
    changed.add(sumContext);
    queryContext.prepareForExecution(changed);
    assertSame(queryContext.getAggregationFunctions()[0], count);
    assertNotSame(queryContext.getAggregationFunctions()[1], sum);
    assertEquals(queryContext.getAggregationFunctions()[1].getInputExpressions(),
        List.of(ExpressionContext.forIdentifier("other")));
    assertEquals(queryContext.getColumns(), Set.of("other"));
  }

  @Test
  public void testRewritePreservesAggregationSlots() {
    String sql = "SELECT DISTINCTCOUNT(DATETRUNC('MONTH', ts, 'MILLISECONDS')) AS days, "
        + "DISTINCTCOUNT(DATETRUNC('MONTH', ts, 'MILLISECONDS')) AS duplicate_days FROM myTable "
        + "HAVING DISTINCTCOUNT(\"$ts$MONTH\") > 0 ORDER BY DISTINCTCOUNT(\"$ts$MONTH\")";
    QueryContext queryContext = aggregationQuery(sql, "MONTH", false);
    QueryContext brokerContext = aggregationQuery(sql, "MONTH", false);
    assertEquals(queryContext.getAggregationFunctions().length, 3);
    InstancePlanMakerImplV2.rewriteQueryContextWithHints(queryContext, List.of(new SegmentContext(_indexSegment)));
    // Converging SELECT and HAVING/ORDER-BY expressions must keep their original wire slots.
    assertEquals(queryContext.getAggregationFunctions().length, 3);
    assertEquals(queryContext.getFilteredAggregationFunctions().size(), 3);
    for (AggregationFunction function : queryContext.getAggregationFunctions()) {
      assertEquals(function.getInputExpressions(), List.of(ExpressionContext.forIdentifier("$ts$MONTH")));
    }
    FunctionContext having = queryContext.getHavingFilter().getPredicate().getLhs().getFunction();
    assertEquals(queryContext.getFilteredAggregationsIndexMap().get(Pair.of(having, null)), Integer.valueOf(2));
    assertEquals(queryContext.getAliasList(), List.of("days", "duplicate_days"));
    String[] physicalNames = new String[3];
    String[] logicalNames = new String[3];
    ColumnDataType[] types = new ColumnDataType[3];
    for (int i = 0; i < 3; i++) {
      physicalNames[i] = queryContext.getAggregationFunctions()[i].getResultColumnName();
      logicalNames[i] = brokerContext.getAggregationFunctions()[i].getResultColumnName();
      types[i] = queryContext.getAggregationFunctions()[i].getIntermediateResultColumnType();
    }
    DataSchema canonical = ReducerDataSchemaUtils.canonicalizeDataSchemaForAggregation(brokerContext,
        new DataSchema(physicalNames, types));
    assertEquals(canonical.getColumnNames(), logicalNames);
    assertEquals(canonical.getColumnDataTypes(), types);
  }

  @Test
  public void testMissingIndexRetainsCompiledAggregation() {
    QueryContext queryContext = aggregationQuery(
        "SELECT DISTINCTCOUNT(DATETRUNC('DAY', ts, 'MILLISECONDS')) AS active_days FROM myTable", "DAY", false);
    AggregationFunction function = queryContext.getAggregationFunctions()[0];
    List<ExpressionContext> inputs = function.getInputExpressions();
    InstancePlanMakerImplV2.rewriteQueryContextWithHints(queryContext, List.of(new SegmentContext(_indexSegment)));
    assertSame(queryContext.getAggregationFunctions()[0], function);
    assertEquals(queryContext.getAggregationFunctions()[0].getInputExpressions(), inputs);
    assertEquals(queryContext.getColumns(), Set.of("ts"));
    assertEquals(queryContext.getAliasList(), List.of("active_days"));
  }

  @DataProvider
  public Object[][] instanceRewriteCases() {
    return new Object[][]{{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @Test(dataProvider = "instanceRewriteCases")
  public void testInstanceRewriteRequiresIndexOnEverySegment(boolean streaming, boolean allIndexed) {
    QueryContext queryContext = aggregationQuery(
        "SELECT DISTINCTCOUNT(DATETRUNC('MONTH', ts, 'MILLISECONDS')) AS active_days FROM myTable", "MONTH", false);
    IndexSegment second = mock(IndexSegment.class);
    when(second.getColumnNames()).thenReturn(allIndexed ? Set.of("ts", "$ts$MONTH") : Set.of("ts"));
    List<SegmentContext> segments = List.of(new SegmentContext(_indexSegment), new SegmentContext(second));
    Map<ExpressionContext, ExpressionContext> hints = queryContext.getExpressionOverrideHints();
    Map<ExpressionContext, ExpressionContext> originalHints = Map.copyOf(hints);
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    ExecutorService executor = mock(ExecutorService.class);
    if (streaming) {
      planMaker.makeStreamingInstancePlan(segments, queryContext, executor, mock(ResultsBlockStreamer.class));
    } else {
      planMaker.makeInstancePlan(segments, queryContext, executor);
    }
    assertSame(queryContext.getExpressionOverrideHints(), hints);
    assertEquals(hints, originalHints);
    AggregationFunction<?, ?> aggregation = queryContext.getAggregationFunctions()[0];
    ExpressionContext input = aggregation.getInputExpressions().get(0);
    if (allIndexed) {
      assertEquals(input, ExpressionContext.forIdentifier("$ts$MONTH"));
      assertEquals(queryContext.getColumns(), Set.of("$ts$MONTH"));
    } else {
      assertEquals(input.getFunction().getFunctionName(), "datetrunc");
      assertEquals(queryContext.getColumns(), Set.of("ts"));
    }
  }

  private static QueryContext aggregationQuery(String sql, String granularity, boolean nullHandlingEnabled) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    pinotQuery.setQueryOptions(Map.of("enableNullHandling", Boolean.toString(nullHandlingEnabled)));
    pinotQuery.setExpressionOverrideHints(Map.of(
        CalciteSqlParser.compileToExpression("DATETRUNC('" + granularity + "', ts, 'MILLISECONDS')"),
        RequestUtils.getIdentifierExpression("$ts$" + granularity)));
    return QueryContextConverterUtils.getQueryContext(pinotQuery);
  }
}
