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
package org.apache.pinot.core.query.request.context.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.MinAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


@SuppressWarnings("rawtypes")
public class BrokerRequestToQueryContextConverterTest {

  private int getAliasCount(List<String> aliasList) {
    int count = 0;
    for (String alias : aliasList) {
      if (alias != null) {
        count++;
      }
    }
    return count;
  }

  @Test
  public void testHardcodedQueries() {
    // Select *
    {
      String query = "SELECT * FROM testTable";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 1);
      assertEquals(selectExpressions.get(0), ExpressionContext.forIdentifier("*"));
      assertEquals(selectExpressions.get(0).toString(), "*");
      assertFalse(queryContext.isDistinct());
      assertEquals(getAliasCount(queryContext.getAliasList()), 0);
      assertNull(queryContext.getFilter());
      assertNull(queryContext.getGroupByExpressions());
      assertNull(queryContext.getOrderByExpressions());
      assertNull(queryContext.getHavingFilter());
      assertEquals(queryContext.getLimit(), 10);
      assertEquals(queryContext.getOffset(), 0);
      assertTrue(queryContext.getColumns().isEmpty());
      assertTrue(QueryContextUtils.isSelectionQuery(queryContext));
      assertFalse(QueryContextUtils.isAggregationQuery(queryContext));
      assertFalse(QueryContextUtils.isDistinctQuery(queryContext));
    }

    // Select COUNT(*)
    {
      String query = "SELECT COUNT(*) FROM testTable";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 1);
      assertEquals(selectExpressions.get(0), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "count",
              Collections.singletonList(ExpressionContext.forIdentifier("*")))));
      assertEquals(selectExpressions.get(0).toString(), "count(*)");
      assertFalse(queryContext.isDistinct());
      assertEquals(getAliasCount(queryContext.getAliasList()), 0);
      assertNull(queryContext.getFilter());
      assertNull(queryContext.getGroupByExpressions());
      assertNull(queryContext.getOrderByExpressions());
      assertNull(queryContext.getHavingFilter());
      assertEquals(queryContext.getLimit(), 10);
      assertEquals(queryContext.getOffset(), 0);
      assertTrue(queryContext.getColumns().isEmpty());
      assertFalse(QueryContextUtils.isSelectionQuery(queryContext));
      assertTrue(QueryContextUtils.isAggregationQuery(queryContext));
      assertFalse(QueryContextUtils.isDistinctQuery(queryContext));
    }

    // Order-by
    {
      String query = "SELECT foo, bar FROM testTable ORDER BY bar ASC, foo DESC LIMIT 50, 100";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 2);
      assertEquals(selectExpressions.get(0), ExpressionContext.forIdentifier("foo"));
      assertEquals(selectExpressions.get(0).toString(), "foo");
      assertEquals(selectExpressions.get(1), ExpressionContext.forIdentifier("bar"));
      assertEquals(selectExpressions.get(1).toString(), "bar");
      assertFalse(queryContext.isDistinct());
      assertEquals(getAliasCount(queryContext.getAliasList()), 0);
      assertNull(queryContext.getFilter());
      List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
      assertNotNull(orderByExpressions);
      assertEquals(orderByExpressions.size(), 2);
      assertEquals(orderByExpressions.get(0),
          new OrderByExpressionContext(ExpressionContext.forIdentifier("bar"), true));
      assertEquals(orderByExpressions.get(0).toString(), "bar ASC");
      assertEquals(orderByExpressions.get(1).toString(), "foo DESC");
      assertNull(queryContext.getHavingFilter());
      assertEquals(queryContext.getLimit(), 100);
      assertEquals(queryContext.getOffset(), 50);
      assertEquals(queryContext.getColumns(), new HashSet<>(Arrays.asList("foo", "bar")));
      assertTrue(QueryContextUtils.isSelectionQuery(queryContext));
      assertFalse(QueryContextUtils.isAggregationQuery(queryContext));
      assertFalse(QueryContextUtils.isDistinctQuery(queryContext));
    }

    // Distinct with order-by
    {
      String query = "SELECT DISTINCT foo, bar, foobar FROM testTable ORDER BY bar DESC, foo LIMIT 15";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 3);
      assertEquals(selectExpressions.get(0), ExpressionContext.forIdentifier("foo"));
      assertEquals(selectExpressions.get(0).toString(), "foo");
      assertEquals(selectExpressions.get(1), ExpressionContext.forIdentifier("bar"));
      assertEquals(selectExpressions.get(1).toString(), "bar");
      assertEquals(selectExpressions.get(2), ExpressionContext.forIdentifier("foobar"));
      assertEquals(selectExpressions.get(2).toString(), "foobar");
      assertTrue(queryContext.isDistinct());
      assertEquals(getAliasCount(queryContext.getAliasList()), 0);
      assertNull(queryContext.getFilter());
      assertNull(queryContext.getGroupByExpressions());
      List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
      assertNotNull(orderByExpressions);
      assertEquals(orderByExpressions.size(), 2);
      assertEquals(orderByExpressions.get(0),
          new OrderByExpressionContext(ExpressionContext.forIdentifier("bar"), false));
      assertEquals(orderByExpressions.get(0).toString(), "bar DESC");
      assertEquals(orderByExpressions.get(1),
          new OrderByExpressionContext(ExpressionContext.forIdentifier("foo"), true));
      assertEquals(orderByExpressions.get(1).toString(), "foo ASC");
      assertNull(queryContext.getHavingFilter());
      assertEquals(queryContext.getLimit(), 15);
      assertEquals(queryContext.getOffset(), 0);
      assertEquals(queryContext.getColumns(), new HashSet<>(Arrays.asList("foo", "bar", "foobar")));
      assertFalse(QueryContextUtils.isSelectionQuery(queryContext));
      assertFalse(QueryContextUtils.isAggregationQuery(queryContext));
      assertTrue(QueryContextUtils.isDistinctQuery(queryContext));
    }

    // Transform with order-by
    {
      String query =
          "SELECT ADD(foo, ADD(bar, 123)), SUB('456', foobar) FROM testTable ORDER BY SUB(456, foobar) LIMIT 30, 20";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 2);
      assertEquals(selectExpressions.get(0), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.TRANSFORM, "add",
              Arrays.asList(ExpressionContext.forIdentifier("foo"), ExpressionContext.forFunction(
                  new FunctionContext(FunctionContext.Type.TRANSFORM, "add",
                      Arrays.asList(ExpressionContext.forIdentifier("bar"),
                          ExpressionContext.forLiteralContext(FieldSpec.DataType.INT, Integer.valueOf(123)))))))));
      assertEquals(selectExpressions.get(0).toString(), "add(foo,add(bar,'123'))");
      assertEquals(selectExpressions.get(1), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.TRANSFORM, "sub",
              Arrays.asList(ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "456"),
                  ExpressionContext.forIdentifier("foobar")))));
      assertEquals(selectExpressions.get(1).toString(), "sub('456',foobar)");
      assertFalse(queryContext.isDistinct());
      assertEquals(getAliasCount(queryContext.getAliasList()), 0);
      assertNull(queryContext.getFilter());
      assertNull(queryContext.getGroupByExpressions());
      List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
      assertNotNull(orderByExpressions);
      assertEquals(orderByExpressions.size(), 1);
      assertEquals(orderByExpressions.get(0), new OrderByExpressionContext(ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.TRANSFORM, "sub",
              Arrays.asList(ExpressionContext.forLiteralContext(FieldSpec.DataType.INT, Integer.valueOf(456)),
                  ExpressionContext.forIdentifier("foobar")))), true));
      assertEquals(orderByExpressions.get(0).toString(), "sub('456',foobar) ASC");
      assertNull(queryContext.getHavingFilter());
      assertEquals(queryContext.getLimit(), 20);
      assertEquals(queryContext.getOffset(), 30);
      assertEquals(queryContext.getColumns(), new HashSet<>(Arrays.asList("foo", "bar", "foobar")));
      assertTrue(QueryContextUtils.isSelectionQuery(queryContext));
      assertFalse(QueryContextUtils.isAggregationQuery(queryContext));
      assertFalse(QueryContextUtils.isDistinctQuery(queryContext));
    }

    // Boolean literal parsing
    {
      String query = "SELECT true = true FROM testTable;";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 1);
      assertEquals(selectExpressions.get(0), ExpressionContext.forLiteralContext(FieldSpec.DataType.BOOLEAN, true));
      assertEquals(selectExpressions.get(0).toString(), "'true'");
    }

    // Aggregation group-by with transform, order-by
    {
      String query =
          "SELECT SUB(foo, bar), bar, SUM(ADD(foo, bar)) FROM testTable GROUP BY SUB(foo, bar), bar ORDER BY "
              + "SUM(ADD(foo, bar)), SUB(foo, bar) DESC LIMIT 20";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 3);
      assertEquals(selectExpressions.get(0), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.TRANSFORM, "sub",
              Arrays.asList(ExpressionContext.forIdentifier("foo"), ExpressionContext.forIdentifier("bar")))));
      assertEquals(selectExpressions.get(0).toString(), "sub(foo,bar)");
      assertEquals(selectExpressions.get(1), ExpressionContext.forIdentifier("bar"));
      assertEquals(selectExpressions.get(1).toString(), "bar");
      assertEquals(selectExpressions.get(2), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "sum", Collections.singletonList(
              ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "add",
                  Arrays.asList(ExpressionContext.forIdentifier("foo"), ExpressionContext.forIdentifier("bar"))))))));
      assertEquals(selectExpressions.get(2).toString(), "sum(add(foo,bar))");
      assertFalse(queryContext.isDistinct());
      assertEquals(getAliasCount(queryContext.getAliasList()), 0);
      assertNull(queryContext.getFilter());
      List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
      assertNotNull(groupByExpressions);
      assertEquals(groupByExpressions.size(), 2);
      assertEquals(groupByExpressions.get(0), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.TRANSFORM, "sub",
              Arrays.asList(ExpressionContext.forIdentifier("foo"), ExpressionContext.forIdentifier("bar")))));
      assertEquals(groupByExpressions.get(0).toString(), "sub(foo,bar)");
      assertEquals(groupByExpressions.get(1), ExpressionContext.forIdentifier("bar"));
      assertEquals(groupByExpressions.get(1).toString(), "bar");
      List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
      assertNotNull(orderByExpressions);
      assertEquals(orderByExpressions.size(), 2);
      assertEquals(orderByExpressions.get(0), new OrderByExpressionContext(ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "sum", Collections.singletonList(
              ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "add",
                  Arrays.asList(ExpressionContext.forIdentifier("foo"), ExpressionContext.forIdentifier("bar"))))))),
          true));
      assertEquals(orderByExpressions.get(0).toString(), "sum(add(foo,bar)) ASC");
      assertEquals(orderByExpressions.get(1), new OrderByExpressionContext(ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.TRANSFORM, "sub",
              Arrays.asList(ExpressionContext.forIdentifier("foo"), ExpressionContext.forIdentifier("bar")))), false));
      assertEquals(orderByExpressions.get(1).toString(), "sub(foo,bar) DESC");
      assertNull(queryContext.getHavingFilter());
      assertEquals(queryContext.getLimit(), 20);
      assertEquals(queryContext.getOffset(), 0);
      assertEquals(queryContext.getColumns(), new HashSet<>(Arrays.asList("foo", "bar")));
      assertFalse(QueryContextUtils.isSelectionQuery(queryContext));
      assertTrue(QueryContextUtils.isAggregationQuery(queryContext));
      assertFalse(QueryContextUtils.isDistinctQuery(queryContext));
    }

    // Filter with transform
    {
      String query = "SELECT * FROM testTable WHERE foo > 15 AND (DIV(bar, foo) BETWEEN 10 AND 20 OR "
          + "TEXT_MATCH(foobar, 'potato'))";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 1);
      assertEquals(selectExpressions.get(0), ExpressionContext.forIdentifier("*"));
      assertEquals(selectExpressions.get(0).toString(), "*");
      assertFalse(queryContext.isDistinct());
      assertEquals(getAliasCount(queryContext.getAliasList()), 0);
      FilterContext filter = queryContext.getFilter();
      assertNotNull(filter);
      assertEquals(filter.getType(), FilterContext.Type.AND);
      List<FilterContext> children = filter.getChildren();
      assertEquals(children.size(), 2);
      assertEquals(children.get(0), FilterContext.forPredicate(
          new RangePredicate(ExpressionContext.forIdentifier("foo"), false, "15", false, "*",
              FieldSpec.DataType.STRING)));
      FilterContext orFilter = children.get(1);
      assertEquals(orFilter.getType(), FilterContext.Type.OR);
      assertEquals(orFilter.getChildren().size(), 2);
      assertEquals(orFilter.getChildren().get(0), FilterContext.forPredicate(new RangePredicate(
          ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "div",
              Arrays.asList(ExpressionContext.forIdentifier("bar"), ExpressionContext.forIdentifier("foo")))), true,
          "10", true, "20", FieldSpec.DataType.STRING)));
      assertEquals(orFilter.getChildren().get(1),
          FilterContext.forPredicate(new TextMatchPredicate(ExpressionContext.forIdentifier("foobar"), "potato")));
      assertEquals(filter.toString(),
          "(foo > '15' AND (div(bar,foo) BETWEEN '10' AND '20' OR text_match(foobar,'potato')))");
      assertNull(queryContext.getGroupByExpressions());
      assertNull(queryContext.getOrderByExpressions());
      assertNull(queryContext.getHavingFilter());
      assertEquals(queryContext.getLimit(), 10);
      assertEquals(queryContext.getOffset(), 0);
      assertEquals(queryContext.getColumns(), new HashSet<>(Arrays.asList("foo", "bar", "foobar")));
      assertTrue(QueryContextUtils.isSelectionQuery(queryContext));
      assertFalse(QueryContextUtils.isAggregationQuery(queryContext));
      assertFalse(QueryContextUtils.isDistinctQuery(queryContext));
    }

    // Alias
    // NOTE: All the references to the alias should already be converted to the original expressions.
    {
      String query =
          "SELECT SUM(foo) AS a, bar AS b FROM testTable WHERE bar IN (5, 10, 15) GROUP BY b ORDER BY a DESC";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 2);
      assertEquals(selectExpressions.get(0), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "sum",
              Collections.singletonList(ExpressionContext.forIdentifier("foo")))));
      assertEquals(selectExpressions.get(0).toString(), "sum(foo)");
      assertEquals(selectExpressions.get(1), ExpressionContext.forIdentifier("bar"));
      assertEquals(selectExpressions.get(1).toString(), "bar");
      assertFalse(queryContext.isDistinct());
      List<String> aliasList = queryContext.getAliasList();
      assertEquals(aliasList.size(), 2);
      assertEquals(aliasList.get(0), "a");
      assertEquals(aliasList.get(1), "b");
      FilterContext filter = queryContext.getFilter();
      assertNotNull(filter);
      assertEquals(filter, FilterContext.forPredicate(
          new InPredicate(ExpressionContext.forIdentifier("bar"), Arrays.asList("5", "10", "15"))));
      assertEquals(filter.toString(), "bar IN ('5','10','15')");
      List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
      assertNotNull(groupByExpressions);
      assertEquals(groupByExpressions.size(), 1);
      assertEquals(groupByExpressions.get(0), ExpressionContext.forIdentifier("bar"));
      assertEquals(groupByExpressions.get(0).toString(), "bar");
      List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
      assertNotNull(orderByExpressions);
      assertEquals(orderByExpressions.size(), 1);
      assertEquals(orderByExpressions.get(0), new OrderByExpressionContext(ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "sum",
              Collections.singletonList(ExpressionContext.forIdentifier("foo")))), false));
      assertEquals(orderByExpressions.get(0).toString(), "sum(foo) DESC");
      assertNull(queryContext.getHavingFilter());
      assertEquals(queryContext.getLimit(), 10);
      assertEquals(queryContext.getOffset(), 0);
      assertEquals(queryContext.getColumns(), new HashSet<>(Arrays.asList("foo", "bar")));
      assertFalse(QueryContextUtils.isSelectionQuery(queryContext));
      assertTrue(QueryContextUtils.isAggregationQuery(queryContext));
      assertFalse(QueryContextUtils.isDistinctQuery(queryContext));
    }

    // Having
    {
      String query = "SELECT SUM(foo), bar FROM testTable GROUP BY bar HAVING SUM(foo) IN (5, 10, 15)";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 2);
      assertEquals(selectExpressions.get(0), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "sum",
              Collections.singletonList(ExpressionContext.forIdentifier("foo")))));
      assertEquals(selectExpressions.get(0).toString(), "sum(foo)");
      assertEquals(selectExpressions.get(1), ExpressionContext.forIdentifier("bar"));
      assertEquals(selectExpressions.get(1).toString(), "bar");
      assertFalse(queryContext.isDistinct());
      assertEquals(getAliasCount(queryContext.getAliasList()), 0);
      assertNull(queryContext.getFilter());
      List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
      assertNotNull(groupByExpressions);
      assertEquals(groupByExpressions.size(), 1);
      assertEquals(groupByExpressions.get(0), ExpressionContext.forIdentifier("bar"));
      assertEquals(groupByExpressions.get(0).toString(), "bar");
      assertNull(queryContext.getOrderByExpressions());
      FilterContext havingFilter = queryContext.getHavingFilter();
      assertNotNull(havingFilter);
      assertEquals(havingFilter, FilterContext.forPredicate(new InPredicate(ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "sum",
              Collections.singletonList(ExpressionContext.forIdentifier("foo")))), Arrays.asList("5", "10", "15"))));
      assertEquals(havingFilter.toString(), "sum(foo) IN ('5','10','15')");
      assertEquals(queryContext.getLimit(), 10);
      assertEquals(queryContext.getOffset(), 0);
      assertEquals(queryContext.getColumns(), new HashSet<>(Arrays.asList("foo", "bar")));
      assertFalse(QueryContextUtils.isSelectionQuery(queryContext));
      assertTrue(QueryContextUtils.isAggregationQuery(queryContext));
      assertFalse(QueryContextUtils.isDistinctQuery(queryContext));
    }

    // Post-aggregation
    {
      String query = "SELECT SUM(col1) * MAX(col2) FROM testTable GROUP BY col3 HAVING SUM(col1) > MIN(col2) AND "
          + "SUM(col4) + col3 < MAX(col4) ORDER BY MAX(col1) + MAX(col2) - SUM(col4), col3 DESC";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      assertEquals(queryContext.getTableName(), "testTable");

      // SELECT clause
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      assertEquals(selectExpressions.size(), 1);
      FunctionContext function = selectExpressions.get(0).getFunction();
      assertEquals(function.getType(), FunctionContext.Type.TRANSFORM);
      assertEquals(function.getFunctionName(), "times");
      List<ExpressionContext> arguments = function.getArguments();
      assertEquals(arguments.size(), 2);
      assertEquals(arguments.get(0), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "sum",
              Collections.singletonList(ExpressionContext.forIdentifier("col1")))));
      assertEquals(arguments.get(1), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "max",
              Collections.singletonList(ExpressionContext.forIdentifier("col2")))));

      // HAVING clause
      FilterContext havingFilter = queryContext.getHavingFilter();
      assertNotNull(havingFilter);
      assertEquals(havingFilter.getType(), FilterContext.Type.AND);
      List<FilterContext> children = havingFilter.getChildren();
      assertEquals(children.size(), 2);
      FilterContext firstChild = children.get(0);
      assertEquals(firstChild.getType(), FilterContext.Type.PREDICATE);
      Predicate predicate = firstChild.getPredicate();
      assertEquals(predicate.getType(), Predicate.Type.RANGE);
      RangePredicate rangePredicate = (RangePredicate) predicate;
      assertEquals(rangePredicate.getLowerBound(), "0");
      assertFalse(rangePredicate.isLowerInclusive());
      assertEquals(rangePredicate.getUpperBound(), RangePredicate.UNBOUNDED);
      assertFalse(rangePredicate.isUpperInclusive());
      function = rangePredicate.getLhs().getFunction();
      assertEquals(function.getFunctionName(), "minus");
      arguments = function.getArguments();
      assertEquals(arguments.size(), 2);
      assertEquals(arguments.get(0), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "sum",
              Collections.singletonList(ExpressionContext.forIdentifier("col1")))));
      assertEquals(arguments.get(1), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "min",
              Collections.singletonList(ExpressionContext.forIdentifier("col2")))));
      // Skip checking the second child of the AND filter

      // ORDER-BY clause
      List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
      assertNotNull(orderByExpressions);
      assertEquals(orderByExpressions.size(), 2);
      OrderByExpressionContext firstOrderByExpression = orderByExpressions.get(0);
      assertTrue(firstOrderByExpression.isAsc());
      function = firstOrderByExpression.getExpression().getFunction();
      assertEquals(function.getFunctionName(), "minus");
      arguments = function.getArguments();
      assertEquals(arguments.size(), 2);
      assertEquals(arguments.get(0).getFunction().getFunctionName(), "plus");
      assertEquals(arguments.get(1), ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, "sum",
              Collections.singletonList(ExpressionContext.forIdentifier("col4")))));

      assertEquals(queryContext.getColumns(), new HashSet<>(Arrays.asList("col1", "col2", "col3", "col4")));
      assertFalse(QueryContextUtils.isSelectionQuery(queryContext));
      assertTrue(QueryContextUtils.isAggregationQuery(queryContext));
      assertFalse(QueryContextUtils.isDistinctQuery(queryContext));

      // Expected: SUM(col1), MAX(col2), MIN(col2), SUM(col4), MAX(col4), MAX(col1)
      //noinspection rawtypes
      AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
      assertNotNull(aggregationFunctions);
      assertEquals(aggregationFunctions.length, 6);
      assertEquals(aggregationFunctions[0].getResultColumnName(), "sum(col1)");
      assertEquals(aggregationFunctions[1].getResultColumnName(), "max(col2)");
      assertEquals(aggregationFunctions[2].getResultColumnName(), "min(col2)");
      assertEquals(aggregationFunctions[3].getResultColumnName(), "sum(col4)");
      assertEquals(aggregationFunctions[4].getResultColumnName(), "max(col4)");
      assertEquals(aggregationFunctions[5].getResultColumnName(), "max(col1)");
      Map<FunctionContext, Integer> aggregationFunctionIndexMap = queryContext.getAggregationFunctionIndexMap();
      assertNotNull(aggregationFunctionIndexMap);
      assertEquals(aggregationFunctionIndexMap.size(), 6);
      assertEquals((int) aggregationFunctionIndexMap.get(new FunctionContext(FunctionContext.Type.AGGREGATION, "sum",
          Collections.singletonList(ExpressionContext.forIdentifier("col1")))), 0);
      assertEquals((int) aggregationFunctionIndexMap.get(new FunctionContext(FunctionContext.Type.AGGREGATION, "max",
          Collections.singletonList(ExpressionContext.forIdentifier("col2")))), 1);
      assertEquals((int) aggregationFunctionIndexMap.get(new FunctionContext(FunctionContext.Type.AGGREGATION, "min",
          Collections.singletonList(ExpressionContext.forIdentifier("col2")))), 2);
      assertEquals((int) aggregationFunctionIndexMap.get(new FunctionContext(FunctionContext.Type.AGGREGATION, "sum",
          Collections.singletonList(ExpressionContext.forIdentifier("col4")))), 3);
      assertEquals((int) aggregationFunctionIndexMap.get(new FunctionContext(FunctionContext.Type.AGGREGATION, "max",
          Collections.singletonList(ExpressionContext.forIdentifier("col4")))), 4);
      assertEquals((int) aggregationFunctionIndexMap.get(new FunctionContext(FunctionContext.Type.AGGREGATION, "max",
          Collections.singletonList(ExpressionContext.forIdentifier("col1")))), 5);
    }

    // DistinctCountThetaSketch (string literal and escape quote)
    {
      String query = "SELECT DISTINCTCOUNTTHETASKETCH(foo, 'nominalEntries=1000', 'bar=''a''', 'bar=''b''', "
          + "'SET_INTERSECT($1, $2)') FROM testTable WHERE bar IN ('a', 'b')";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      FunctionContext function = queryContext.getSelectExpressions().get(0).getFunction();
      assertEquals(function.getType(), FunctionContext.Type.AGGREGATION);
      assertEquals(function.getFunctionName(), "distinctcountthetasketch");
      List<ExpressionContext> arguments = function.getArguments();
      assertEquals(arguments.get(0), ExpressionContext.forIdentifier("foo"));
      assertEquals(arguments.get(1),
          ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "nominalEntries=1000"));
      assertEquals(arguments.get(2), ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "bar='a'"));
      assertEquals(arguments.get(3), ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "bar='b'"));
      assertEquals(arguments.get(4),
          ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "SET_INTERSECT($1, $2)"));
      assertEquals(queryContext.getColumns(), new HashSet<>(Arrays.asList("foo", "bar")));
      assertFalse(QueryContextUtils.isSelectionQuery(queryContext));
      assertTrue(QueryContextUtils.isAggregationQuery(queryContext));
      assertFalse(QueryContextUtils.isDistinctQuery(queryContext));
    }
  }

  @Test
  public void testFilteredAggregations() {
    {
      String query =
          "SELECT COUNT(*) FILTER(WHERE foo > 5), COUNT(*) FILTER(WHERE foo < 6) FROM testTable WHERE bar > 0";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

      AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
      assertNotNull(aggregationFunctions);
      assertEquals(aggregationFunctions.length, 2);
      assertTrue(aggregationFunctions[0] instanceof CountAggregationFunction);
      assertTrue(aggregationFunctions[1] instanceof CountAggregationFunction);

      List<Pair<AggregationFunction, FilterContext>> filteredAggregationFunctions =
          queryContext.getFilteredAggregationFunctions();
      assertNotNull(filteredAggregationFunctions);
      assertEquals(filteredAggregationFunctions.size(), 2);
      assertTrue(filteredAggregationFunctions.get(0).getLeft() instanceof CountAggregationFunction);
      assertEquals(filteredAggregationFunctions.get(0).getRight().toString(), "foo > '5'");
      assertTrue(filteredAggregationFunctions.get(1).getLeft() instanceof CountAggregationFunction);
      assertEquals(filteredAggregationFunctions.get(1).getRight().toString(), "foo < '6'");

      Map<FunctionContext, Integer> aggregationIndexMap = queryContext.getAggregationFunctionIndexMap();
      assertNotNull(aggregationIndexMap);
      assertEquals(aggregationIndexMap.size(), 1);
      for (Map.Entry<FunctionContext, Integer> entry : aggregationIndexMap.entrySet()) {
        FunctionContext aggregation = entry.getKey();
        int index = entry.getValue();
        assertEquals(aggregation.toString(), "count(*)");
        assertTrue(index == 0 || index == 1);
      }

      Map<Pair<FunctionContext, FilterContext>, Integer> filteredAggregationsIndexMap =
          queryContext.getFilteredAggregationsIndexMap();
      assertNotNull(filteredAggregationsIndexMap);
      assertEquals(filteredAggregationsIndexMap.size(), 2);
      for (Map.Entry<Pair<FunctionContext, FilterContext>, Integer> entry : filteredAggregationsIndexMap.entrySet()) {
        Pair<FunctionContext, FilterContext> pair = entry.getKey();
        FunctionContext aggregation = pair.getLeft();
        FilterContext filter = pair.getRight();
        int index = entry.getValue();
        assertEquals(aggregation.toString(), "count(*)");
        switch (index) {
          case 0:
            assertEquals(filter.toString(), "foo > '5'");
            break;
          case 1:
            assertEquals(filter.toString(), "foo < '6'");
            break;
          default:
            fail();
            break;
        }
      }
    }

    {
      String query = "SELECT SUM(salary), SUM(salary) FILTER(WHERE salary IS NOT NULL), MIN(salary), "
          + "MIN(salary) FILTER(WHERE salary > 50000) FROM testTable WHERE bar > 0";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

      AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
      assertNotNull(aggregationFunctions);
      assertEquals(aggregationFunctions.length, 4);
      assertTrue(aggregationFunctions[0] instanceof SumAggregationFunction);
      assertTrue(aggregationFunctions[1] instanceof SumAggregationFunction);
      assertTrue(aggregationFunctions[2] instanceof MinAggregationFunction);
      assertTrue(aggregationFunctions[3] instanceof MinAggregationFunction);

      List<Pair<AggregationFunction, FilterContext>> filteredAggregationFunctions =
          queryContext.getFilteredAggregationFunctions();
      assertNotNull(filteredAggregationFunctions);
      assertEquals(filteredAggregationFunctions.size(), 4);
      assertTrue(filteredAggregationFunctions.get(0).getLeft() instanceof SumAggregationFunction);
      assertNull(filteredAggregationFunctions.get(0).getRight());
      assertTrue(filteredAggregationFunctions.get(1).getLeft() instanceof SumAggregationFunction);
      assertEquals(filteredAggregationFunctions.get(1).getRight().toString(), "salary IS NOT NULL");
      assertTrue(filteredAggregationFunctions.get(2).getLeft() instanceof MinAggregationFunction);
      assertNull(filteredAggregationFunctions.get(2).getRight());
      assertTrue(filteredAggregationFunctions.get(3).getLeft() instanceof MinAggregationFunction);
      assertEquals(filteredAggregationFunctions.get(3).getRight().toString(), "salary > '50000'");

      Map<FunctionContext, Integer> aggregationIndexMap = queryContext.getAggregationFunctionIndexMap();
      assertNotNull(aggregationIndexMap);
      assertEquals(aggregationIndexMap.size(), 2);
      for (Map.Entry<FunctionContext, Integer> entry : aggregationIndexMap.entrySet()) {
        FunctionContext aggregation = entry.getKey();
        int index = entry.getValue();
        switch (index) {
          case 0:
          case 1:
            assertEquals(aggregation.toString(), "sum(salary)");
            break;
          case 2:
          case 3:
            assertEquals(aggregation.toString(), "min(salary)");
            break;
          default:
            fail();
            break;
        }
      }

      Map<Pair<FunctionContext, FilterContext>, Integer> filteredAggregationsIndexMap =
          queryContext.getFilteredAggregationsIndexMap();
      assertNotNull(filteredAggregationsIndexMap);
      assertEquals(filteredAggregationsIndexMap.size(), 4);
      for (Map.Entry<Pair<FunctionContext, FilterContext>, Integer> entry : filteredAggregationsIndexMap.entrySet()) {
        Pair<FunctionContext, FilterContext> pair = entry.getKey();
        FunctionContext aggregation = pair.getLeft();
        FilterContext filter = pair.getRight();
        int index = entry.getValue();
        switch (index) {
          case 0:
            assertEquals(aggregation.toString(), "sum(salary)");
            assertNull(filter);
            break;
          case 1:
            assertEquals(aggregation.toString(), "sum(salary)");
            assertEquals(filter.toString(), "salary IS NOT NULL");
            break;
          case 2:
            assertEquals(aggregation.toString(), "min(salary)");
            assertNull(filter);
            break;
          case 3:
            assertEquals(aggregation.toString(), "min(salary)");
            assertEquals(filter.toString(), "salary > '50000'");
            break;
          default:
            fail();
            break;
        }
      }
    }
  }

  @Test
  public void testDeduplicateOrderByExpressions() {
    String query = "SELECT name FROM employees ORDER BY name DESC NULLS LAST, name ASC";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    assertNotNull(queryContext.getOrderByExpressions());
    assertEquals(queryContext.getOrderByExpressions().size(), 1);
  }

  @Test
  public void testRemoveOrderByFunctions() {
    String query = "SELECT A FROM testTable ORDER BY datetrunc(A) DESC NULLS LAST";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    assertNotNull(queryContext.getOrderByExpressions());
    List<OrderByExpressionContext> orderByExpressionContexts = queryContext.getOrderByExpressions();
    assertEquals(orderByExpressionContexts.size(), 1);
    OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);
    assertFalse(orderByExpressionContext.isAsc());
    assertTrue(orderByExpressionContext.isNullsLast());
    assertEquals(orderByExpressionContext.getExpression().getFunction().getFunctionName(), "datetrunc");
    assertEquals(orderByExpressionContext.getExpression().getFunction().getArguments().get(0).getIdentifier(), "A");
  }

  @Test
  public void testOrderByDefault() {
    String query = "SELECT A FROM testTable ORDER BY A";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    assertNotNull(queryContext.getOrderByExpressions());
    List<OrderByExpressionContext> orderByExpressionContexts = queryContext.getOrderByExpressions();
    assertEquals(orderByExpressionContexts.size(), 1);
    OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);
    assertTrue(orderByExpressionContext.isAsc());
    assertTrue(orderByExpressionContext.isNullsLast());
  }

  @Test
  public void testOrderByNullsLast() {
    String query = "SELECT A FROM testTable ORDER BY A NULLS LAST";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    assertNotNull(queryContext.getOrderByExpressions());
    List<OrderByExpressionContext> orderByExpressionContexts = queryContext.getOrderByExpressions();
    assertEquals(orderByExpressionContexts.size(), 1);
    OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);
    assertTrue(orderByExpressionContext.isAsc());
    assertTrue(orderByExpressionContext.isNullsLast());
  }

  @Test
  public void testOrderByNullsFirst() {
    String query = "SELECT A FROM testTable ORDER BY A NULLS FIRST";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    assertNotNull(queryContext.getOrderByExpressions());
    List<OrderByExpressionContext> orderByExpressionContexts = queryContext.getOrderByExpressions();
    assertEquals(orderByExpressionContexts.size(), 1);
    OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);
    assertTrue(orderByExpressionContext.isAsc());
    assertFalse(orderByExpressionContext.isNullsLast());
  }

  @Test
  public void testOrderByAscNullsFirst() {
    String query = "SELECT A FROM testTable ORDER BY A ASC NULLS FIRST";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    assertNotNull(queryContext.getOrderByExpressions());
    List<OrderByExpressionContext> orderByExpressionContexts = queryContext.getOrderByExpressions();
    assertEquals(orderByExpressionContexts.size(), 1);
    OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);
    assertTrue(orderByExpressionContext.isAsc());
    assertFalse(orderByExpressionContext.isNullsLast());
  }

  @Test
  public void testOrderByAscNullsLast() {
    String query = "SELECT A FROM testTable ORDER BY A ASC NULLS LAST";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    assertNotNull(queryContext.getOrderByExpressions());
    List<OrderByExpressionContext> orderByExpressionContexts = queryContext.getOrderByExpressions();
    assertEquals(orderByExpressionContexts.size(), 1);
    OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);
    assertTrue(orderByExpressionContext.isAsc());
    assertTrue(orderByExpressionContext.isNullsLast());
  }

  @Test
  public void testOrderByDescNullsFirst() {
    String query = "SELECT A FROM testTable ORDER BY A DESC NULLS FIRST";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    assertNotNull(queryContext.getOrderByExpressions());
    List<OrderByExpressionContext> orderByExpressionContexts = queryContext.getOrderByExpressions();
    assertEquals(orderByExpressionContexts.size(), 1);
    OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);
    assertFalse(orderByExpressionContext.isAsc());
    assertFalse(orderByExpressionContext.isNullsLast());
  }

  @Test
  public void testOrderByDescNullsLast() {
    String query = "SELECT A FROM testTable ORDER BY A DESC NULLS LAST";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    assertNotNull(queryContext.getOrderByExpressions());
    List<OrderByExpressionContext> orderByExpressionContexts = queryContext.getOrderByExpressions();
    assertEquals(orderByExpressionContexts.size(), 1);
    OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);
    assertFalse(orderByExpressionContext.isAsc());
    assertTrue(orderByExpressionContext.isNullsLast());
  }

  @Test
  public void testDistinctOrderByNullsLast() {
    String query = "SELECT DISTINCT A FROM testTable ORDER BY A NULLS LAST";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    assertNotNull(queryContext.getOrderByExpressions());
    List<OrderByExpressionContext> orderByExpressionContexts = queryContext.getOrderByExpressions();
    assertEquals(orderByExpressionContexts.size(), 1);
    OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);
    assertTrue(orderByExpressionContext.isAsc());
    assertTrue(orderByExpressionContext.isNullsLast());
  }

  @Test
  public void testConstantFilter() {
    String query = "SELECT * FROM testTable WHERE true";
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertNull(queryContext.getFilter());

    query = "SELECT * FROM testTable WHERE false";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertSame(queryContext.getFilter(), FilterContext.CONSTANT_FALSE);

    query = "SELECT * FROM testTable WHERE 'true'";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertNull(queryContext.getFilter());

    query = "SELECT * FROM testTable WHERE 'false'";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertSame(queryContext.getFilter(), FilterContext.CONSTANT_FALSE);

    query = "SELECT * FROM testTable WHERE 1 = 1";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertNull(queryContext.getFilter());

    query = "SELECT * FROM testTable WHERE 1 = 0";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertSame(queryContext.getFilter(), FilterContext.CONSTANT_FALSE);

    query = "SELECT * FROM testTable WHERE 1 = 1 AND 2 = 2";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertNull(queryContext.getFilter());

    query = "SELECT * FROM testTable WHERE 1 = 1 AND 2 = 3";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertSame(queryContext.getFilter(), FilterContext.CONSTANT_FALSE);

    query = "SELECT * FROM testTable WHERE 1 = 1 OR 2 = 3";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertNull(queryContext.getFilter());

    query = "SELECT * FROM testTable WHERE 1 = 0 OR 2 = 3";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertSame(queryContext.getFilter(), FilterContext.CONSTANT_FALSE);

    query = "SELECT * FROM testTable WHERE NOT 1 = 1";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertSame(queryContext.getFilter(), FilterContext.CONSTANT_FALSE);

    query = "SELECT * FROM testTable WHERE NOT 1 = 0";
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertNull(queryContext.getFilter());
  }
}
