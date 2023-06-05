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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


public class QueryOverrideWithHintsTest {

  private final IndexSegment _indexSegment = new IndexSegment() {
    @Override
    public String getSegmentName() {
      return null;
    }

    @Override
    public SegmentMetadata getSegmentMetadata() {
      return null;
    }

    @Override
    public Set<String> getColumnNames() {
      return ImmutableSet.of("$ts$MONTH");
    }

    @Override
    public Set<String> getPhysicalColumnNames() {
      return null;
    }

    @Override
    public DataSource getDataSource(String columnName) {
      return null;
    }

    @Override
    public List<StarTreeV2> getStarTrees() {
      return null;
    }

    @Nullable
    @Override
    public ThreadSafeMutableRoaringBitmap getValidDocIds() {
      return null;
    }

    @Nullable
    @Override
    public ThreadSafeMutableRoaringBitmap getQueryableDocIds() {
      return null;
    }

    @Override
    public GenericRow getRecord(int docId, GenericRow reuse) {
      return null;
    }

    @Override
    public Object getValue(int docId, String column) {
      return null;
    }

    @Override
    public void destroy() {
    }
  };

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
    expressionContext1 = ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "abc");
    expressionContext2 = ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "abc");
    assertEquals(expressionContext1, expressionContext2);
    assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "abcd");
    assertNotEquals(expressionContext1, expressionContext2);
    assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "");
    assertNotEquals(expressionContext1, expressionContext2);
    assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());

    expressionContext1 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "func1",
        ImmutableList.of(ExpressionContext.forIdentifier("abc"),
            ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "abc"))));
    expressionContext2 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "func1",
        ImmutableList.of(ExpressionContext.forIdentifier("abc"),
            ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "abc"))));
    assertEquals(expressionContext1, expressionContext2);
    assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());

    expressionContext1 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "datetrunc",
        ImmutableList.of(ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "DAY"),
            ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "event_time_ts"))));
    expressionContext2 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "datetrunc",
        ImmutableList.of(ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "DAY"),
            ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "event_time_ts"))));
    assertEquals(expressionContext1, expressionContext2);
    assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
  }

  @Test
  public void testOverrideFilterWithExpressionOverrideHints() {
    ExpressionContext dateTruncFunctionExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "dateTrunc", new ArrayList<>(new ArrayList<>(
            ImmutableList.of(ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "MONTH"),
                ExpressionContext.forIdentifier("ts"))))));
    ExpressionContext timestampIndexColumn = ExpressionContext.forIdentifier("$ts$MONTH");
    ExpressionContext equalsExpression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "EQUALS", new ArrayList<>(
            ImmutableList.of(dateTruncFunctionExpr,
                ExpressionContext.forLiteralContext(FieldSpec.DataType.INT, 1000)))));
    FilterContext filter = RequestContextUtils.getFilter(equalsExpression);
    Map<ExpressionContext, ExpressionContext> hints = ImmutableMap.of(dateTruncFunctionExpr, timestampIndexColumn);
    InstancePlanMakerImplV2.overrideWithExpressionHints(filter, _indexSegment, hints);
    assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    assertEquals(filter.getPredicate().getLhs(), timestampIndexColumn);
    assertEquals(((EqPredicate) filter.getPredicate()).getValue(), "1000");

    FilterContext andFilter = RequestContextUtils.getFilter(ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "AND",
            new ArrayList<>(ImmutableList.of(equalsExpression, equalsExpression)))));
    InstancePlanMakerImplV2.overrideWithExpressionHints(andFilter, _indexSegment, hints);
    assertEquals(andFilter.getChildren().get(0).getPredicate().getLhs(), timestampIndexColumn);
    assertEquals(andFilter.getChildren().get(1).getPredicate().getLhs(), timestampIndexColumn);
  }

  @Test
  public void testOverrideWithExpressionOverrideHints() {
    ExpressionContext dateTruncFunctionExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "dateTrunc", new ArrayList<>(
            ImmutableList.of(ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "MONTH"),
                ExpressionContext.forIdentifier("ts")))));
    ExpressionContext timestampIndexColumn = ExpressionContext.forIdentifier("$ts$MONTH");
    ExpressionContext equalsExpression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "EQUALS", new ArrayList<>(
            ImmutableList.of(dateTruncFunctionExpr,
                ExpressionContext.forLiteralContext(FieldSpec.DataType.INT, 1000)))));
    Map<ExpressionContext, ExpressionContext> hints = ImmutableMap.of(dateTruncFunctionExpr, timestampIndexColumn);
    ExpressionContext newEqualsExpression =
        InstancePlanMakerImplV2.overrideWithExpressionHints(equalsExpression, _indexSegment, hints);
    assertEquals(newEqualsExpression.getFunction().getFunctionName(), "equals");
    assertEquals(newEqualsExpression.getFunction().getArguments().get(0), timestampIndexColumn);
    assertEquals(newEqualsExpression.getFunction().getArguments().get(1),
        ExpressionContext.forLiteralContext(FieldSpec.DataType.INT, 1000));
  }

  @Test
  public void testNotOverrideWithExpressionOverrideHints() {
    ExpressionContext dateTruncFunctionExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "dateTrunc", new ArrayList<>(
            ImmutableList.of(ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "DAY"),
                ExpressionContext.forIdentifier("ts")))));
    ExpressionContext timestampIndexColumn = ExpressionContext.forIdentifier("$ts$DAY");
    ExpressionContext equalsExpression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "EQUALS", new ArrayList<>(
            ImmutableList.of(dateTruncFunctionExpr,
                ExpressionContext.forLiteralContext(FieldSpec.DataType.INT, 1000)))));
    Map<ExpressionContext, ExpressionContext> hints = ImmutableMap.of(dateTruncFunctionExpr, timestampIndexColumn);
    ExpressionContext newEqualsExpression =
        InstancePlanMakerImplV2.overrideWithExpressionHints(equalsExpression, _indexSegment, hints);
    assertEquals(newEqualsExpression.getFunction().getFunctionName(), "equals");
    // No override as the physical column is not in the index segment.
    assertEquals(newEqualsExpression.getFunction().getArguments().get(0), dateTruncFunctionExpr);
    assertEquals(newEqualsExpression.getFunction().getArguments().get(1),
        ExpressionContext.forLiteralContext(FieldSpec.DataType.INT, 1000));
  }

  @Test
  public void testRewriteExpressionsWithHints() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT datetrunc('MONTH', ts), count(*), sum(abc) from myTable group by datetrunc('MONTH', ts) ");
    Expression dateTruncFunctionExpr = RequestUtils.getFunctionExpression("datetrunc");
    dateTruncFunctionExpr.getFunctionCall().setOperands(new ArrayList<>(
        ImmutableList.of(RequestUtils.getLiteralExpression("MONTH"), RequestUtils.getIdentifierExpression("ts"))));
    Expression timestampIndexColumn = RequestUtils.getIdentifierExpression("$ts$MONTH");
    pinotQuery.setExpressionOverrideHints(ImmutableMap.of(dateTruncFunctionExpr, timestampIndexColumn));
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    InstancePlanMakerImplV2.rewriteQueryContextWithHints(queryContext, _indexSegment);
    assertEquals(queryContext.getSelectExpressions().get(0).getIdentifierName(), "$ts$MONTH");
    assertEquals(queryContext.getGroupByExpressions().get(0).getIdentifierName(), "$ts$MONTH");
  }

  @Test
  public void testNotRewriteExpressionsWithHints() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT datetrunc('DAY', ts), count(*), sum(abc) from myTable group by datetrunc('DAY', ts)");
    Expression dateTruncFunctionExpr = RequestUtils.getFunctionExpression("datetrunc");
    dateTruncFunctionExpr.getFunctionCall().setOperands(new ArrayList<>(
        ImmutableList.of(RequestUtils.getLiteralExpression("DAY"), RequestUtils.getIdentifierExpression("ts"))));
    Expression timestampIndexColumn = RequestUtils.getIdentifierExpression("$ts$DAY");
    pinotQuery.setExpressionOverrideHints(ImmutableMap.of(dateTruncFunctionExpr, timestampIndexColumn));
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    InstancePlanMakerImplV2.rewriteQueryContextWithHints(queryContext, _indexSegment);
    assertEquals(queryContext.getSelectExpressions().get(0).getFunction(),
        queryContext.getExpressionOverrideHints().keySet().iterator().next().getFunction());
  }
}
