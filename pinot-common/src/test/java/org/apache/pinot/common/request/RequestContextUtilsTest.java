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
package org.apache.pinot.common.request;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RequestContextUtilsTest {
  private IndexSegment _indexSegment = new IndexSegment() {
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
      return null;
    }

    @Override
    public Set<String> getPhysicalColumnNames() {
      return ImmutableSet.of("$ts$MONTH");
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

    @Override
    public GenericRow getRecord(int docId, GenericRow reuse) {
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
    Assert.assertEquals(expressionContext1, expressionContext2);
    Assert.assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forIdentifier("abcd");
    Assert.assertNotEquals(expressionContext1, expressionContext2);
    Assert.assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forIdentifier("");
    Assert.assertNotEquals(expressionContext1, expressionContext2);
    Assert.assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());

    expressionContext1 = ExpressionContext.forLiteral("abc");
    expressionContext2 = ExpressionContext.forLiteral("abc");
    Assert.assertEquals(expressionContext1, expressionContext2);
    Assert.assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forLiteral("abcd");
    Assert.assertNotEquals(expressionContext1, expressionContext2);
    Assert.assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forLiteral("");
    Assert.assertNotEquals(expressionContext1, expressionContext2);
    Assert.assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());

    expressionContext1 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "func1",
        ImmutableList.of(ExpressionContext.forIdentifier("abc"), ExpressionContext.forLiteral("abc"))));
    expressionContext2 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "func1",
        ImmutableList.of(ExpressionContext.forIdentifier("abc"), ExpressionContext.forLiteral("abc"))));
    Assert.assertEquals(expressionContext1, expressionContext2);
    Assert.assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());

    expressionContext1 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "datetrunc",
        ImmutableList.of(ExpressionContext.forLiteral("DAY"), ExpressionContext.forLiteral("event_time_ts"))));
    expressionContext2 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "datetrunc",
        ImmutableList.of(ExpressionContext.forLiteral("DAY"), ExpressionContext.forLiteral("event_time_ts"))));
    Assert.assertEquals(expressionContext1, expressionContext2);
    Assert.assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
  }

  @Test
  public void testOverrideFilterWithExpressionOverrideHints() {
    ExpressionContext dateTruncFunctionExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "dateTrunc",
            ImmutableList.of(ExpressionContext.forLiteral("MONTH"), ExpressionContext.forIdentifier("ts"))));
    ExpressionContext timestampIndexColumn = ExpressionContext.forIdentifier("$ts$MONTH");
    ExpressionContext equalsExpression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "EQUALS",
            ImmutableList.of(dateTruncFunctionExpr, ExpressionContext.forLiteral("1000"))));
    FilterContext filter = RequestContextUtils.getFilter(equalsExpression);
    Map<ExpressionContext, ExpressionContext> hints = ImmutableMap.of(dateTruncFunctionExpr, timestampIndexColumn);
    FilterContext newFilterContext =
        RequestContextUtils.overrideFilterWithExpressionOverrideHints(filter, _indexSegment, hints);
    Assert.assertEquals(newFilterContext.getType(), FilterContext.Type.PREDICATE);
    Assert.assertEquals(newFilterContext.getPredicate().getLhs(), timestampIndexColumn);
    Assert.assertEquals(((EqPredicate) newFilterContext.getPredicate()).getValue(), "1000");

    FilterContext andFilter = RequestContextUtils.getFilter(ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "AND",
            ImmutableList.of(equalsExpression, equalsExpression))));
    FilterContext newAndFilterContext =
        RequestContextUtils.overrideFilterWithExpressionOverrideHints(andFilter, _indexSegment, hints);
    Assert.assertEquals(newAndFilterContext.getChildren().get(0).getPredicate().getLhs(), timestampIndexColumn);
    Assert.assertEquals(newAndFilterContext.getChildren().get(1).getPredicate().getLhs(), timestampIndexColumn);
  }

  @Test
  public void testOverrideWithExpressionOverrideHints() {
    ExpressionContext dateTruncFunctionExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "dateTrunc",
            ImmutableList.of(ExpressionContext.forLiteral("MONTH"), ExpressionContext.forIdentifier("ts"))));
    ExpressionContext timestampIndexColumn = ExpressionContext.forIdentifier("$ts$MONTH");
    ExpressionContext equalsExpression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "EQUALS",
            ImmutableList.of(dateTruncFunctionExpr, ExpressionContext.forLiteral("1000"))));
    Map<ExpressionContext, ExpressionContext> hints = ImmutableMap.of(dateTruncFunctionExpr, timestampIndexColumn);
    ExpressionContext newEqualsExpression =
        RequestContextUtils.overrideWithExpressionHints(_indexSegment, hints, equalsExpression);
    Assert.assertEquals(newEqualsExpression.getFunction().getFunctionName(), "equals");
    Assert.assertEquals(newEqualsExpression.getFunction().getArguments().get(0), timestampIndexColumn);
    Assert.assertEquals(newEqualsExpression.getFunction().getArguments().get(1), ExpressionContext.forLiteral("1000"));
  }

  @Test
  public void testNotOverrideWithExpressionOverrideHints() {
    ExpressionContext dateTruncFunctionExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "dateTrunc",
            ImmutableList.of(ExpressionContext.forLiteral("DAY"), ExpressionContext.forIdentifier("ts"))));
    ExpressionContext timestampIndexColumn = ExpressionContext.forIdentifier("$ts$DAY");
    ExpressionContext equalsExpression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "EQUALS",
            ImmutableList.of(dateTruncFunctionExpr, ExpressionContext.forLiteral("1000"))));
    Map<ExpressionContext, ExpressionContext> hints = ImmutableMap.of(dateTruncFunctionExpr, timestampIndexColumn);
    ExpressionContext newEqualsExpression =
        RequestContextUtils.overrideWithExpressionHints(_indexSegment, hints, equalsExpression);
    Assert.assertEquals(newEqualsExpression.getFunction().getFunctionName(), "equals");
    // No override as the physical column is not in the index segment.
    Assert.assertEquals(newEqualsExpression.getFunction().getArguments().get(0), dateTruncFunctionExpr);
    Assert.assertEquals(newEqualsExpression.getFunction().getArguments().get(1), ExpressionContext.forLiteral("1000"));
  }
}
