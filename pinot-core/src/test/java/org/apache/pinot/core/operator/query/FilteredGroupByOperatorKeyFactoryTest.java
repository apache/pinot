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
package org.apache.pinot.core.operator.query;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

/// Exercises ownership of a query-supplied generator on filtered aggregation failure.
public class FilteredGroupByOperatorKeyFactoryTest {
  @Test
  public void testFactoryIsLazyAndGeneratorClosesWhenLaneConstructionFails() {
    QueryContext queryContext = mock(QueryContext.class);
    ExpressionContext key = ExpressionContext.forIdentifier("key");
    AggregationFunction function = mock(AggregationFunction.class);
    BaseProjectOperator<?> project = mock(BaseProjectOperator.class);
    ColumnContext column = mock(ColumnContext.class);
    AggregationInfo lane = mock(AggregationInfo.class);
    GroupKeyGenerator generator = mock(GroupKeyGenerator.class);
    AtomicInteger creations = new AtomicInteger();
    when(queryContext.getAggregationFunctions()).thenReturn(new AggregationFunction[]{function});
    when(queryContext.getFilteredAggregationFunctions()).thenReturn(List.of(Pair.of(function, null)));
    when(queryContext.getGroupByExpressions()).thenReturn(List.of(key));
    when(queryContext.getNumGroupByKeyColumns()).thenReturn(1);
    when(queryContext.getLimit()).thenReturn(1);
    when(project.getResultColumnContext(key)).thenReturn(column);
    when(project.getExecutionStatistics()).thenReturn(new ExecutionStatistics(0, 0, 0, 0));
    when(column.getDataType()).thenReturn(DataType.INT);
    when(function.getIntermediateResultColumnType()).thenReturn(ColumnDataType.LONG);
    doReturn(project).when(lane).getProjectOperator();
    when(lane.getFunctions()).thenReturn(new AggregationFunction[]{function});
    when(generator.getGlobalGroupKeyUpperBound()).thenReturn(10);
    when(function.createGroupByResultHolder(0, 10)).thenThrow(new IllegalStateException("holder failure"));
    doThrow(new IllegalStateException("close failure")).when(generator).close();
    FilteredGroupByOperator operator = new FilteredGroupByOperator(queryContext, List.of(lane), 1L,
        ignored -> {
          creations.incrementAndGet();
          return generator;
        });

    assertEquals(creations.get(), 0);
    IllegalStateException failure = expectThrows(IllegalStateException.class, operator::getNextBlock);
    assertEquals(failure.getMessage(), "holder failure");
    assertEquals(failure.getSuppressed().length, 1);
    assertEquals(failure.getSuppressed()[0].getMessage(), "close failure");
    assertEquals(creations.get(), 1);
    verify(generator).close();
  }

  @Test
  public void testSuccessfulZeroFunctionLaneSharesGeneratorUntilCallerCleanup() {
    QueryContext queryContext = mock(QueryContext.class);
    ExpressionContext key = ExpressionContext.forIdentifier("key");
    AggregationFunction function = mock(AggregationFunction.class);
    BaseProjectOperator<?> project = mock(BaseProjectOperator.class);
    ColumnContext column = mock(ColumnContext.class);
    AggregationInfo zeroFunctionLane = mock(AggregationInfo.class);
    AggregationInfo aggregateLane = mock(AggregationInfo.class);
    GroupKeyGenerator generator = mock(GroupKeyGenerator.class);
    GroupByResultHolder resultHolder = mock(GroupByResultHolder.class);
    AtomicInteger creations = new AtomicInteger();
    when(queryContext.getAggregationFunctions()).thenReturn(new AggregationFunction[]{function});
    when(queryContext.getFilteredAggregationFunctions()).thenReturn(List.of(Pair.of(function, null)));
    when(queryContext.getGroupByExpressions()).thenReturn(List.of(key));
    when(queryContext.getNumGroupByKeyColumns()).thenReturn(1);
    when(queryContext.getLimit()).thenReturn(1);
    when(queryContext.getNumGroupsLimit()).thenReturn(10);
    when(queryContext.getNumGroupsWarningLimit()).thenReturn(10);
    when(queryContext.getEffectiveSegmentGroupTrimSize()).thenReturn(0);
    when(project.getResultColumnContext(key)).thenReturn(column);
    when(project.getExecutionStatistics()).thenReturn(new ExecutionStatistics(0, 0, 0, 0));
    when(column.getDataType()).thenReturn(DataType.INT);
    when(column.isSingleValue()).thenReturn(true);
    when(function.getIntermediateResultColumnType()).thenReturn(ColumnDataType.LONG);
    when(function.createGroupByResultHolder(0, 10)).thenReturn(resultHolder);
    doReturn(project).when(zeroFunctionLane).getProjectOperator();
    doReturn(project).when(aggregateLane).getProjectOperator();
    when(zeroFunctionLane.getFunctions()).thenReturn(new AggregationFunction[0]);
    when(aggregateLane.getFunctions()).thenReturn(new AggregationFunction[]{function});
    when(generator.getGlobalGroupKeyUpperBound()).thenReturn(10);
    FilteredGroupByOperator operator = new FilteredGroupByOperator(queryContext,
        List.of(zeroFunctionLane, aggregateLane), 1L, ignored -> {
          creations.incrementAndGet();
          return generator;
        });

    var result = operator.getNextBlock();
    assertEquals(creations.get(), 1);
    verify(generator, never()).close();
    result.getAggregationGroupByResult().closeGroupKeyGenerator();
    verify(generator).close();
  }

  @Test
  public void testLimitZeroDoesNotCreateGenerator() {
    QueryContext queryContext = mock(QueryContext.class);
    AggregationFunction function = mock(AggregationFunction.class);
    ExpressionContext key = ExpressionContext.forIdentifier("key");
    BaseProjectOperator<?> project = mock(BaseProjectOperator.class);
    ColumnContext column = mock(ColumnContext.class);
    AggregationInfo lane = mock(AggregationInfo.class);
    AtomicInteger creations = new AtomicInteger();
    when(queryContext.getAggregationFunctions()).thenReturn(new AggregationFunction[]{function});
    when(queryContext.getFilteredAggregationFunctions()).thenReturn(List.of(Pair.of(function, null)));
    when(queryContext.getGroupByExpressions()).thenReturn(List.of(key));
    when(queryContext.getNumGroupByKeyColumns()).thenReturn(1);
    when(queryContext.getLimit()).thenReturn(0);
    when(project.getResultColumnContext(key)).thenReturn(column);
    when(column.getDataType()).thenReturn(DataType.INT);
    when(function.getIntermediateResultColumnType()).thenReturn(ColumnDataType.LONG);
    doReturn(project).when(lane).getProjectOperator();
    FilteredGroupByOperator operator = new FilteredGroupByOperator(queryContext, List.of(lane), 1L, ignored -> {
      creations.incrementAndGet();
      return mock(GroupKeyGenerator.class);
    });

    operator.getNextBlock();
    assertEquals(creations.get(), 0);
  }
}
