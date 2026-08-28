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
package org.apache.pinot.core.operator.combine;

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Verifies raw group-by result cleanup on combine failures.
@SuppressWarnings("rawtypes")
public class GroupByCombineOperatorLifecycleTest {
  @Test
  public void testClosesRawResultWhenIndexedTableConstructionFails() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable GROUP BY intColumn");
    GroupKeyGenerator groupKeyGenerator = mock(GroupKeyGenerator.class);
    AggregationGroupByResult rawResult = new AggregationGroupByResult(groupKeyGenerator,
        queryContext.getAggregationFunctions(), new GroupByResultHolder[1]);
    GroupByResultsBlock resultsBlock = mock(GroupByResultsBlock.class);
    when(resultsBlock.getAggregationGroupByResult()).thenReturn(rawResult);
    IllegalStateException indexedTableFailure = new IllegalStateException("indexed-table failure");
    when(resultsBlock.getNumGroups()).thenThrow(indexedTableFailure);
    Operator operator = mock(Operator.class);
    when(operator.nextBlock()).thenReturn(resultsBlock);
    ExecutorService executorService = mock(ExecutorService.class);
    TestGroupByCombineOperator combineOperator =
        new TestGroupByCombineOperator(List.of(operator), queryContext, executorService);

    RuntimeException thrown = expectThrows(RuntimeException.class, combineOperator::process);

    assertSame(thrown.getCause(), indexedTableFailure);
    verify(groupKeyGenerator).close();
  }

  private static class TestGroupByCombineOperator extends GroupByCombineOperator {
    private TestGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
        ExecutorService executorService) {
      super(operators, queryContext, executorService);
    }

    private void process() {
      processSegments();
    }
  }
}
