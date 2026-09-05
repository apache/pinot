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

import java.util.Optional;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Verifies provider-owned generator cleanup when segment group-by execution fails.
public class GroupByOperatorLifecycleTest {
  @Test
  public void testClosesProviderGeneratorWhenExecutionFails() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable GROUP BY intColumn");
    ExpressionContext expression = queryContext.getGroupByExpressions().get(0);
    BaseProjectOperator<?> projectOperator = mock(BaseProjectOperator.class);
    ColumnContext columnContext = mock(ColumnContext.class);
    when(columnContext.getDataType()).thenReturn(DataType.INT);
    when(columnContext.isSingleValue()).thenReturn(true);
    when(columnContext.isDictionaryEncoded()).thenReturn(false);
    when(projectOperator.getResultColumnContext(expression)).thenReturn(columnContext);

    IllegalStateException executionFailure = new IllegalStateException("project failure");
    IllegalArgumentException closeFailure = new IllegalArgumentException("close failure");
    when(projectOperator.nextBlock()).thenThrow(executionFailure);
    GroupKeyGenerator groupKeyGenerator = mock(GroupKeyGenerator.class);
    when(groupKeyGenerator.getGlobalGroupKeyUpperBound()).thenReturn(16);
    doThrow(closeFailure).when(groupKeyGenerator).close();
    AggregationInfo aggregationInfo = mock(AggregationInfo.class);
    doReturn(projectOperator).when(aggregationInfo).getProjectOperator();
    when(aggregationInfo.isUseStarTree()).thenReturn(false);
    GroupByOperator operator = new GroupByOperator(queryContext, aggregationInfo, 10,
        context -> Optional.of(groupKeyGenerator));

    IllegalStateException thrown = expectThrows(IllegalStateException.class, operator::nextBlock);

    assertSame(thrown, executionFailure);
    assertEquals(thrown.getSuppressed(), new Throwable[]{closeFailure});
    verify(groupKeyGenerator).close();
  }
}
