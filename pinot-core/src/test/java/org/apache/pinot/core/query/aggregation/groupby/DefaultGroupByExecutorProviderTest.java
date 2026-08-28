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
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.Optional;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies built-in fallback and provider ownership during executor initialization.
public class DefaultGroupByExecutorProviderTest {
  @Test
  public void testDefaultProviderPreservesRawSideDictionaryFastPath() {
    TestSetup setup = new TestSetup();
    when(setup._columnContext.getDictionary()).thenReturn(mock(Dictionary.class));

    DefaultGroupByExecutor defaultExecutor = new DefaultGroupByExecutor(setup._queryContext, setup._expressions,
        setup._projectOperator, GroupKeyGeneratorProvider.DEFAULT);

    assertTrue(defaultExecutor.getGroupKeyGenerator() instanceof NoDictionarySingleColumnGroupKeyGenerator);
    verify(setup._columnContext, never()).getDataSource();
    defaultExecutor.getResult().closeGroupKeyGenerator();

    DefaultGroupByExecutor fallbackExecutor = new DefaultGroupByExecutor(setup._queryContext, setup._expressions,
        setup._projectOperator, context -> Optional.empty());
    assertEquals(fallbackExecutor.getGroupKeyGenerator().getClass(), NoDictionarySingleColumnGroupKeyGenerator.class);
    fallbackExecutor.getResult().closeGroupKeyGenerator();
  }

  @Test
  public void testClosesProviderGeneratorWhenInitializationFails() {
    TestSetup setup = new TestSetup();
    IllegalStateException initializationFailure = new IllegalStateException("initialization failure");
    IllegalArgumentException closeFailure = new IllegalArgumentException("close failure");
    GroupKeyGenerator groupKeyGenerator = mock(GroupKeyGenerator.class);
    when(groupKeyGenerator.getGlobalGroupKeyUpperBound()).thenThrow(initializationFailure);
    doThrow(closeFailure).when(groupKeyGenerator).close();

    IllegalStateException thrown = expectThrows(IllegalStateException.class,
        () -> new DefaultGroupByExecutor(setup._queryContext, setup._expressions, setup._projectOperator,
            context -> Optional.of(groupKeyGenerator)));

    assertSame(thrown, initializationFailure);
    assertEquals(thrown.getSuppressed(), new Throwable[]{closeFailure});
    verify(groupKeyGenerator).close();
  }

  private static class TestSetup {
    private final QueryContext _queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable GROUP BY intColumn");
    private final ExpressionContext[] _expressions =
        _queryContext.getGroupByExpressions().toArray(new ExpressionContext[0]);
    private final BaseProjectOperator<?> _projectOperator = mock(BaseProjectOperator.class);
    private final ColumnContext _columnContext = mock(ColumnContext.class);

    private TestSetup() {
      when(_projectOperator.getResultColumnContext(_expressions[0])).thenReturn(_columnContext);
      when(_columnContext.getDataType()).thenReturn(DataType.INT);
      when(_columnContext.isSingleValue()).thenReturn(true);
      when(_columnContext.isDictionaryEncoded()).thenReturn(false);
    }
  }
}
