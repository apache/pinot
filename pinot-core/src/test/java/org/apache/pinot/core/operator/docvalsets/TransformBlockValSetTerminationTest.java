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
package org.apache.pinot.core.operator.docvalsets;

import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;


/// Checks that [TransformBlockValSet] does not evaluate the transform function once the query is terminated, timed
/// out or interrupted.
public class TransformBlockValSetTerminationTest {
  private static final String[] VALUES = {"a", "b"};

  @Test
  public void testEvaluatesWhenQueryIsActive() {
    TransformFunction transformFunction = mockTransformFunction();
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      assertSame(newValSet(transformFunction).getStringValuesSV(), VALUES);
    }
  }

  @Test
  public void testEvaluatesWithoutQueryThreadContext() {
    TransformFunction transformFunction = mockTransformFunction();
    assertSame(newValSet(transformFunction).getStringValuesSV(), VALUES);
  }

  @Test
  public void testStopsWhenQueryIsTerminated() {
    TransformFunction transformFunction = mockTransformFunction();
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext,
        ThreadAccountantUtils.getNoOpAccountant())) {
      executionContext.terminate(QueryErrorCode.QUERY_CANCELLATION, "terminated for test");
      TerminationException e =
          expectThrows(TerminationException.class, () -> newValSet(transformFunction).getStringValuesSV());
      assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_CANCELLATION);
    }
    verify(transformFunction, never()).transformToStringValuesSV(any());
  }

  @Test
  public void testStopsWhenQueryIsTimedOut() {
    TransformFunction transformFunction = mockTransformFunction();
    long nowMs = System.currentTimeMillis();
    QueryExecutionContext executionContext = new QueryExecutionContext(QueryExecutionContext.QueryType.SSE, 123L,
        "cid", "default", nowMs - 1000, nowMs - 1, nowMs - 1, "brokerId", "instanceId", "");
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext,
        ThreadAccountantUtils.getNoOpAccountant())) {
      QueryException e = expectThrows(QueryException.class, () -> newValSet(transformFunction).getStringValuesSV());
      assertEquals(e.getErrorCode(), QueryErrorCode.EXECUTION_TIMEOUT);
    }
    verify(transformFunction, never()).transformToStringValuesSV(any());
  }

  @Test
  public void testStopsWhenThreadIsInterrupted() {
    TransformFunction transformFunction = mockTransformFunction();
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      Thread.currentThread().interrupt();
      assertThrows(EarlyTerminationException.class, () -> newValSet(transformFunction).getStringValuesSV());
    } finally {
      // Clear the flag in case the check did not consume it.
      Thread.interrupted();
    }
    verify(transformFunction, never()).transformToStringValuesSV(any());
  }

  private static TransformFunction mockTransformFunction() {
    TransformFunction transformFunction = mock(TransformFunction.class);
    when(transformFunction.getName()).thenReturn("testTransform");
    when(transformFunction.transformToStringValuesSV(any())).thenReturn(VALUES);
    return transformFunction;
  }

  private static TransformBlockValSet newValSet(TransformFunction transformFunction) {
    ValueBlock valueBlock = mock(ValueBlock.class);
    when(valueBlock.getNumDocs()).thenReturn(VALUES.length);
    return new TransformBlockValSet(valueBlock, transformFunction);
  }
}
