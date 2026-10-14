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
package org.apache.pinot.core.operator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.query.RemoteAccessStats;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class InstanceResponseOperatorTest {

  @Test
  public void testLiteModeLeafStageLimitReachedWhenNumRowsEqualsLimit() {
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.LITE_MODE_IMPLICIT_LEAF_STAGE_LIMIT, "10");

    SelectionResultsBlock resultsBlock = mock(SelectionResultsBlock.class);
    when(resultsBlock.getNumRows()).thenReturn(10);
    when(resultsBlock.getResultsMetadata()).thenReturn(new HashMap<>());
    when(resultsBlock.getNumServerThreads()).thenReturn(1);

    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getQueryOptions()).thenReturn(queryOptions);

    InstanceResponseOperator operator = new InstanceResponseOperator(
        mock(BaseCombineOperator.class), List.of(), List.of(), queryContext);

    InstanceResponseBlock responseBlock = operator.buildInstanceResponseBlock(resultsBlock);

    assertEquals(responseBlock.getResponseMetadata().get(MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName()),
        "true");
  }

  @Test
  public void testLiteModeLeafStageLimitNotReachedWhenNumRowsBelowLimit() {
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.LITE_MODE_IMPLICIT_LEAF_STAGE_LIMIT, "10");

    SelectionResultsBlock resultsBlock = mock(SelectionResultsBlock.class);
    when(resultsBlock.getNumRows()).thenReturn(5);
    when(resultsBlock.getResultsMetadata()).thenReturn(new HashMap<>());
    when(resultsBlock.getNumServerThreads()).thenReturn(1);

    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getQueryOptions()).thenReturn(queryOptions);

    InstanceResponseOperator operator = new InstanceResponseOperator(
        mock(BaseCombineOperator.class), List.of(), List.of(), queryContext);

    InstanceResponseBlock responseBlock = operator.buildInstanceResponseBlock(resultsBlock);

    assertFalse(responseBlock.getResponseMetadata()
        .containsKey(MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName()));
  }

  @Test
  public void testNoLiteModeMetadataWhenOptionAbsent() {
    Map<String, String> queryOptions = new HashMap<>();

    SelectionResultsBlock resultsBlock = mock(SelectionResultsBlock.class);
    when(resultsBlock.getNumRows()).thenReturn(100);
    when(resultsBlock.getResultsMetadata()).thenReturn(new HashMap<>());
    when(resultsBlock.getNumServerThreads()).thenReturn(1);

    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getQueryOptions()).thenReturn(queryOptions);

    InstanceResponseOperator operator = new InstanceResponseOperator(
        mock(BaseCombineOperator.class), List.of(), List.of(), queryContext);

    InstanceResponseBlock responseBlock = operator.buildInstanceResponseBlock(resultsBlock);

    assertFalse(responseBlock.getResponseMetadata()
        .containsKey(MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName()));
  }

  @Test
  public void testLiteModeLeafStageLimitReachedWhenNumRowsExceedsLimit() {
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.LITE_MODE_IMPLICIT_LEAF_STAGE_LIMIT, "10");

    SelectionResultsBlock resultsBlock = mock(SelectionResultsBlock.class);
    when(resultsBlock.getNumRows()).thenReturn(15);
    when(resultsBlock.getResultsMetadata()).thenReturn(new HashMap<>());
    when(resultsBlock.getNumServerThreads()).thenReturn(1);

    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getQueryOptions()).thenReturn(queryOptions);

    InstanceResponseOperator operator = new InstanceResponseOperator(
        mock(BaseCombineOperator.class), List.of(), List.of(), queryContext);

    InstanceResponseBlock responseBlock = operator.buildInstanceResponseBlock(resultsBlock);

    assertTrue(responseBlock.getResponseMetadata()
        .containsKey(MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName()));
  }

  /// Releasing the fetched buffers reports the remote accesses recorded on the fetch contexts to the query execution.
  @Test
  public void testReleaseReportsRemoteAccessesOfFetchContexts() {
    FetchContext fetchContext1 = new FetchContext(UUID.randomUUID(), "segment1", Set.of("col"));
    FetchContext fetchContext2 = new FetchContext(UUID.randomUUID(), "segment2", Set.of("col"));
    IndexSegment segment1 = mock(IndexSegment.class);
    IndexSegment segment2 = mock(IndexSegment.class);
    doAnswer(_ -> {
      fetchContext1.getRemoteAccessStats().addRemoteAccesses(3, 300);
      return null;
    }).when(segment1).release(fetchContext1);
    doAnswer(_ -> {
      fetchContext2.getRemoteAccessStats().addRemoteAccesses(4, 400);
      return null;
    }).when(segment2).release(fetchContext2);
    InstanceResponseOperator operator = new InstanceResponseOperator(mock(BaseCombineOperator.class),
        List.of(new SegmentContext(segment1), new SegmentContext(segment2)), List.of(fetchContext1, fetchContext2),
        mock(QueryContext.class));

    try (QueryThreadContext threadContext = QueryThreadContext.openForSseTest()) {
      operator.releaseAll();
      RemoteAccessStats remoteAccessStats = threadContext.getRemoteAccessStats();
      assertEquals(remoteAccessStats.getNumRemoteAccesses(), 7);
      assertEquals(remoteAccessStats.getRemoteAccessBytes(), 700);
    }
  }
}
