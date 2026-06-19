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
package org.apache.pinot.query.runtime.operator;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.Table;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock.EarlyTerminationReason;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerOperator;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class LeafOperatorTest {
  private final ExecutorService _executorService = Executors.newCachedThreadPool();
  private final AtomicReference<LeafOperator> _operatorRef = new AtomicReference<>();

  private AutoCloseable _mocks;

  @Mock
  private VirtualServerAddress _serverAddress;

  @BeforeMethod
  public void setUpMethod() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDownMethod()
      throws Exception {
    _mocks.close();
  }

  @AfterClass
  public void tearDown() {
    _executorService.shutdown();
  }

  private QueryExecutor mockQueryExecutor(List<BaseResultsBlock> dataBlocks, InstanceResponseBlock metadataBlock) {
    QueryExecutor queryExecutor = mock(QueryExecutor.class);
    when(queryExecutor.execute(any(), any(), any())).thenAnswer(invocation -> {
      LeafOperator operator = _operatorRef.get();
      for (BaseResultsBlock dataBlock : dataBlocks) {
        operator.addResultsBlock(dataBlock);
      }
      return metadataBlock;
    });
    return queryExecutor;
  }

  private List<ServerQueryRequest> mockQueryRequests(int numRequests) {
    ServerQueryRequest queryRequest = mock(ServerQueryRequest.class);
    when(queryRequest.getQueryContext()).thenReturn(mock(QueryContext.class));
    List<ServerQueryRequest> queryRequests = new ArrayList<>(numRequests);
    for (int i = 0; i < numRequests; i++) {
      queryRequests.add(queryRequest);
    }
    return queryRequests;
  }

  @Test
  public void shouldReturnDataBlockThenMetadataBlock() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<BaseResultsBlock> dataBlocks = Collections.singletonList(
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2}), queryContext));
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then:
    List<Object[]> rows = ((MseBlock.Data) resultBlock).asRowHeap().getRows();
    assertEquals(rows.get(0), new Object[]{"foo", 1});
    assertEquals(rows.get(1), new Object[]{"", 2});
    assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading 2 blocks");

    operator.close();
  }

  @Test
  public void calculateStatsIsIdempotentWhenFoldingPipelineBreaker() {
    // Regression for a pipeline-breaker leaf whose own operators were duplicated in the flat stats. calculateStats()
    // runs more than once per opchain (the MailboxSendOperator serializing its EOS stats, then again from the
    // scheduler completion callback that feeds the stream-stats listener). The folded pipeline-breaker stats are a
    // shared mutable instance, so LeafOperator.calculateUpstreamStats() must hand back a COPY -- otherwise each call
    // appends this leaf's own LEAF entry again, inflating the flat operator list and breaking the stats-tree encoder
    // with a treeSize != flatSize mismatch.
    DataSchema schema = new DataSchema(new String[]{"intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT intCol FROM tbl");
    List<BaseResultsBlock> dataBlocks = Collections.singletonList(
        new SelectionResultsBlock(schema, Collections.singletonList(new Object[]{1}), queryContext));
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);

    // The leaf folds two operators (MAILBOX_RECEIVE + PIPELINE_BREAKER) ahead of its own LEAF entry.
    OpChainExecutionContext context = OperatorTestUtil.getTracingContext();
    MultiStageQueryStats pipelineBreakerStats = new MultiStageQueryStats.Builder(context.getStageId())
        .customizeOpen(open -> open
            .addLastOperator(MultiStageOperator.Type.MAILBOX_RECEIVE,
                new StatMap<>(BaseMailboxReceiveOperator.StatKey.class))
            .addLastOperator(MultiStageOperator.Type.PIPELINE_BREAKER,
                new StatMap<>(PipelineBreakerOperator.StatKey.class)))
        .build();

    LeafOperator operator = new LeafOperator(context, mockQueryRequests(1), schema, queryExecutor, _executorService,
        pipelineBreakerStats);
    _operatorRef.set(operator);

    // Drain so the leaf produces its stats.
    while (!operator.nextBlock().isEos()) {
      // consume blocks
    }

    int firstCount = operator.calculateStats().getCurrentStats().getLastOperatorIndex() + 1;
    int secondCount = operator.calculateStats().getCurrentStats().getLastOperatorIndex() + 1;

    // MAILBOX_RECEIVE + PIPELINE_BREAKER (folded) + LEAF (this operator) = 3. A shared upstream instance would
    // append LEAF a second time on the second call, yielding 4.
    assertEquals(firstCount, 3, "Expected MAILBOX_RECEIVE, PIPELINE_BREAKER, LEAF");
    assertEquals(secondCount, firstCount, "calculateStats() must be idempotent for a pipeline-breaker leaf");

    operator.close();
  }

  @Test
  public void shouldHandleDesiredDataSchemaConversionCorrectly() {
    // Given:
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT boolCol, tsCol, boolCol AS newNamedBoolCol FROM tbl");
    DataSchema resultSchema = new DataSchema(new String[]{"boolCol", "tsCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.TIMESTAMP});
    DataSchema desiredSchema =
        new DataSchema(new String[]{"boolCol", "tsCol", "newNamedBoolCol"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.TIMESTAMP, DataSchema.ColumnDataType.BOOLEAN
        });
    List<BaseResultsBlock> dataBlocks = Collections.singletonList(new SelectionResultsBlock(resultSchema,
        Arrays.asList(new Object[]{1, 1660000000000L}, new Object[]{0, 1600000000000L}), queryContext));
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), desiredSchema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then:
    List<Object[]> rows = ((MseBlock.Data) resultBlock).asRowHeap().getRows();
    assertEquals(rows.get(0), new Object[]{1, 1660000000000L, 1});
    assertEquals(rows.get(1), new Object[]{0, 1600000000000L, 0});
    assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading 2 blocks");

    operator.close();
  }

  @Test
  public void shouldReturnMultipleDataBlockThenMetadataBlock() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<BaseResultsBlock> dataBlocks = Arrays.asList(
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2}), queryContext),
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"bar", 3}, new Object[]{"foo", 4}), queryContext));
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock1 = operator.nextBlock();
    MseBlock resultBlock2 = operator.nextBlock();
    MseBlock resultBlock3 = operator.nextBlock();

    // Then:
    List<Object[]> rows1 = ((MseBlock.Data) resultBlock1).asRowHeap().getRows();
    List<Object[]> rows2 = ((MseBlock.Data) resultBlock2).asRowHeap().getRows();
    assertEquals(rows1.get(0), new Object[]{"foo", 1});
    assertEquals(rows1.get(1), new Object[]{"", 2});
    assertEquals(rows2.get(0), new Object[]{"bar", 3});
    assertEquals(rows2.get(1), new Object[]{"foo", 4});
    assertTrue(resultBlock3.isEos(), "Expected EOS after reading 2 blocks");

    operator.close();
  }

  @Test
  public void shouldHandleMultipleRequests() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<BaseResultsBlock> dataBlocks = Arrays.asList(
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2}), queryContext),
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"bar", 3}, new Object[]{"foo", 4}), queryContext));
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(2), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // Then: the 5th block should be EOS
    assertTrue(operator.nextBlock().isData());
    assertTrue(operator.nextBlock().isData());
    assertTrue(operator.nextBlock().isData());
    assertTrue(operator.nextBlock().isData());
    assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading 5 blocks");

    operator.close();
  }

  @Test
  public void shouldGetErrorBlockWhenInstanceResponseContainsError() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<BaseResultsBlock> dataBlocks = Collections.singletonList(
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2}), queryContext));
    InstanceResponseBlock errorBlock = new InstanceResponseBlock();
    errorBlock.addException(QueryErrorCode.QUERY_EXECUTION, "foobar");
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, errorBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then: error block can be returned as first or second block depending on the sequence of the execution
    if (!resultBlock.isError()) {
      assertTrue(operator.nextBlock().isError());
    }

    operator.close();
  }

  @Test
  public void shouldNotErrorOutWhenIncorrectDataSchemaProvidedWithEmptyRowsSelection() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema resultSchema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    DataSchema desiredSchema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<BaseResultsBlock> dataBlocks = Collections.emptyList();
    InstanceResponseBlock emptySelectionResponseBlock =
        new InstanceResponseBlock(new SelectionResultsBlock(resultSchema, Collections.emptyList(), queryContext));
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, emptySelectionResponseBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), desiredSchema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then:
    assertTrue(resultBlock.isEos());

    operator.close();
  }

  @Test
  public void closeMethodInterruptsSseTasks() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<BaseResultsBlock> dataBlocks = Collections.singletonList(
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2}), queryContext));
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafOperator operator =
        spy(new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService));

    _operatorRef.set(operator);

    // When:
    operator.close();

    // Then:
    verify(operator).cancelSseTasks();
  }

  @Test
  public void cancelMethodInterruptsSseTasks() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<BaseResultsBlock> dataBlocks = Collections.singletonList(
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2}), queryContext));
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafOperator operator =
        spy(new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService));

    _operatorRef.set(operator);

    // When:
    operator.cancel(new RuntimeException("test"));

    // Then:
    verify(operator).cancelSseTasks();
  }

  @Test
  public void earlyTerminateMethodInterruptsSseTasks() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<BaseResultsBlock> dataBlocks = Collections.singletonList(
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2}), queryContext));
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafOperator operator =
        spy(new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService));

    _operatorRef.set(operator);

    // When:
    operator.earlyTerminate();

    // Then:
    verify(operator).cancelSseTasks();
  }

  @Test
  public void executionThreadShouldNotBlockOnLastResultsBlockWhenCancelled()
      throws Exception {
    // Given: operator with queue size 1
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");

    Map<String, String> opChainMetadata = new HashMap<>();
    opChainMetadata.put("maxStreamingPendingBlocks", "1");
    opChainMetadata.put("timeoutMs", "100000");
    OpChainExecutionContext context = OperatorTestUtil.getContext(opChainMetadata);
    CountDownLatch resultsBlockAdded = new CountDownLatch(1);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    LeafOperator operator =
        new LeafOperator(context, mockQueryRequests(1), schema, mock(QueryExecutor.class), executorService) {
          @Override
          void execute() {
            try {
              // Fill queue and block on second add
              SelectionResultsBlock dataBlock =
                  new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2}),
                      queryContext);
              // First data block is consumed by the first call of getNextBlock()
              addResultsBlock(dataBlock);
              // Second data block will remain in the blocking queue and block the third data block
              addResultsBlock(dataBlock);
              resultsBlockAdded.countDown();
              addResultsBlock(dataBlock);
            } catch (Exception e) {
              assertTrue(e instanceof InterruptedException);
            }
          }
        };

    // Main thread read the next block to start the execution
    assertTrue(operator.getNextBlock() instanceof MseBlock.Data);

    // Wait for blocking queue to fill up
    resultsBlockAdded.await();

    // Early terminate the operator, which will also clear the queue and interrupt the child
    operator.earlyTerminate();
    assertSame(operator.getNextBlock(), SuccessMseBlock.INSTANCE);

    // Child thread should exit
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

    operator.close();
  }

  @Test
  public void shouldPropagateDistinctEarlyTerminationReason() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT DISTINCT intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    metadataBlock.getResponseMetadata().put(DataTable.MetadataKey.EARLY_TERMINATION_REASON.getName(),
        EarlyTerminationReason.DISTINCT_MAX_ROWS.name());
    QueryExecutor queryExecutor = mockQueryExecutor(Collections.emptyList(), metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading the metadata block");

    // Then:
    StatMap<LeafOperator.StatKey> leafStats = operator.copyStatMaps();
    assertEquals(List.copyOf(leafStats.getStringSet(LeafOperator.StatKey.EARLY_TERMINATION_REASONS)),
        List.of(EarlyTerminationReason.DISTINCT_MAX_ROWS.name()));

    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    MultiStageOperator.Type.LEAF.mergeInto(brokerResponse, leafStats);
    assertEquals(brokerResponse.getEarlyTerminationReasons(), List.of(EarlyTerminationReason.DISTINCT_MAX_ROWS.name()));
    assertTrue(brokerResponse.isPartialResult());
    JsonNode responseJson = JsonUtils.objectToJsonNode(brokerResponse);
    assertEquals(responseJson.path("earlyTerminationReasons").path(0).asText(),
        EarlyTerminationReason.DISTINCT_MAX_ROWS.name());
    assertFalse(responseJson.has("maxRowsInDistinctReached"));
    assertFalse(responseJson.has("maxRowsWithoutChangeInDistinctReached"));
    assertFalse(responseJson.has("maxExecutionTimeInDistinctReached"));
    assertTrue(responseJson.path("partialResult").asBoolean(false));

    operator.close();
  }

  @Test
  public void shouldSkipNoneEarlyTerminationReason() {
    assertSkippedEarlyTerminationReason(EarlyTerminationReason.NONE.name());
  }

  @Test
  public void shouldSkipUnknownEarlyTerminationReason() {
    assertSkippedEarlyTerminationReason("UNKNOWN_REASON");
  }

  @Test
  public void shouldSkipEmptyEarlyTerminationReason() {
    assertSkippedEarlyTerminationReason("");
  }

  @Test
  public void shouldSkipNullEarlyTerminationReason() {
    assertSkippedEarlyTerminationReason(null);
  }

  private void assertSkippedEarlyTerminationReason(@Nullable String earlyTerminationReason) {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT DISTINCT intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    metadataBlock.getResponseMetadata().put(DataTable.MetadataKey.EARLY_TERMINATION_REASON.getName(),
        earlyTerminationReason);
    QueryExecutor queryExecutor = mockQueryExecutor(Collections.emptyList(), metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading the metadata block");

    // Then:
    StatMap<LeafOperator.StatKey> leafStats = operator.copyStatMaps();
    assertTrue(leafStats.getStringSet(LeafOperator.StatKey.EARLY_TERMINATION_REASONS).isEmpty());

    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    MultiStageOperator.Type.LEAF.mergeInto(brokerResponse, leafStats);
    assertTrue(brokerResponse.getEarlyTerminationReasons().isEmpty());
    assertFalse(brokerResponse.isPartialResult());

    operator.close();
  }

  @Test
  public void shouldReturnAggregationResultBlock() {
    // Given:
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*), SUM(intCol) FROM tbl");
    AggregationFunction[] aggFunctions = queryContext.getAggregationFunctions();
    AggregationResultsBlock aggBlock =
        new AggregationResultsBlock(aggFunctions, Arrays.asList(2L, 10.0), queryContext);
    DataSchema schema = aggBlock.getDataSchema();
    List<BaseResultsBlock> dataBlocks = Collections.singletonList(aggBlock);
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then:
    List<Object[]> rows = ((MseBlock.Data) resultBlock).asRowHeap().getRows();
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{2L, 10.0});
    assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading the aggregation block");

    operator.close();
  }

  @Test
  public void shouldReturnGroupByResultBlock() {
    // Given:
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT strCol, COUNT(*) FROM tbl GROUP BY strCol");
    DataSchema schema = new DataSchema(new String[]{"strCol", "count(*)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    List<Record> records = Arrays.asList(new Record(new Object[]{"a", 3L}), new Record(new Object[]{"b", 5L}));
    Table table = mock(Table.class);
    when(table.getDataSchema()).thenReturn(schema);
    when(table.size()).thenReturn(records.size());
    when(table.iterator()).thenAnswer(inv -> records.iterator());
    GroupByResultsBlock groupByBlock = new GroupByResultsBlock(table, queryContext);
    List<BaseResultsBlock> dataBlocks = Collections.singletonList(groupByBlock);
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then:
    List<Object[]> rows = ((MseBlock.Data) resultBlock).asRowHeap().getRows();
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{"a", 3L});
    assertEquals(rows.get(1), new Object[]{"b", 5L});
    assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading the group-by block");

    operator.close();
  }

  @Test
  public void shouldReturnDistinctResultBlock() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT DISTINCT intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    List<Object[]> distinctRows = Arrays.asList(new Object[]{1}, new Object[]{2}, new Object[]{3});
    DistinctTable distinctTable = mock(DistinctTable.class);
    when(distinctTable.getDataSchema()).thenReturn(schema);
    when(distinctTable.size()).thenReturn(distinctRows.size());
    when(distinctTable.getRows()).thenReturn(distinctRows);
    DistinctResultsBlock distinctBlock = new DistinctResultsBlock(distinctTable, queryContext);
    List<BaseResultsBlock> dataBlocks = Collections.singletonList(distinctBlock);
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(dataBlocks, metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then:
    List<Object[]> rows = ((MseBlock.Data) resultBlock).asRowHeap().getRows();
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0), new Object[]{1});
    assertEquals(rows.get(1), new Object[]{2});
    assertEquals(rows.get(2), new Object[]{3});
    assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading the distinct block");

    operator.close();
  }

  @Test
  public void shouldPropagateLiteModeLeafStageLimitReached() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    metadataBlock.getResponseMetadata().put(DataTable.MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName(),
        "true");
    QueryExecutor queryExecutor = mockQueryExecutor(Collections.emptyList(), metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading the metadata block");

    // Then:
    StatMap<LeafOperator.StatKey> leafStats = operator.copyStatMaps();
    assertTrue(leafStats.getBoolean(LeafOperator.StatKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED));

    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    MultiStageOperator.Type.LEAF.mergeInto(brokerResponse, leafStats);
    assertTrue(brokerResponse.isMseLiteLeafStageLimitReached());
    assertTrue(brokerResponse.isPartialResult());
    JsonNode responseJson = JsonUtils.objectToJsonNode(brokerResponse);
    assertTrue(responseJson.path("mseLiteLeafStageLimitReached").asBoolean(false));
    assertTrue(responseJson.path("partialResult").asBoolean(false));

    operator.close();
  }

  @Test
  public void shouldNotSetLiteModeLeafStageLimitWhenNotReached() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    InstanceResponseBlock metadataBlock = new InstanceResponseBlock(new MetadataResultsBlock());
    QueryExecutor queryExecutor = mockQueryExecutor(Collections.emptyList(), metadataBlock);
    LeafOperator operator =
        new LeafOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema, queryExecutor,
            _executorService);
    _operatorRef.set(operator);

    // When:
    assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading the metadata block");

    // Then:
    StatMap<LeafOperator.StatKey> leafStats = operator.copyStatMaps();
    assertFalse(leafStats.getBoolean(LeafOperator.StatKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED));

    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    MultiStageOperator.Type.LEAF.mergeInto(brokerResponse, leafStats);
    assertFalse(brokerResponse.isMseLiteLeafStageLimitReached());
    assertFalse(brokerResponse.isPartialResult());

    operator.close();
  }
}
