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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


// TODO: add tests for Agg / GroupBy / Distinct result blocks
public class LeafStageTransferableBlockOperatorTest {
  private final ExecutorService _executorService = Executors.newCachedThreadPool();
  private final AtomicReference<LeafStageTransferableBlockOperator> _operatorRef = new AtomicReference<>();

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
      LeafStageTransferableBlockOperator operator = _operatorRef.get();
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
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema,
            queryExecutor, _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then:
    List<Object[]> rows = ((MseBlock.Data) resultBlock).asRowHeap().getRows();
    Assert.assertEquals(rows.get(0), new Object[]{"foo", 1});
    Assert.assertEquals(rows.get(1), new Object[]{"", 2});
    Assert.assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading 2 blocks");

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
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1),
            desiredSchema, queryExecutor, _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then:
    List<Object[]> rows = ((MseBlock.Data) resultBlock).asRowHeap().getRows();
    Assert.assertEquals(rows.get(0), new Object[]{1, 1660000000000L, 1});
    Assert.assertEquals(rows.get(1), new Object[]{0, 1600000000000L, 0});
    Assert.assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading 2 blocks");

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
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema,
            queryExecutor, _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock1 = operator.nextBlock();
    MseBlock resultBlock2 = operator.nextBlock();
    MseBlock resultBlock3 = operator.nextBlock();

    // Then:
    List<Object[]> rows1 = ((MseBlock.Data) resultBlock1).asRowHeap().getRows();
    List<Object[]> rows2 = ((MseBlock.Data) resultBlock2).asRowHeap().getRows();
    Assert.assertEquals(rows1.get(0), new Object[]{"foo", 1});
    Assert.assertEquals(rows1.get(1), new Object[]{"", 2});
    Assert.assertEquals(rows2.get(0), new Object[]{"bar", 3});
    Assert.assertEquals(rows2.get(1), new Object[]{"foo", 4});
    Assert.assertTrue(resultBlock3.isEos(), "Expected EOS after reading 2 blocks");

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
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(2), schema,
            queryExecutor, _executorService);
    _operatorRef.set(operator);

    // Then: the 5th block should be EOS
    Assert.assertTrue(operator.nextBlock().isData());
    Assert.assertTrue(operator.nextBlock().isData());
    Assert.assertTrue(operator.nextBlock().isData());
    Assert.assertTrue(operator.nextBlock().isData());
    Assert.assertTrue(operator.nextBlock().isEos(), "Expected EOS after reading 5 blocks");

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
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema,
            queryExecutor, _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then: error block can be returned as first or second block depending on the sequence of the execution
    if (!resultBlock.isError()) {
      Assert.assertTrue(operator.nextBlock().isError());
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
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1),
            desiredSchema, queryExecutor, _executorService);
    _operatorRef.set(operator);

    // When:
    MseBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertTrue(resultBlock.isEos());

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
    LeafStageTransferableBlockOperator operator =
        spy(
            new LeafStageTransferableBlockOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema,
                queryExecutor, _executorService)
        );

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
    LeafStageTransferableBlockOperator operator =
        spy(
            new LeafStageTransferableBlockOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema,
                queryExecutor, _executorService)
        );

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
    LeafStageTransferableBlockOperator operator =
        spy(
            new LeafStageTransferableBlockOperator(OperatorTestUtil.getTracingContext(), mockQueryRequests(1), schema,
                queryExecutor, _executorService)
        );

    _operatorRef.set(operator);

    // When:
    operator.earlyTerminate();

    // Then:
    verify(operator).cancelSseTasks();
  }
}
