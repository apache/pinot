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

import com.google.common.annotations.VisibleForTesting;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


// TODO: add tests for Agg / GroupBy / Distinct result blocks
public class LeafStageTransferableBlockOperatorTest {
  private AutoCloseable _mocks;

  @Mock
  private VirtualServerAddress _serverAddress;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldReturnDataBlockThenMetadataBlock() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(new InstanceResponseBlock(
        new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2})), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(resultsBlockList), getStaticServerQueryRequests(resultsBlockList.size()), schema);

    // When:
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(resultBlock.getContainer().get(0), new Object[]{"foo", 1});
    Assert.assertEquals(resultBlock.getContainer().get(1), new Object[]{"", 2});
    Assert.assertTrue(operator.nextBlock().isEndOfStreamBlock(), "Expected EOS after reading two rows");
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
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(new InstanceResponseBlock(
        new SelectionResultsBlock(resultSchema,
            Arrays.asList(new Object[]{1, 1660000000000L}, new Object[]{0, 1600000000000L})), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(resultsBlockList), getStaticServerQueryRequests(resultsBlockList.size()),
            desiredSchema);

    // When:
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(resultBlock.getContainer().get(0), new Object[]{true, new Timestamp(1660000000000L), true});
    Assert.assertEquals(resultBlock.getContainer().get(1), new Object[]{false, new Timestamp(1600000000000L), false});
    Assert.assertTrue(operator.nextBlock().isEndOfStreamBlock(), "Expected EOS after reading two rows");
  }

  @Test
  public void shouldHandleCanonicalizationCorrectly() {
    // TODO: not all stored types are supported, add additional datatype when they are supported.
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT boolCol, tsCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"boolCol", "tsCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.TIMESTAMP});
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(new InstanceResponseBlock(
        new SelectionResultsBlock(schema,
            Arrays.asList(new Object[]{1, 1660000000000L}, new Object[]{0, 1600000000000L})), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(resultsBlockList), getStaticServerQueryRequests(resultsBlockList.size()), schema);

    // When:
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(resultBlock.getContainer().get(0), new Object[]{true, new Timestamp(1660000000000L)});
    Assert.assertEquals(resultBlock.getContainer().get(1), new Object[]{false, new Timestamp(1600000000000L)});
    Assert.assertTrue(operator.nextBlock().isEndOfStreamBlock(), "Expected EOS after reading two rows");
  }

  @Test
  public void shouldReturnMultipleDataBlockThenMetadataBlock() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<InstanceResponseBlock> resultsBlockList = Arrays.asList(new InstanceResponseBlock(
            new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2})),
            queryContext),
        new InstanceResponseBlock(
            new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"bar", 3}, new Object[]{"foo", 4})),
            queryContext),
        new InstanceResponseBlock(new SelectionResultsBlock(schema, Collections.emptyList()), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(resultsBlockList), getStaticServerQueryRequests(resultsBlockList.size()), schema);

    // When:
    TransferableBlock resultBlock1 = operator.nextBlock();
    TransferableBlock resultBlock2 = operator.nextBlock();
    TransferableBlock resultBlock3 = operator.nextBlock();

    // Then:
    Assert.assertEquals(resultBlock1.getContainer().get(0), new Object[]{"foo", 1});
    Assert.assertEquals(resultBlock1.getContainer().get(1), new Object[]{"", 2});
    Assert.assertEquals(resultBlock2.getContainer().get(0), new Object[]{"bar", 3});
    Assert.assertEquals(resultBlock2.getContainer().get(1), new Object[]{"foo", 4});
    Assert.assertEquals(resultBlock3.getContainer().size(), 0);
    Assert.assertTrue(operator.nextBlock().isEndOfStreamBlock(), "Expected EOS after reading two rows");
  }

  @Test
  public void shouldGetErrorBlockWhenInstanceResponseContainsError() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    InstanceResponseBlock errorBlock = new InstanceResponseBlock();
    errorBlock.addException(QueryException.QUERY_EXECUTION_ERROR.getErrorCode(), "foobar");
    List<InstanceResponseBlock> resultsBlockList = Arrays.asList(new InstanceResponseBlock(
            new SelectionResultsBlock(schema, Arrays.asList(new Object[]{"foo", 1}, new Object[]{"", 2})),
            queryContext),
        errorBlock,
        new InstanceResponseBlock(new SelectionResultsBlock(schema, Collections.emptyList()), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(resultsBlockList), getStaticServerQueryRequests(resultsBlockList.size()), schema);

    // When:
    TransferableBlock resultBlock = operator.nextBlock();
    // Then:
    Assert.assertEquals(resultBlock.getContainer().get(0), new Object[]{"foo", 1});
    Assert.assertEquals(resultBlock.getContainer().get(1), new Object[]{"", 2});

    // When:
    resultBlock = operator.nextBlock();
    Assert.assertTrue(resultBlock.isErrorBlock());
  }

  @Test
  public void shouldReorderWhenQueryContextAskForNotInOrderGroupByAsDistinct() {
    // Given:
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT intCol, strCol FROM tbl GROUP BY strCol, intCol");
    // result schema doesn't match with DISTINCT columns using GROUP BY.
    DataSchema schema = new DataSchema(new String[]{"intCol", "strCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(new InstanceResponseBlock(
        new DistinctResultsBlock(new DistinctTable(schema,
            Arrays.asList(new Record(new Object[]{1, "foo"}), new Record(new Object[]{2, "bar"})))), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(resultsBlockList), getStaticServerQueryRequests(resultsBlockList.size()), schema);

    // When:
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(resultBlock.getContainer().get(0), new Object[]{1, "foo"});
    Assert.assertEquals(resultBlock.getContainer().get(1), new Object[]{2, "bar"});
  }

  @Test
  public void shouldParsedBlocksSuccessfullyWithDistinctQuery() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT DISTINCT strCol, intCol FROM tbl");
    // result schema doesn't match with DISTINCT columns using GROUP BY.
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(new InstanceResponseBlock(
        new DistinctResultsBlock(new DistinctTable(schema,
            Arrays.asList(new Record(new Object[]{"foo", 1}), new Record(new Object[]{"bar", 2})))), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(resultsBlockList), getStaticServerQueryRequests(resultsBlockList.size()), schema);

    // When:
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(resultBlock.getContainer().get(0), new Object[]{"foo", 1});
    Assert.assertEquals(resultBlock.getContainer().get(1), new Object[]{"bar", 2});
  }

  @Test
  public void shouldReorderWhenQueryContextAskForGroupByOutOfOrder() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT intCol, count(*), sum(doubleCol), strCol FROM tbl GROUP BY strCol, intCol");
    // result schema doesn't match with columns ordering using GROUP BY, this should not occur.
    DataSchema schema =
        new DataSchema(new String[]{"intCol", "count(*)", "sum(doubleCol)", "strCol"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG,
            DataSchema.ColumnDataType.STRING
        });
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(
        new InstanceResponseBlock(new GroupByResultsBlock(schema, Collections.emptyList()), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(resultsBlockList), getStaticServerQueryRequests(resultsBlockList.size()), schema);

    // When:
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertFalse(resultBlock.isErrorBlock());
  }

  @Test
  public void shouldNotErrorOutWhenQueryContextAskForGroupByOutOfOrderWithHaving() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol, count(*), "
        + "sum(doubleCol) FROM tbl GROUP BY strCol, intCol HAVING sum(doubleCol) < 10 AND count(*) > 0");
    // result schema contains duplicate reference from agg and having. it will repeat itself.
    DataSchema schema = new DataSchema(new String[]{"strCol", "intCol", "count(*)", "sum(doubleCol)", "sum(doubleCol)"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG
        });
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(
        new InstanceResponseBlock(new GroupByResultsBlock(schema, Collections.emptyList()), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(resultsBlockList), getStaticServerQueryRequests(resultsBlockList.size()), schema);

    // When:
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertFalse(resultBlock.isErrorBlock());
  }

  @Test
  public void shouldNotErrorOutWhenDealingWithAggregationResults() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT count(*), sum(doubleCol) FROM tbl");
    // result schema doesn't match with DISTINCT columns using GROUP BY.
    DataSchema schema = new DataSchema(new String[]{"count_star", "sum(doubleCol)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG});
    List<InstanceResponseBlock> resultsBlockList = Collections.singletonList(new InstanceResponseBlock(
        new AggregationResultsBlock(queryContext.getAggregationFunctions(), Collections.emptyList()), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(resultsBlockList), getStaticServerQueryRequests(resultsBlockList.size()), schema);

    // When:
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertFalse(resultBlock.isErrorBlock());
  }

  @Test
  public void shouldNotErrorOutWhenIncorrectDataSchemaProvidedWithEmptyRowsSelection() {
    // Given:
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl");
    DataSchema resultSchema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    DataSchema desiredSchema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});

    // When:
    List<InstanceResponseBlock> responseBlockList = Collections.singletonList(
        new InstanceResponseBlock(new SelectionResultsBlock(resultSchema, Collections.emptyList()), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(responseBlockList), getStaticServerQueryRequests(responseBlockList.size()),
            desiredSchema);
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(resultBlock.getContainer().size(), 0);
    Assert.assertEquals(resultBlock.getDataSchema(), desiredSchema);
  }

  @Test
  public void shouldNotErrorOutWhenIncorrectDataSchemaProvidedWithEmptyRowsDistinct() {
    // Given:
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT strCol, intCol FROM tbl GROUP BY strCol, intCol");
    DataSchema resultSchema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    DataSchema desiredSchema = new DataSchema(new String[]{"strCol", "intCol"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});

    // When:
    List<InstanceResponseBlock> responseBlockList = Collections.singletonList(
        new InstanceResponseBlock(new DistinctResultsBlock(new DistinctTable(resultSchema, Collections.emptyList())),
            queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(responseBlockList), getStaticServerQueryRequests(responseBlockList.size()),
            desiredSchema);
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(resultBlock.getContainer().size(), 0);
    Assert.assertEquals(resultBlock.getDataSchema(), desiredSchema);
  }

  @Test
  public void shouldNotErrorOutWhenIncorrectDataSchemaProvidedWithEmptyRowsGroupBy() {
    // Given:
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT strCol, SUM(intCol) FROM tbl GROUP BY strCol");
    DataSchema resultSchema = new DataSchema(new String[]{"strCol", "SUM(intCol)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    DataSchema desiredSchema = new DataSchema(new String[]{"strCol", "SUM(intCol)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});

    // When:
    List<InstanceResponseBlock> responseBlockList = Collections.singletonList(
        new InstanceResponseBlock(new GroupByResultsBlock(resultSchema, Collections.emptyList()), queryContext));
    LeafStageTransferableBlockOperator operator =
        new LeafStageTransferableBlockOperator(OperatorTestUtil.getDefaultContext(),
            getStaticBlockProcessor(responseBlockList), getStaticServerQueryRequests(responseBlockList.size()),
            desiredSchema);
    TransferableBlock resultBlock = operator.nextBlock();

    // Then:
    Assert.assertEquals(resultBlock.getContainer().size(), 0);
    Assert.assertEquals(resultBlock.getDataSchema(), desiredSchema);
  }

  @VisibleForTesting
  static Function<ServerQueryRequest, InstanceResponseBlock> getStaticBlockProcessor(
      List<InstanceResponseBlock> resultBlockList) {
    return new StaticBlockProcessor(resultBlockList)::process;
  }

  static List<ServerQueryRequest> getStaticServerQueryRequests(int count) {
    List<ServerQueryRequest> staticMockRequests = new ArrayList<>();
    while (count > 0) {
      staticMockRequests.add(mock(ServerQueryRequest.class));
      count--;
    }
    return staticMockRequests;
  }

  private static class StaticBlockProcessor {
    private final List<InstanceResponseBlock> _resultBlockList;
    private int _currentIdx;

    StaticBlockProcessor(List<InstanceResponseBlock> resultBlockList) {
      _resultBlockList = resultBlockList;
      _currentIdx = 0;
    }

    public InstanceResponseBlock process(ServerQueryRequest request) {
      return _resultBlockList.get(_currentIdx++);
    }
  }
}
