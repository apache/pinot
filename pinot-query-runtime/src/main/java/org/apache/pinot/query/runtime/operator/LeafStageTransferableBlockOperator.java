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
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionValue;


/**
 * Leaf-stage transfer block operator is used to wrap around the leaf stage process results. They are passed to the
 * Pinot server to execute query thus only one {@link DataTable} were returned. However, to conform with the
 * intermediate stage operators. An additional {@link MetadataBlock} needs to be transferred after the data block.
 *
 * <p>In order to achieve this:
 * <ul>
 *   <li>The leaf-stage result is split into data payload block and metadata payload block.</li>
 *   <li>In case the leaf-stage result contains error or only metadata, we skip the data payload block.</li>
 *   <li>Leaf-stage result blocks are in the {@link ColumnDataType#getStoredColumnDataTypes()} format
 *       thus requires canonicalization.</li>
 * </ul>
 */
public class LeafStageTransferableBlockOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "LEAF_STAGE_TRANSFER_OPERATOR";

  // Use a special results block to indicate that this is the last results block
  private static final MetadataResultsBlock LAST_RESULTS_BLOCK = new MetadataResultsBlock();

  private final List<ServerQueryRequest> _requests;
  private final DataSchema _dataSchema;
  private final QueryExecutor _queryExecutor;
  private final ExecutorService _executorService;

  // Use a limit-sized BlockingQueue to store the results blocks and apply back pressure to the single-stage threads
  private final BlockingQueue<BaseResultsBlock> _blockingQueue;

  private Future<Void> _executionFuture;
  private volatile Map<Integer, String> _exceptions;
  private volatile Map<String, String> _executionStats;

  public LeafStageTransferableBlockOperator(OpChainExecutionContext context, List<ServerQueryRequest> requests,
      DataSchema dataSchema, QueryExecutor queryExecutor, ExecutorService executorService) {
    super(context);
    int numRequests = requests.size();
    Preconditions.checkArgument(numRequests == 1 || numRequests == 2, "Expected 1 or 2 requests, got: %s", numRequests);
    _requests = requests;
    _dataSchema = dataSchema;
    _queryExecutor = queryExecutor;
    _executorService = executorService;
    Integer maxStreamingPendingBlocks = QueryOptionsUtils.getMaxStreamingPendingBlocks(context.getOpChainMetadata());
    _blockingQueue = new ArrayBlockingQueue<>(maxStreamingPendingBlocks != null ? maxStreamingPendingBlocks
        : QueryOptionValue.DEFAULT_MAX_STREAMING_PENDING_BLOCKS);
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_executionFuture == null) {
      _executionFuture = startExecution();
    }
    try {
      BaseResultsBlock resultsBlock =
          _blockingQueue.poll(_context.getDeadlineMs() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      if (resultsBlock == null) {
        throw new TimeoutException("Timed out waiting for results block");
      }
      // Terminate when receiving exception block
      Map<Integer, String> exceptions = _exceptions;
      if (exceptions != null) {
        return TransferableBlockUtils.getErrorTransferableBlock(exceptions);
      }
      if (_isEarlyTerminated || resultsBlock == LAST_RESULTS_BLOCK) {
        return constructMetadataBlock();
      } else {
        // Regular data block
        return composeTransferableBlock(resultsBlock, _dataSchema);
      }
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private TransferableBlock constructMetadataBlock() {
    // All data blocks have been returned. Record the stats and return EOS.
    Map<String, String> executionStats = _executionStats;
    if (executionStats != null) {
      OperatorStats operatorStats = _opChainStats.getOperatorStats(_context, getOperatorId());
      operatorStats.recordExecutionStats(executionStats);
    }
    return TransferableBlockUtils.getEndOfStreamTransferableBlock();
  }

  private Future<Void> startExecution() {
    ResultsBlockConsumer resultsBlockConsumer = new ResultsBlockConsumer();
    return _executorService.submit(() -> {
      try {
        if (_requests.size() == 1) {
          ServerQueryRequest request = _requests.get(0);
          InstanceResponseBlock instanceResponseBlock =
              _queryExecutor.execute(request, _executorService, resultsBlockConsumer);
          // TODO: Revisit if we should treat all exceptions as query failure. Currently MERGE_RESPONSE_ERROR and
          //       SERVER_SEGMENT_MISSING_ERROR are counted as query failure.
          Map<Integer, String> exceptions = instanceResponseBlock.getExceptions();
          if (!exceptions.isEmpty()) {
            _exceptions = exceptions;
          } else {
            // NOTE: Instance response block might contain data (not metadata only) when all the segments are pruned.
            //       Add the results block if it contains data.
            BaseResultsBlock resultsBlock = instanceResponseBlock.getResultsBlock();
            if (resultsBlock != null && resultsBlock.getNumRows() > 0) {
              addResultsBlock(resultsBlock);
            }
            // Collect the execution stats
            _executionStats = instanceResponseBlock.getResponseMetadata();
          }
        } else {
          assert _requests.size() == 2;
          Future<?>[] futures = new Future[2];
          CountDownLatch latch = new CountDownLatch(2);
          for (int i = 0; i < 2; i++) {
            ServerQueryRequest request = _requests.get(i);
            futures[i] = _executorService.submit(() -> {
              try {
                InstanceResponseBlock instanceResponseBlock =
                    _queryExecutor.execute(request, _executorService, resultsBlockConsumer);
                Map<Integer, String> exceptions = instanceResponseBlock.getExceptions();
                if (!exceptions.isEmpty()) {
                  // Drain the latch when receiving exception block and not wait for the other thread to finish
                  _exceptions = exceptions;
                  latch.countDown();
                } else {
                  // NOTE: Instance response block might contain data (not metadata only) when all the segments are
                  //       pruned. Add the results block if it contains data.
                  BaseResultsBlock resultsBlock = instanceResponseBlock.getResultsBlock();
                  if (resultsBlock != null && resultsBlock.getNumRows() > 0) {
                    addResultsBlock(resultsBlock);
                  }
                  // Collect the execution stats
                  Map<String, String> executionStats = instanceResponseBlock.getResponseMetadata();
                  synchronized (LeafStageTransferableBlockOperator.this) {
                    if (_executionStats == null) {
                      _executionStats = executionStats;
                    } else {
                      aggregateExecutionStats(_executionStats, executionStats);
                    }
                  }
                }
                return null;
              } finally {
                latch.countDown();
              }
            });
          }
          try {
            if (!latch.await(_context.getDeadlineMs() - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
              throw new TimeoutException("Timed out waiting for leaf stage to finish");
            }
            // Propagate the exception thrown by the leaf stage
            for (Future<?> future : futures) {
              future.get();
            }
          } finally {
            for (Future<?> future : futures) {
              future.cancel(true);
            }
          }
        }
        return null;
      } finally {
        // Always add the last results block to mark the end of the execution
        addResultsBlock(LAST_RESULTS_BLOCK);
      }
    });
  }

  @VisibleForTesting
  void addResultsBlock(BaseResultsBlock resultsBlock)
      throws InterruptedException, TimeoutException {
    if (!_blockingQueue.offer(resultsBlock, _context.getDeadlineMs() - System.currentTimeMillis(),
        TimeUnit.MILLISECONDS)) {
      throw new TimeoutException("Timed out waiting to add results block");
    }
  }

  // TODO: Revisit the stats aggregation logic
  private void aggregateExecutionStats(Map<String, String> stats1, Map<String, String> stats2) {
    for (Map.Entry<String, String> entry : stats2.entrySet()) {
      String k2 = entry.getKey();
      String v2 = entry.getValue();
      stats1.compute(k2, (k1, v1) -> {
        if (v1 == null) {
          return v2;
        }
        try {
          return Long.toString(Long.parseLong(v1) + Long.parseLong(v2));
        } catch (Exception e) {
          return v1 + "\n" + v2;
        }
      });
    }
  }

  /**
   * Leaf stage operators should always collect stats for the tables used in queries
   * Otherwise the Broker response will just contain zeros for every stat value
   */
  @Override
  protected boolean shouldCollectStats() {
    return true;
  }

  @Override
  public void close() {
    if (_executionFuture != null) {
      _executionFuture.cancel(true);
    }
  }

  /**
   * Composes the {@link TransferableBlock} from the {@link BaseResultsBlock} returned from single-stage engine. It
   * converts the data types of the results to conform with the desired data schema asked by the multi-stage engine.
   */
  private static TransferableBlock composeTransferableBlock(BaseResultsBlock resultsBlock,
      DataSchema desiredDataSchema) {
    if (resultsBlock instanceof SelectionResultsBlock) {
      return composeSelectTransferableBlock((SelectionResultsBlock) resultsBlock, desiredDataSchema);
    } else {
      return composeDirectTransferableBlock(resultsBlock, desiredDataSchema);
    }
  }

  /**
   * For selection, we need to check if the columns are in order. If not, we need to re-arrange the columns.
   */
  private static TransferableBlock composeSelectTransferableBlock(SelectionResultsBlock resultsBlock,
      DataSchema desiredDataSchema) {
    int[] columnIndices = getColumnIndices(resultsBlock);
    if (!inOrder(columnIndices)) {
      return composeColumnIndexedTransferableBlock(resultsBlock, desiredDataSchema, columnIndices);
    } else {
      return composeDirectTransferableBlock(resultsBlock, desiredDataSchema);
    }
  }

  private static int[] getColumnIndices(SelectionResultsBlock resultsBlock) {
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assert dataSchema != null;
    String[] columnNames = dataSchema.getColumnNames();
    Object2IntOpenHashMap<String> columnIndexMap = new Object2IntOpenHashMap<>(columnNames.length);
    for (int i = 0; i < columnNames.length; i++) {
      columnIndexMap.put(columnNames[i], i);
    }
    QueryContext queryContext = resultsBlock.getQueryContext();
    assert queryContext != null;
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    int numSelectExpressions = selectExpressions.size();
    int[] columnIndices = new int[numSelectExpressions];
    for (int i = 0; i < numSelectExpressions; i++) {
      columnIndices[i] = columnIndexMap.getInt(selectExpressions.get(i).toString());
    }
    return columnIndices;
  }

  private static boolean inOrder(int[] columnIndices) {
    for (int i = 0; i < columnIndices.length; i++) {
      if (columnIndices[i] != i) {
        return false;
      }
    }
    return true;
  }

  private static TransferableBlock composeColumnIndexedTransferableBlock(BaseResultsBlock block,
      DataSchema outputDataSchema, int[] columnIndices) {
    List<Object[]> resultRows = block.getRows();
    DataSchema inputDataSchema = block.getDataSchema();
    assert resultRows != null && inputDataSchema != null;
    ColumnDataType[] inputStoredTypes = inputDataSchema.getStoredColumnDataTypes();
    ColumnDataType[] outputStoredTypes = outputDataSchema.getStoredColumnDataTypes();
    List<Object[]> convertedRows = new ArrayList<>(resultRows.size());
    boolean needConvert = false;
    int numColumns = columnIndices.length;
    for (int colId = 0; colId < numColumns; colId++) {
      if (inputStoredTypes[columnIndices[colId]] != outputStoredTypes[colId]) {
        needConvert = true;
        break;
      }
    }
    if (needConvert) {
      for (Object[] row : resultRows) {
        convertedRows.add(reorderAndConvertRow(row, inputStoredTypes, outputStoredTypes, columnIndices));
      }
    } else {
      for (Object[] row : resultRows) {
        convertedRows.add(reorderRow(row, columnIndices));
      }
    }
    return new TransferableBlock(convertedRows, outputDataSchema, DataBlock.Type.ROW);
  }

  private static Object[] reorderAndConvertRow(Object[] row, ColumnDataType[] inputStoredTypes,
      ColumnDataType[] outputStoredTypes, int[] columnIndices) {
    int numColumns = columnIndices.length;
    Object[] resultRow = new Object[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      int inputColId = columnIndices[colId];
      Object value = row[inputColId];
      if (value != null) {
        if (inputStoredTypes[inputColId] != outputStoredTypes[colId]) {
          resultRow[colId] = TypeUtils.convert(value, outputStoredTypes[colId]);
        } else {
          resultRow[colId] = value;
        }
      }
    }
    return resultRow;
  }

  private static Object[] reorderRow(Object[] row, int[] columnIndices) {
    int numColumns = columnIndices.length;
    Object[] resultRow = new Object[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      resultRow[colId] = row[columnIndices[colId]];
    }
    return resultRow;
  }

  private static TransferableBlock composeDirectTransferableBlock(BaseResultsBlock block, DataSchema outputDataSchema) {
    List<Object[]> resultRows = block.getRows();
    DataSchema inputDataSchema = block.getDataSchema();
    assert resultRows != null && inputDataSchema != null;
    ColumnDataType[] inputStoredTypes = inputDataSchema.getStoredColumnDataTypes();
    ColumnDataType[] outputStoredTypes = outputDataSchema.getStoredColumnDataTypes();
    if (!Arrays.equals(inputStoredTypes, outputStoredTypes)) {
      for (Object[] row : resultRows) {
        convertRow(row, inputStoredTypes, outputStoredTypes);
      }
    }
    return new TransferableBlock(resultRows, outputDataSchema, DataBlock.Type.ROW);
  }

  public static void convertRow(Object[] row, ColumnDataType[] inputStoredTypes, ColumnDataType[] outputStoredTypes) {
    int numColumns = row.length;
    for (int colId = 0; colId < numColumns; colId++) {
      Object value = row[colId];
      if (value != null && inputStoredTypes[colId] != outputStoredTypes[colId]) {
        row[colId] = TypeUtils.convert(value, outputStoredTypes[colId]);
      }
    }
  }

  private class ResultsBlockConsumer implements ResultsBlockStreamer {

    @Override
    public void send(BaseResultsBlock block)
        throws InterruptedException, TimeoutException {
      addResultsBlock(block);
    }
  }
}
