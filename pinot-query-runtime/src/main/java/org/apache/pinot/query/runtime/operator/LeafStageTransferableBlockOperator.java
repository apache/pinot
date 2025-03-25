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
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExplainV2ResultBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.plan.ExplainInfo;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.logger.ServerQueryLogger;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.ExplainMode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  private static final Logger LOGGER = LoggerFactory.getLogger(LeafStageTransferableBlockOperator.class);
  private static final String EXPLAIN_NAME = "LEAF_STAGE_TRANSFER_OPERATOR";

  // Use a special results block to indicate that this is the last results block
  private static final MetadataResultsBlock LAST_RESULTS_BLOCK = new MetadataResultsBlock();

  private final List<ServerQueryRequest> _requests;
  private final DataSchema _dataSchema;
  private final QueryExecutor _queryExecutor;
  private final ExecutorService _executorService;

  // Use a limit-sized BlockingQueue to store the results blocks and apply back pressure to the single-stage threads
  private final BlockingQueue<BaseResultsBlock> _blockingQueue;

  @Nullable
  private volatile Future<Void> _executionFuture;
  private volatile Map<Integer, String> _exceptions;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

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
    String tableName = context.getLeafStageContext().getStagePlan().getStageMetadata().getTableName();
    _statMap.merge(StatKey.TABLE, tableName);
  }

  public List<ServerQueryRequest> getRequests() {
    return _requests;
  }

  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  public MultiStageQueryStats getQueryStats() {
    return MultiStageQueryStats.createLeaf(_context.getStageId(), _statMap);
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  public Type getOperatorType() {
    return Type.LEAF;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
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
  protected TransferableBlock getNextBlock()
      throws InterruptedException, TimeoutException {
    if (_executionFuture == null) {
      _executionFuture = startExecution();
    }
    if (_isEarlyTerminated) {
      return constructMetadataBlock();
    }
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
    if (resultsBlock == LAST_RESULTS_BLOCK) {
      return constructMetadataBlock();
    } else {
      // Regular data block
      return composeTransferableBlock(resultsBlock);
    }
  }

  @Override
  protected void earlyTerminate() {
    super.earlyTerminate();
    cancelSseTasks();
  }

  @Override
  public void cancel(Throwable e) {
    super.cancel(e);
    cancelSseTasks();
  }

  @VisibleForTesting
  protected void cancelSseTasks() {
    Future<Void> executionFuture = _executionFuture;
    if (executionFuture != null) {
      executionFuture.cancel(true);
    }
  }

  private void mergeExecutionStats(@Nullable Map<String, String> executionStats) {
    if (executionStats != null) {
      for (Map.Entry<String, String> entry : executionStats.entrySet()) {
        DataTable.MetadataKey key = DataTable.MetadataKey.getByName(entry.getKey());
        if (key == null) {
          LOGGER.debug("Skipping unknown execution stat: {}", entry.getKey());
          continue;
        }
        switch (key) {
          case UNKNOWN:
            LOGGER.debug("Skipping unknown execution stat: {}", entry.getKey());
            break;
          case TABLE:
            _statMap.merge(StatKey.TABLE, entry.getValue());
            break;
          case NUM_DOCS_SCANNED:
            _statMap.merge(StatKey.NUM_DOCS_SCANNED, Long.parseLong(entry.getValue()));
            break;
          case NUM_ENTRIES_SCANNED_IN_FILTER:
            _statMap.merge(StatKey.NUM_ENTRIES_SCANNED_IN_FILTER, Long.parseLong(entry.getValue()));
            break;
          case NUM_ENTRIES_SCANNED_POST_FILTER:
            _statMap.merge(StatKey.NUM_ENTRIES_SCANNED_POST_FILTER, Long.parseLong(entry.getValue()));
            break;
          case NUM_SEGMENTS_QUERIED:
            _statMap.merge(StatKey.NUM_SEGMENTS_QUERIED, Integer.parseInt(entry.getValue()));
            break;
          case NUM_SEGMENTS_PROCESSED:
            _statMap.merge(StatKey.NUM_SEGMENTS_PROCESSED, Integer.parseInt(entry.getValue()));
            break;
          case NUM_SEGMENTS_MATCHED:
            _statMap.merge(StatKey.NUM_SEGMENTS_MATCHED, Integer.parseInt(entry.getValue()));
            break;
          case NUM_CONSUMING_SEGMENTS_QUERIED:
            _statMap.merge(StatKey.NUM_CONSUMING_SEGMENTS_QUERIED, Integer.parseInt(entry.getValue()));
            break;
          case MIN_CONSUMING_FRESHNESS_TIME_MS:
            _statMap.merge(StatKey.MIN_CONSUMING_FRESHNESS_TIME_MS, Long.parseLong(entry.getValue()));
            break;
          case TOTAL_DOCS:
            _statMap.merge(StatKey.TOTAL_DOCS, Long.parseLong(entry.getValue()));
            break;
          case NUM_GROUPS_LIMIT_REACHED:
            _statMap.merge(StatKey.NUM_GROUPS_LIMIT_REACHED, Boolean.parseBoolean(entry.getValue()));
            break;
          case TIME_USED_MS:
            _statMap.merge(StatKey.EXECUTION_TIME_MS, Long.parseLong(entry.getValue()));
            break;
          case TRACE_INFO:
            LOGGER.debug("Skipping trace info: {}", entry.getValue());
            break;
          case REQUEST_ID:
            LOGGER.debug("Skipping request ID: {}", entry.getValue());
            break;
          case NUM_RESIZES:
            _statMap.merge(StatKey.NUM_RESIZES, Integer.parseInt(entry.getValue()));
            break;
          case RESIZE_TIME_MS:
            _statMap.merge(StatKey.RESIZE_TIME_MS, Long.parseLong(entry.getValue()));
            break;
          case THREAD_CPU_TIME_NS:
            _statMap.merge(StatKey.THREAD_CPU_TIME_NS, Long.parseLong(entry.getValue()));
            break;
          case SYSTEM_ACTIVITIES_CPU_TIME_NS:
            _statMap.merge(StatKey.SYSTEM_ACTIVITIES_CPU_TIME_NS, Long.parseLong(entry.getValue()));
            break;
          case RESPONSE_SER_CPU_TIME_NS:
            _statMap.merge(StatKey.RESPONSE_SER_CPU_TIME_NS, Long.parseLong(entry.getValue()));
            break;
          case NUM_SEGMENTS_PRUNED_BY_SERVER:
            _statMap.merge(StatKey.NUM_SEGMENTS_PRUNED_BY_SERVER, Integer.parseInt(entry.getValue()));
            break;
          case NUM_SEGMENTS_PRUNED_INVALID:
            _statMap.merge(StatKey.NUM_SEGMENTS_PRUNED_INVALID, Integer.parseInt(entry.getValue()));
            break;
          case NUM_SEGMENTS_PRUNED_BY_LIMIT:
            _statMap.merge(StatKey.NUM_SEGMENTS_PRUNED_BY_LIMIT, Integer.parseInt(entry.getValue()));
            break;
          case NUM_SEGMENTS_PRUNED_BY_VALUE:
            _statMap.merge(StatKey.NUM_SEGMENTS_PRUNED_BY_VALUE, Integer.parseInt(entry.getValue()));
            break;
          case EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS:
            LOGGER.debug("Skipping empty filter segments: {}", entry.getValue());
            break;
          case EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS:
            LOGGER.debug("Skipping match all filter segments: {}", entry.getValue());
            break;
          case NUM_CONSUMING_SEGMENTS_PROCESSED:
            _statMap.merge(StatKey.NUM_CONSUMING_SEGMENTS_PROCESSED, Integer.parseInt(entry.getValue()));
            break;
          case NUM_CONSUMING_SEGMENTS_MATCHED:
            _statMap.merge(StatKey.NUM_CONSUMING_SEGMENTS_MATCHED, Integer.parseInt(entry.getValue()));
            break;
          default: {
            throw new IllegalArgumentException("Unhandled V1 execution stat: " + entry.getKey());
          }
        }
      }
    }
  }

  private TransferableBlock constructMetadataBlock() {
    MultiStageQueryStats multiStageQueryStats = MultiStageQueryStats.createLeaf(_context.getStageId(), _statMap);
    return TransferableBlockUtils.getEndOfStreamTransferableBlock(multiStageQueryStats);
  }

  public ExplainedNode explain() {
    Preconditions.checkState(_requests.stream()
        .allMatch(request -> request.getQueryContext().getExplain() == ExplainMode.NODE),
        "All requests must have explain mode set to ExplainMode.NODE");

    if (_executionFuture == null) {
      _executionFuture = startExecution();
    }

    List<PlanNode> childNodes = new ArrayList<>();
    while (true) {
      BaseResultsBlock resultsBlock;
      try {
        long timeout = _context.getDeadlineMs() - System.currentTimeMillis();
        resultsBlock = _blockingQueue.poll(timeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for results block", e);
      }
      if (resultsBlock == null) {
        throw new RuntimeException("Timed out waiting for results block");
      }
      // Terminate when receiving exception block
      Map<Integer, String> exceptions = _exceptions;
      if (exceptions != null) {
        throw new RuntimeException("Received exception block: " + exceptions);
      }
      if (_isEarlyTerminated || resultsBlock == LAST_RESULTS_BLOCK) {
        break;
      } else if (!(resultsBlock instanceof ExplainV2ResultBlock)) {
        throw new IllegalArgumentException("Expected ExplainV2ResultBlock, got: " + resultsBlock.getClass().getName());
      } else {
        ExplainV2ResultBlock block = (ExplainV2ResultBlock) resultsBlock;
        for (ExplainInfo physicalPlan : block.getPhysicalPlans()) {
          childNodes.add(asNode(physicalPlan));
        }
      }
    }
    String tableName = _context.getStageMetadata().getTableName();
    Map<String, Plan.ExplainNode.AttributeValue> attributes;
    if (tableName == null) { // this should never happen, but let's be paranoid to never fail
      attributes = Collections.emptyMap();
    } else {
      attributes = Collections.singletonMap("table", Plan.ExplainNode.AttributeValue.newBuilder()
          .setString(tableName)
          .build());
    }
    return new ExplainedNode(_context.getStageId(), _dataSchema, null, childNodes,
        "LeafStageCombineOperator", attributes);
  }

  private ExplainedNode asNode(ExplainInfo info) {
    int size = info.getInputs().size();
    List<PlanNode> inputs = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      inputs.add(asNode(info.getInputs().get(i)));
    }

    return new ExplainedNode(_context.getStageId(), _dataSchema, null, inputs, info.getTitle(),
        info.getAttributes());
  }

  private Future<Void> startExecution() {
    ResultsBlockConsumer resultsBlockConsumer = new ResultsBlockConsumer();
    ServerQueryLogger queryLogger = ServerQueryLogger.getInstance();
    ThreadExecutionContext parentContext = Tracing.getThreadAccountant().getThreadExecutionContext();
    return _executorService.submit(() -> {
      try {
        if (_requests.size() == 1) {
          ServerQueryRequest request = _requests.get(0);
          Tracing.ThreadAccountantOps.setupWorker(1, parentContext);

          InstanceResponseBlock instanceResponseBlock =
              _queryExecutor.execute(request, _executorService, resultsBlockConsumer);
          if (queryLogger != null) {
            queryLogger.logQuery(request, instanceResponseBlock, "MultistageEngine");
          }
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
            mergeExecutionStats(instanceResponseBlock.getResponseMetadata());
          }
        } else {
          assert _requests.size() == 2;
          Future<Map<String, String>>[] futures = new Future[2];
          // TODO: this latch mechanism is not the most elegant. We should change it to use a CompletionService.
          //  In order to interrupt the execution in case of error, we could different mechanisms like throwing in the
          //  future, or using a shared volatile variable.
          CountDownLatch latch = new CountDownLatch(2);
          for (int i = 0; i < 2; i++) {
            ServerQueryRequest request = _requests.get(i);
            int taskId = i;
            futures[i] = _executorService.submit(() -> {
              Tracing.ThreadAccountantOps.setupWorker(taskId, parentContext);

              try {
                InstanceResponseBlock instanceResponseBlock =
                    _queryExecutor.execute(request, _executorService, resultsBlockConsumer);
                if (queryLogger != null) {
                  queryLogger.logQuery(request, instanceResponseBlock, "MultistageEngine");
                }
                Map<Integer, String> exceptions = instanceResponseBlock.getExceptions();
                if (!exceptions.isEmpty()) {
                  // Drain the latch when receiving exception block and not wait for the other thread to finish
                  _exceptions = exceptions;
                  latch.countDown();
                  return Collections.emptyMap();
                } else {
                  // NOTE: Instance response block might contain data (not metadata only) when all the segments are
                  //       pruned. Add the results block if it contains data.
                  BaseResultsBlock resultsBlock = instanceResponseBlock.getResultsBlock();
                  if (resultsBlock != null && resultsBlock.getNumRows() > 0) {
                    addResultsBlock(resultsBlock);
                  }
                  // Collect the execution stats
                  return instanceResponseBlock.getResponseMetadata();
                }
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
            for (Future<Map<String, String>> future : futures) {
              Map<String, String> stats =
                  future.get(_context.getDeadlineMs() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
              mergeExecutionStats(stats);
            }
          } catch (TimeoutException e) {
            throw new TimeoutException("Timed out waiting for leaf stage to finish");
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
      stats1.merge(k2, v2, (val1, val2) -> {
        try {
          return Long.toString(Long.parseLong(val1) + Long.parseLong(val2));
        } catch (Exception e) {
          return val1 + "\n" + val2;
        }
      });
    }
  }

  @Override
  public void close() {
    cancelSseTasks();
  }

  /**
   * Composes the {@link TransferableBlock} from the {@link BaseResultsBlock} returned from single-stage engine. It
   * converts the data types of the results to conform with the desired data schema asked by the multi-stage engine.
   */
  private TransferableBlock composeTransferableBlock(BaseResultsBlock resultsBlock) {
    if (resultsBlock instanceof SelectionResultsBlock) {
      return composeSelectTransferableBlock((SelectionResultsBlock) resultsBlock);
    } else {
      return composeDirectTransferableBlock(resultsBlock);
    }
  }

  /**
   * For selection, we need to check if the columns are in order. If not, we need to re-arrange the columns.
   */
  private TransferableBlock composeSelectTransferableBlock(SelectionResultsBlock resultsBlock) {
    int[] columnIndices = getColumnIndices(resultsBlock);
    if (!inOrder(columnIndices)) {
      return composeColumnIndexedTransferableBlock(resultsBlock, columnIndices);
    } else {
      return composeDirectTransferableBlock(resultsBlock);
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

  private TransferableBlock composeColumnIndexedTransferableBlock(SelectionResultsBlock block, int[] columnIndices) {
    List<Object[]> resultRows = block.getRows();
    DataSchema inputDataSchema = block.getDataSchema();
    assert resultRows != null && inputDataSchema != null;
    ColumnDataType[] inputStoredTypes = inputDataSchema.getStoredColumnDataTypes();
    ColumnDataType[] outputStoredTypes = _dataSchema.getStoredColumnDataTypes();
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
    return new TransferableBlock(convertedRows, _dataSchema, DataBlock.Type.ROW);
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

  private TransferableBlock composeDirectTransferableBlock(BaseResultsBlock block) {
    List<Object[]> resultRows = block.getRows();
    DataSchema inputDataSchema = block.getDataSchema();
    assert resultRows != null && inputDataSchema != null;
    ColumnDataType[] inputStoredTypes = inputDataSchema.getStoredColumnDataTypes();
    ColumnDataType[] outputStoredTypes = _dataSchema.getStoredColumnDataTypes();
    if (!Arrays.equals(inputStoredTypes, outputStoredTypes)) {
      for (Object[] row : resultRows) {
        convertRow(row, inputStoredTypes, outputStoredTypes);
      }
    }
    return new TransferableBlock(resultRows, _dataSchema, DataBlock.Type.ROW,
        _requests.get(0).getQueryContext().getAggregationFunctions());
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

  public enum StatKey implements StatMap.Key {
    TABLE(StatMap.Type.STRING, null),
    EXECUTION_TIME_MS(StatMap.Type.LONG, null) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG, null) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    NUM_DOCS_SCANNED(StatMap.Type.LONG),
    TOTAL_DOCS(StatMap.Type.LONG),
    NUM_ENTRIES_SCANNED_IN_FILTER(StatMap.Type.LONG),
    NUM_ENTRIES_SCANNED_POST_FILTER(StatMap.Type.LONG),
    NUM_SEGMENTS_QUERIED(StatMap.Type.INT),
    NUM_SEGMENTS_PROCESSED(StatMap.Type.INT),
    NUM_SEGMENTS_MATCHED(StatMap.Type.INT),
    NUM_CONSUMING_SEGMENTS_QUERIED(StatMap.Type.INT),
    NUM_CONSUMING_SEGMENTS_PROCESSED(StatMap.Type.INT),
    NUM_CONSUMING_SEGMENTS_MATCHED(StatMap.Type.INT),
    MIN_CONSUMING_FRESHNESS_TIME_MS(StatMap.Type.LONG) {
      @Override
      public long merge(long value1, long value2) {
        return StatMap.Key.minPositive(value1, value2);
      }
    },
    NUM_SEGMENTS_PRUNED_BY_SERVER(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_INVALID(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_BY_LIMIT(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_BY_VALUE(StatMap.Type.INT),
    NUM_GROUPS_LIMIT_REACHED(StatMap.Type.BOOLEAN),
    NUM_RESIZES(StatMap.Type.INT, null),
    RESIZE_TIME_MS(StatMap.Type.LONG, null),
    THREAD_CPU_TIME_NS(StatMap.Type.LONG, null),
    SYSTEM_ACTIVITIES_CPU_TIME_NS(StatMap.Type.LONG, null),
    RESPONSE_SER_CPU_TIME_NS(StatMap.Type.LONG, null) {
      @Override
      public String getStatName() {
        return "responseSerializationCpuTimeNs";
      }
    };

    private final StatMap.Type _type;
    @Nullable
    private final BrokerResponseNativeV2.StatKey _brokerKey;

    StatKey(StatMap.Type type) {
      _type = type;
      _brokerKey = BrokerResponseNativeV2.StatKey.valueOf(name());
    }

    StatKey(StatMap.Type type, @Nullable BrokerResponseNativeV2.StatKey brokerKey) {
      _type = type;
      _brokerKey = brokerKey;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }

    public void updateBrokerMetadata(StatMap<BrokerResponseNativeV2.StatKey> oldMetadata, StatMap<StatKey> stats) {
      if (_brokerKey != null) {
        switch (_type) {
          case LONG:
            if (_brokerKey.getType() == StatMap.Type.INT) {
              oldMetadata.merge(_brokerKey, (int) stats.getLong(this));
            } else {
              oldMetadata.merge(_brokerKey, stats.getLong(this));
            }
            break;
          case INT:
            oldMetadata.merge(_brokerKey, stats.getInt(this));
            break;
          case BOOLEAN:
            oldMetadata.merge(_brokerKey, stats.getBoolean(this));
            break;
          case STRING:
            oldMetadata.merge(_brokerKey, stats.getString(this));
            break;
          default:
            throw new IllegalStateException("Unsupported type: " + _type);
        }
      }
    }
  }
}
