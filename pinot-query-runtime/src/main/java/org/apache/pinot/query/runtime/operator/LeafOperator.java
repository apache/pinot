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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
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
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Leaf operator processes the leaf stage of a multi-stage query with single-stage engine on the server.
/// The data schema of the result expected from leaf stage might be different from the one returned from single-stage
/// engine, thus the leaf stage operator needs to convert the data types of the result to conform with the expected
/// data schema.
public class LeafOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeafOperator.class);
  private static final String EXPLAIN_NAME = "LEAF";
  private static final ErrorMseBlock CANCELLED_BLOCK =
      ErrorMseBlock.fromError(QueryErrorCode.QUERY_CANCELLATION, "Cancelled while waiting for leaf results");
  private static final ErrorMseBlock TIMEOUT_BLOCK =
      ErrorMseBlock.fromError(QueryErrorCode.EXECUTION_TIMEOUT, "Timed out waiting for leaf results");

  // Use a special results block to indicate that this is the last results block
  @VisibleForTesting
  static final MetadataResultsBlock LAST_RESULTS_BLOCK = new MetadataResultsBlock();

  private final List<ServerQueryRequest> _requests;
  private final DataSchema _dataSchema;
  private final QueryExecutor _queryExecutor;
  private final ExecutorService _executorService;
  private final String _tableName;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);
  private final AtomicReference<ErrorMseBlock> _errorBlock = new AtomicReference<>();

  // Use a limit-sized BlockingQueue to store the results blocks and apply back pressure to the single-stage threads
  @VisibleForTesting
  final BlockingQueue<BaseResultsBlock> _blockingQueue;

  @Nullable
  private volatile Future<?> _executionFuture;
  private volatile boolean _terminated;

  public LeafOperator(OpChainExecutionContext context, List<ServerQueryRequest> requests, DataSchema dataSchema,
      QueryExecutor queryExecutor, ExecutorService executorService) {
    super(context);
    int numRequests = requests.size();
    Preconditions.checkArgument(numRequests == 1 || numRequests == 2, "Expected 1 or 2 requests, got: %s", numRequests);
    _requests = requests;
    _dataSchema = dataSchema;
    _queryExecutor = queryExecutor;
    _executorService = executorService;
    _tableName = context.getStageMetadata().getTableName();
    Preconditions.checkArgument(_tableName != null, "Table name must be set in the stage metadata");
    _statMap.merge(StatKey.TABLE, _tableName);
    Integer maxStreamingPendingBlocks = QueryOptionsUtils.getMaxStreamingPendingBlocks(context.getOpChainMetadata());
    _blockingQueue = new ArrayBlockingQueue<>(maxStreamingPendingBlocks != null ? maxStreamingPendingBlocks
        : QueryOptionValue.DEFAULT_MAX_STREAMING_PENDING_BLOCKS);
  }

  public List<ServerQueryRequest> getRequests() {
    return _requests;
  }

  public DataSchema getDataSchema() {
    return _dataSchema;
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
    return List.of();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_executionFuture == null) {
      _executionFuture = startExecution();
    }
    if (_isEarlyTerminated) {
      terminateAndClearResultsBlocks();
      return SuccessMseBlock.INSTANCE;
    }
    BaseResultsBlock resultsBlock;
    try {
      // Here we use passive deadline because we end up waiting for the SSE operators which can timeout by their own.
      resultsBlock =
          _blockingQueue.poll(_context.getPassiveDeadlineMs() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      terminateAndClearResultsBlocks();
      return CANCELLED_BLOCK;
    }
    if (resultsBlock == null) {
      terminateAndClearResultsBlocks();
      return TIMEOUT_BLOCK;
    }
    // Terminate when there is error block
    ErrorMseBlock errorBlock = getErrorBlock();
    if (errorBlock != null) {
      terminateAndClearResultsBlocks();
      return errorBlock;
    }
    if (resultsBlock == LAST_RESULTS_BLOCK) {
      _terminated = true;
      return SuccessMseBlock.INSTANCE;
    } else {
      // Regular data block
      return composeMseBlock(resultsBlock);
    }
  }

  public ExplainedNode explain() {
    if (_executionFuture == null) {
      _executionFuture = startExecution();
    }
    List<PlanNode> childNodes = new ArrayList<>();
    while (true) {
      if (_isEarlyTerminated) {
        terminateAndClearResultsBlocks();
        break;
      }
      BaseResultsBlock resultsBlock;
      try {
        // Here we use passive deadline because we end up waiting for the SSE operators which can timeout by their own.
        resultsBlock =
            _blockingQueue.poll(_context.getPassiveDeadlineMs() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        terminateAndClearResultsBlocks();
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for results block", e);
      }
      if (resultsBlock == null) {
        terminateAndClearResultsBlocks();
        throw new RuntimeException("Timed out waiting for results block");
      }
      // Terminate when there is error block
      ErrorMseBlock errorBlock = getErrorBlock();
      if (errorBlock != null) {
        terminateAndClearResultsBlocks();
        throw new RuntimeException("Received error block: " + errorBlock.getErrorMessages());
      }
      if (resultsBlock == LAST_RESULTS_BLOCK) {
        _terminated = true;
        break;
      }
      if (resultsBlock instanceof ExplainV2ResultBlock) {
        ExplainV2ResultBlock block = (ExplainV2ResultBlock) resultsBlock;
        for (ExplainInfo physicalPlan : block.getPhysicalPlans()) {
          childNodes.add(asNode(physicalPlan));
        }
      } else {
        terminateAndClearResultsBlocks();
        throw new IllegalArgumentException("Expected ExplainV2ResultBlock, got: " + resultsBlock.getClass().getName());
      }
    }
    Map<String, Plan.ExplainNode.AttributeValue> attributes =
        Map.of("table", Plan.ExplainNode.AttributeValue.newBuilder().setString(_tableName).build());
    return new ExplainedNode(_context.getStageId(), _dataSchema, null, childNodes, "LeafStageCombineOperator",
        attributes);
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  @Override
  protected void earlyTerminate() {
    _isEarlyTerminated = true;
    cancelSseTasks();
  }

  @Override
  public void cancel(Throwable e) {
    cancelSseTasks();
  }

  @Override
  public void close() {
    cancelSseTasks();
  }

  @VisibleForTesting
  void cancelSseTasks() {
    Future<?> executionFuture = _executionFuture;
    if (executionFuture != null) {
      executionFuture.cancel(true);
    }
  }

  private synchronized void mergeExecutionStats(Map<String, String> executionStats) {
    for (Map.Entry<String, String> entry : executionStats.entrySet()) {
      String key = entry.getKey();
      DataTable.MetadataKey metadataKey = DataTable.MetadataKey.getByName(key);
      if (metadataKey == null || metadataKey == DataTable.MetadataKey.UNKNOWN) {
        LOGGER.debug("Skipping unknown execution stat: {}", key);
        continue;
      }
      switch (metadataKey) {
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
        case GROUPS_TRIMMED:
          _statMap.merge(StatKey.GROUPS_TRIMMED, Boolean.parseBoolean(entry.getValue()));
          break;
        case NUM_GROUPS_LIMIT_REACHED:
          _statMap.merge(StatKey.NUM_GROUPS_LIMIT_REACHED, Boolean.parseBoolean(entry.getValue()));
          break;
        case NUM_GROUPS_WARNING_LIMIT_REACHED:
          _statMap.merge(StatKey.NUM_GROUPS_WARNING_LIMIT_REACHED, Boolean.parseBoolean(entry.getValue()));
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
        case THREAD_MEM_ALLOCATED_BYTES:
          _statMap.merge(StatKey.THREAD_MEM_ALLOCATED_BYTES, Long.parseLong(entry.getValue()));
          break;
        case RESPONSE_SER_MEM_ALLOCATED_BYTES:
          _statMap.merge(StatKey.RESPONSE_SER_MEM_ALLOCATED_BYTES, Long.parseLong(entry.getValue()));
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
        case SORTED:
          break;
        default:
          throw new IllegalArgumentException("Unhandled leaf execution stat: " + key);
      }
    }
  }

  private ExplainedNode asNode(ExplainInfo info) {
    int size = info.getInputs().size();
    List<PlanNode> inputs = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      inputs.add(asNode(info.getInputs().get(i)));
    }

    return new ExplainedNode(_context.getStageId(), _dataSchema, null, inputs, info.getTitle(), info.getAttributes());
  }

  @Nullable
  private ErrorMseBlock getErrorBlock() {
    return _errorBlock.get();
  }

  private void setErrorBlock(ErrorMseBlock errorBlock) {
    // Keep the first encountered error block
    _errorBlock.compareAndSet(null, errorBlock);
  }

  private Future<?> startExecution() {
    ThreadExecutionContext parentContext = Tracing.getThreadAccountant().getThreadExecutionContext();
    return _executorService.submit(() -> {
      try {
        execute(parentContext);
      } catch (Exception e) {
        setErrorBlock(
            ErrorMseBlock.fromError(QueryErrorCode.INTERNAL, "Caught exception while executing leaf stage: " + e));
      } finally {
        // Always add the last results block to mark the end of the execution and notify the main thread waiting for the
        // results block.
        try {
          addResultsBlock(LAST_RESULTS_BLOCK);
        } catch (Exception e) {
          if (!(e instanceof EarlyTerminationException)) {
            LOGGER.warn("Failed to add the last results block", e);
          }
        }
      }
    });
  }

  @VisibleForTesting
  void execute(ThreadExecutionContext parentContext) {
    ResultsBlockConsumer resultsBlockConsumer = new ResultsBlockConsumer();
    ServerQueryLogger queryLogger = ServerQueryLogger.getInstance();
    if (_requests.size() == 1) {
      ServerQueryRequest request = _requests.get(0);
      Tracing.ThreadAccountantOps.setupWorker(1, parentContext);

      InstanceResponseBlock instanceResponseBlock =
          _queryExecutor.execute(request, _executorService, resultsBlockConsumer);
      if (queryLogger != null) {
        queryLogger.logQuery(request, instanceResponseBlock, "MultistageEngine");
      }
      // Collect the execution stats
      mergeExecutionStats(instanceResponseBlock.getResponseMetadata());
      // TODO: Revisit if we should treat all exceptions as query failure. Currently MERGE_RESPONSE_ERROR and
      //       SERVER_SEGMENT_MISSING_ERROR are counted as query failure.
      Map<Integer, String> exceptions = instanceResponseBlock.getExceptions();
      if (!exceptions.isEmpty()) {
        setErrorBlock(ErrorMseBlock.fromMap(QueryErrorCode.fromKeyMap(exceptions)));
      } else {
        // NOTE: Instance response block might contain data (not metadata only) when all the segments are pruned.
        //       Add the results block if it contains data.
        BaseResultsBlock resultsBlock = instanceResponseBlock.getResultsBlock();
        if (resultsBlock != null && resultsBlock.getNumRows() > 0) {
          try {
            addResultsBlock(resultsBlock);
          } catch (InterruptedException e) {
            setErrorBlock(CANCELLED_BLOCK);
          } catch (TimeoutException e) {
            setErrorBlock(TIMEOUT_BLOCK);
          } catch (Exception e) {
            if (!(e instanceof EarlyTerminationException)) {
              LOGGER.warn("Failed to add results block", e);
            }
          }
        }
      }
    } else {
      // Hit 2 physical tables, one REALTIME and one OFFLINE
      assert _requests.size() == 2;
      Future<?>[] futures = new Future[2];
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
            // Collect the execution stats
            mergeExecutionStats(instanceResponseBlock.getResponseMetadata());
            Map<Integer, String> exceptions = instanceResponseBlock.getExceptions();
            if (!exceptions.isEmpty()) {
              setErrorBlock(ErrorMseBlock.fromMap(QueryErrorCode.fromKeyMap(exceptions)));
              // Drain the latch when receiving exception block and not wait for the other thread to finish
              latch.countDown();
            } else {
              // NOTE: Instance response block might contain data (not metadata only) when all the segments are
              //       pruned. Add the results block if it contains data.
              BaseResultsBlock resultsBlock = instanceResponseBlock.getResultsBlock();
              if (resultsBlock != null && resultsBlock.getNumRows() > 0) {
                try {
                  addResultsBlock(resultsBlock);
                } catch (InterruptedException e) {
                  setErrorBlock(CANCELLED_BLOCK);
                } catch (TimeoutException e) {
                  setErrorBlock(TIMEOUT_BLOCK);
                } catch (Exception e) {
                  if (!(e instanceof EarlyTerminationException)) {
                    LOGGER.warn("Failed to add results block", e);
                  }
                }
              }
            }
          } finally {
            latch.countDown();
          }
        });
      }
      try {
        if (!latch.await(_context.getPassiveDeadlineMs() - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
          setErrorBlock(TIMEOUT_BLOCK);
        }
      } catch (InterruptedException e) {
        setErrorBlock(CANCELLED_BLOCK);
      } finally {
        for (Future<?> future : futures) {
          future.cancel(true);
        }
      }
    }
  }

  @VisibleForTesting
  void addResultsBlock(BaseResultsBlock resultsBlock)
      throws InterruptedException, TimeoutException {
    if (_terminated) {
      throw new EarlyTerminationException("Query has been terminated");
    }
    if (!_blockingQueue.offer(resultsBlock, _context.getPassiveDeadlineMs() - System.currentTimeMillis(),
        TimeUnit.MILLISECONDS)) {
      throw new TimeoutException("Timed out waiting to add results block");
    }
  }

  private void terminateAndClearResultsBlocks() {
    _terminated = true;
    _blockingQueue.clear();
  }

  /**
   * Composes the {@link MseBlock} from the {@link BaseResultsBlock} returned from single-stage engine. It
   * converts the data types of the results to conform with the desired data schema asked by the multi-stage engine.
   */
  private RowHeapDataBlock composeMseBlock(BaseResultsBlock resultsBlock) {
    if (resultsBlock instanceof SelectionResultsBlock) {
      return composeSelectMseBlock((SelectionResultsBlock) resultsBlock);
    } else {
      return composeDirectMseBlock(resultsBlock);
    }
  }

  /**
   * For selection, we need to check if the columns are in order. If not, we need to re-arrange the columns.
   */
  private RowHeapDataBlock composeSelectMseBlock(SelectionResultsBlock resultsBlock) {
    int[] columnIndices = getColumnIndices(resultsBlock);
    if (!inOrder(columnIndices)) {
      return composeColumnIndexedMseBlock(resultsBlock, columnIndices);
    } else {
      return composeDirectMseBlock(resultsBlock);
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

  private RowHeapDataBlock composeColumnIndexedMseBlock(SelectionResultsBlock block, int[] columnIndices) {
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
    return new RowHeapDataBlock(convertedRows, _dataSchema);
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

  private RowHeapDataBlock composeDirectMseBlock(BaseResultsBlock block) {
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
    return new RowHeapDataBlock(resultRows, _dataSchema, _requests.get(0).getQueryContext().getAggregationFunctions());
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
    GROUPS_TRIMMED(StatMap.Type.BOOLEAN),
    NUM_GROUPS_LIMIT_REACHED(StatMap.Type.BOOLEAN),
    NUM_GROUPS_WARNING_LIMIT_REACHED(StatMap.Type.BOOLEAN),
    NUM_RESIZES(StatMap.Type.INT, null),
    RESIZE_TIME_MS(StatMap.Type.LONG, null),
    THREAD_CPU_TIME_NS(StatMap.Type.LONG, null),
    THREAD_MEM_ALLOCATED_BYTES(StatMap.Type.LONG, null),
    RESPONSE_SER_MEM_ALLOCATED_BYTES(StatMap.Type.LONG, null),
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
