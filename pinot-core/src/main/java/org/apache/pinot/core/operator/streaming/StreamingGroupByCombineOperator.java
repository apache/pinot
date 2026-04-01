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
package org.apache.pinot.core.operator.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Streaming combine operator for group-by queries. Instead of accumulating all groups into a single IndexedTable
/// before returning (like {@link org.apache.pinot.core.operator.combine.GroupByCombineOperator}), this operator
/// flushes partial results when the number of accumulated groups reaches a configurable threshold.
///
/// <p>This bounds server memory usage for high-cardinality group-by queries on MSE leaf stages, while still
/// performing partial aggregation to reduce data volume compared to skipping leaf-stage group-by entirely.
///
/// <p>The downstream FINAL stage merges partial results from multiple flushes correctly because:
/// <ul>
///   <li>Hash exchange routes the same group key to the same FINAL worker</li>
///   <li>AggregationFunction.merge() is associative</li>
/// </ul>
@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamingGroupByCombineOperator extends BaseStreamingCombineOperator<GroupByResultsBlock> {
  private static final String EXPLAIN_NAME = "STREAMING_COMBINE_GROUP_BY";

  private final int _flushThreshold;
  private final int _numAggregationFunctions;
  private final int _numGroupByExpressions;
  private final int _numColumns;

  // Main-thread-only state for accumulating group-by results
  private DataSchema _dataSchema;
  private SimpleIndexedTable _indexedTable;
  private boolean _groupsTrimmed;
  private boolean _numGroupsLimitReached;
  private boolean _numGroupsWarningLimitReached;

  public StreamingGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, int flushThreshold) {
    super(null, operators, overrideMaxExecutionThreads(queryContext, operators.size()), executorService);
    _flushThreshold = flushThreshold;

    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    _numAggregationFunctions = aggregationFunctions.length;
    assert _queryContext.getGroupByExpressions() != null;
    _numGroupByExpressions = _queryContext.getGroupByExpressions().size();
    _numColumns = _numGroupByExpressions + _numAggregationFunctions;
  }

  /// For group-by queries, when maxExecutionThreads is not explicitly configured, override it to create as many tasks
  /// as the default number of query worker threads (or the number of operators / segments if that's lower).
  private static QueryContext overrideMaxExecutionThreads(QueryContext queryContext, int numOperators) {
    int maxExecutionThreads = queryContext.getMaxExecutionThreads();
    if (maxExecutionThreads <= 0) {
      queryContext.setMaxExecutionThreads(Math.min(numOperators, ResourceManager.DEFAULT_QUERY_WORKER_THREADS));
    }
    return queryContext;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected boolean isQuerySatisfied(GroupByResultsBlock resultsBlock, Object tracker) {
    return false;
  }

  /// Polls per-segment result blocks from worker threads, merges them into a SimpleIndexedTable, and flushes
  /// when the table reaches the flush threshold. Returns one block per call:
  /// - A GroupByResultsBlock when flushing accumulated groups
  /// - A MetadataResultsBlock when all operators are done and remaining data has been flushed
  /// - An ExceptionResultsBlock on error or timeout
  @Override
  protected BaseResultsBlock getNextBlock() {
    long endTimeMs = _queryContext.getEndTimeMs();
    try {
      while (_numOperatorsFinished < _numOperators) {
        QueryThreadContext.checkTermination(this::getExplainName);
        BaseResultsBlock resultsBlock =
            _blockingQueue.poll(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        if (resultsBlock == null) {
          throw QueryErrorCode.EXECUTION_TIMEOUT.asException("Timed out while streaming group-by results");
        }
        if (resultsBlock instanceof ExceptionResultsBlock) {
          return checkTerminateExceptionAndAttachExecutionStats(resultsBlock);
        }
        if (resultsBlock == LAST_RESULTS_BLOCK) {
          _numOperatorsFinished++;
          continue;
        }
        mergeBlock((GroupByResultsBlock) resultsBlock);
        if (_indexedTable != null && _indexedTable.size() >= _flushThreshold) {
          return flushTable();
        }
      }
    } catch (Exception e) {
      return createExceptionResultsBlockAndAttachExecutionStats(e, "streaming group-by results");
    }
    // All operators done — flush any remaining accumulated data
    if (_indexedTable != null && _indexedTable.size() > 0) {
      return flushTable();
    }
    // Return final metadata block
    return attachExecutionStats(new MetadataResultsBlock());
  }

  private void mergeBlock(GroupByResultsBlock resultsBlock) {
    if (_indexedTable == null) {
      _dataSchema = resultsBlock.getDataSchema();
      _indexedTable = createNewIndexedTable();
    }
    if (resultsBlock.isGroupsTrimmed()) {
      _groupsTrimmed = true;
    }
    if (resultsBlock.isNumGroupsLimitReached()) {
      _numGroupsLimitReached = true;
    }
    if (resultsBlock.isNumGroupsWarningLimitReached()) {
      _numGroupsWarningLimitReached = true;
    }
    List<IntermediateRecord> intermediateRecords = resultsBlock.getIntermediateRecords();
    if (intermediateRecords == null) {
      AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
      if (aggregationGroupByResult != null) {
        try {
          Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
          int mergedKeys = 0;
          while (groupKeyIterator.hasNext()) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
            GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
            Object[] keys = groupKey._keys;
            Object[] values = Arrays.copyOf(keys, _numColumns);
            int groupId = groupKey._groupId;
            for (int i = 0; i < _numAggregationFunctions; i++) {
              values[_numGroupByExpressions + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
            }
            _indexedTable.upsert(new Key(keys), new Record(values));
          }
        } finally {
          aggregationGroupByResult.closeGroupKeyGenerator();
        }
      }
    } else {
      int mergedKeys = 0;
      for (IntermediateRecord intermediateResult : intermediateRecords) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
        _indexedTable.upsert(intermediateResult._key, intermediateResult._record);
      }
    }
  }

  private GroupByResultsBlock flushTable() {
    _indexedTable.finish(false);
    GroupByResultsBlock block = new GroupByResultsBlock(_indexedTable, _queryContext);
    block.setGroupsTrimmed(_groupsTrimmed);
    block.setNumGroupsLimitReached(_numGroupsLimitReached);
    block.setNumGroupsWarningLimitReached(_numGroupsWarningLimitReached);
    _indexedTable = createNewIndexedTable();
    return block;
  }

  private SimpleIndexedTable createNewIndexedTable() {
    int initialCapacity = HashUtil.getHashMapCapacity(_flushThreshold);
    return new SimpleIndexedTable(_dataSchema, false, _queryContext, Integer.MAX_VALUE,
        Integer.MAX_VALUE, Integer.MAX_VALUE, initialCapacity, _executorService);
  }
}
