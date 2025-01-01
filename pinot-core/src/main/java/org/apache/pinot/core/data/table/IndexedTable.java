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
package org.apache.pinot.core.data.table;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.trace.TraceCallable;


/**
 * Base implementation of Map-based Table for indexed lookup
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class IndexedTable extends BaseTable {
  private static final int THREAD_POOL_SIZE = Math.max(Runtime.getRuntime().availableProcessors(), 1);
  private static final ThreadPoolExecutor EXECUTOR_SERVICE = (ThreadPoolExecutor) Executors.newFixedThreadPool(
      THREAD_POOL_SIZE, new ThreadFactory() {
        private final AtomicInteger _threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r, "IndexedTable-pool-thread-" + _threadNumber.getAndIncrement());
          t.setDaemon(true);
          return t;
        }
      });

  protected final Map<Key, Record> _lookupMap;
  protected final boolean _hasFinalInput;
  protected final int _resultSize;
  protected final int _numKeyColumns;
  protected final AggregationFunction[] _aggregationFunctions;
  protected final boolean _hasOrderBy;
  protected final TableResizer _tableResizer;
  protected final int _trimSize;
  protected final int _trimThreshold;
  protected final int _numThreadsForFinalReduce;
  protected final int _parallelChunkSizeForFinalReduce;

  protected Collection<Record> _topRecords;
  private int _numResizes;
  private long _resizeTimeNs;

  /**
   * Constructor for the IndexedTable.
   *
   * @param dataSchema    Data schema of the table
   * @param hasFinalInput Whether the input is the final aggregate result
   * @param queryContext  Query context
   * @param resultSize    Number of records to keep in the final result after calling {@link #finish(boolean, boolean)}
   * @param trimSize      Number of records to keep when trimming the table
   * @param trimThreshold Trim the table when the number of records exceeds the threshold
   * @param lookupMap     Map from keys to records
   */
  protected IndexedTable(DataSchema dataSchema, boolean hasFinalInput, QueryContext queryContext, int resultSize,
      int trimSize, int trimThreshold, Map<Key, Record> lookupMap) {
    super(dataSchema);

    Preconditions.checkArgument(resultSize >= 0, "Result size can't be negative");
    Preconditions.checkArgument(trimSize >= 0, "Trim size can't be negative");
    Preconditions.checkArgument(trimThreshold >= 0, "Trim threshold can't be negative");

    _lookupMap = lookupMap;
    _hasFinalInput = hasFinalInput;
    _resultSize = resultSize;

    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    _numKeyColumns = groupByExpressions.size();
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _hasOrderBy = queryContext.getOrderByExpressions() != null;
    _tableResizer = _hasOrderBy ? new TableResizer(dataSchema, hasFinalInput, queryContext) : null;
    // NOTE: Trim should be disabled when there is no ORDER BY
    assert _hasOrderBy || (trimSize == Integer.MAX_VALUE && trimThreshold == Integer.MAX_VALUE);
    _trimSize = trimSize;
    _trimThreshold = trimThreshold;
    // NOTE: The upper limit of threads number for final reduce is set to 2 * number of available processors by default
    _numThreadsForFinalReduce = Math.min(queryContext.getNumThreadsForFinalReduce(),
        Math.max(1, 2 * Runtime.getRuntime().availableProcessors()));
    _parallelChunkSizeForFinalReduce = queryContext.getParallelChunkSizeForFinalReduce();
  }

  @Override
  public boolean upsert(Record record) {
    // NOTE: The record will always have key columns (group-by expressions) in the front. This is handled in
    //       AggregationGroupByOrderByOperator.
    Object[] keyValues = Arrays.copyOf(record.getValues(), _numKeyColumns);
    return upsert(new Key(keyValues), record);
  }

  /**
   * Adds a record with new key or updates a record with existing key.
   */
  protected void addOrUpdateRecord(Key key, Record newRecord) {
    _lookupMap.compute(key, (k, v) -> v == null ? newRecord : updateRecord(v, newRecord));
  }

  /**
   * Updates a record with existing key. Record with new key will be ignored.
   */
  protected void updateExistingRecord(Key key, Record newRecord) {
    _lookupMap.computeIfPresent(key, (k, v) -> updateRecord(v, newRecord));
  }

  private Record updateRecord(Record existingRecord, Record newRecord) {
    Object[] existingValues = existingRecord.getValues();
    Object[] newValues = newRecord.getValues();
    int numAggregations = _aggregationFunctions.length;
    int index = _numKeyColumns;
    if (!_hasFinalInput) {
      for (int i = 0; i < numAggregations; i++, index++) {
        existingValues[index] = _aggregationFunctions[i].merge(existingValues[index], newValues[index]);
      }
    } else {
      for (int i = 0; i < numAggregations; i++, index++) {
        existingValues[index] = _aggregationFunctions[i].mergeFinalResult((Comparable) existingValues[index],
            (Comparable) newValues[index]);
      }
    }
    return existingRecord;
  }

  /**
   * Resizes the lookup map based on the trim size.
   */
  protected void resize() {
    assert _hasOrderBy;
    long startTimeNs = System.nanoTime();
    _tableResizer.resizeRecordsMap(_lookupMap, _trimSize);
    long resizeTimeNs = System.nanoTime() - startTimeNs;
    _numResizes++;
    _resizeTimeNs += resizeTimeNs;
  }

  @Override
  public void finish(boolean sort, boolean storeFinalResult) {
    if (_hasOrderBy) {
      long startTimeNs = System.nanoTime();
      _topRecords = _tableResizer.getTopRecords(_lookupMap, _resultSize, sort);
      long resizeTimeNs = System.nanoTime() - startTimeNs;
      _numResizes++;
      _resizeTimeNs += resizeTimeNs;
    } else {
      _topRecords = _lookupMap.values();
    }
    // TODO: Directly return final result in _tableResizer.getTopRecords to avoid extracting final result multiple times
    assert !(_hasFinalInput && !storeFinalResult);
    if (storeFinalResult && !_hasFinalInput) {
      ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
      int numAggregationFunctions = _aggregationFunctions.length;
      for (int i = 0; i < numAggregationFunctions; i++) {
        columnDataTypes[i + _numKeyColumns] = _aggregationFunctions[i].getFinalResultColumnType();
      }
      int numThreadsForFinalReduce = inferNumThreadsForFinalReduce();
      // Submit task when the EXECUTOR_SERVICE is not overloaded
      if ((numThreadsForFinalReduce > 1) && (EXECUTOR_SERVICE.getQueue().size() < THREAD_POOL_SIZE * 3)) {
        // Multi-threaded final reduce
        List<Future<Void>> futures = new ArrayList<>();
        try {
          List<Record> topRecordsList = new ArrayList<>(_topRecords);
          int chunkSize = (topRecordsList.size() + numThreadsForFinalReduce - 1) / numThreadsForFinalReduce;
          for (int threadId = 0; threadId < numThreadsForFinalReduce; threadId++) {
            int startIdx = threadId * chunkSize;
            int endIdx = Math.min(startIdx + chunkSize, topRecordsList.size());
            if (startIdx < endIdx) {
              // Submit a task for processing a chunk of values
              futures.add(EXECUTOR_SERVICE.submit(new TraceCallable<Void>() {
                @Override
                public Void callJob() {
                  for (int recordIdx = startIdx; recordIdx < endIdx; recordIdx++) {
                    Object[] values = topRecordsList.get(recordIdx).getValues();
                    for (int i = 0; i < numAggregationFunctions; i++) {
                      int colId = i + _numKeyColumns;
                      values[colId] = _aggregationFunctions[i].extractFinalResult(values[colId]);
                    }
                  }
                  return null;
                }
              }));
            }
          }
          // Wait for all tasks to complete
          for (Future<Void> future : futures) {
            future.get();
          }
        } catch (InterruptedException | ExecutionException e) {
          // Cancel all running tasks
          for (Future<Void> future : futures) {
            future.cancel(true);
          }
          throw new RuntimeException("Error during multi-threaded final reduce", e);
        }
      } else {
        for (Record record : _topRecords) {
          Object[] values = record.getValues();
          for (int i = 0; i < numAggregationFunctions; i++) {
            int colId = i + _numKeyColumns;
            values[colId] = _aggregationFunctions[i].extractFinalResult(values[colId]);
          }
        }
      }
    }
  }

  private int inferNumThreadsForFinalReduce() {
    if (_numThreadsForFinalReduce > 1) {
      return _numThreadsForFinalReduce;
    }
    if (containsExpensiveAggregationFunctions()) {
      int parallelChunkSize = _parallelChunkSizeForFinalReduce;
      if (_topRecords != null && _topRecords.size() > parallelChunkSize) {
        int estimatedThreads = (int) Math.ceil((double) _topRecords.size() / parallelChunkSize);
        if (estimatedThreads == 0) {
          return 1;
        }
        return Math.min(estimatedThreads, THREAD_POOL_SIZE);
      }
    }
    // Default to 1 thread
    return 1;
  }

  private boolean containsExpensiveAggregationFunctions() {
    for (AggregationFunction aggregationFunction : _aggregationFunctions) {
      switch (aggregationFunction.getType()) {
        case FUNNELCOMPLETECOUNT:
        case FUNNELCOUNT:
        case FUNNELMATCHSTEP:
        case FUNNELMAXSTEP:
          return true;
        default:
          break;
      }
    }
    return false;
  }

  @Override
  public int size() {
    return _topRecords != null ? _topRecords.size() : _lookupMap.size();
  }

  @Override
  public Iterator<Record> iterator() {
    return _topRecords.iterator();
  }

  public int getNumResizes() {
    return _numResizes;
  }

  public long getResizeTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(_resizeTimeNs);
  }
}
