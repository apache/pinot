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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
import org.apache.pinot.core.util.trace.TraceCallable;


/**
 * Table used for wrapping merged result of sorted group-by aggregation
 * This table is initialized with a {@link SortedRecords}, which is already merged and trimmed
 * to desired size.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SortedRecordTable extends BaseTable {
  private final ExecutorService _executorService;
  private final int _numKeyColumns;
  private final AggregationFunction[] _aggregationFunctions;

  private final Record[] _records;
  private final int _size;
  private final int _numThreadsExtractFinalResult;
  private final int _chunkSizeExtractFinalResult;

  public SortedRecordTable(SortedRecords sortedRecords, DataSchema dataSchema,
      QueryContext queryContext, ExecutorService executorService) {
    super(dataSchema);
    assert queryContext.getGroupByExpressions() != null;
    _numKeyColumns = queryContext.getGroupByExpressions().size();
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _executorService = executorService;
    _numThreadsExtractFinalResult = Math.min(queryContext.getNumThreadsExtractFinalResult(),
        Math.max(1, ResourceManager.DEFAULT_QUERY_RUNNER_THREADS));
    _chunkSizeExtractFinalResult = queryContext.getChunkSizeExtractFinalResult();
    _records = sortedRecords._records;
    _size = sortedRecords._size;
  }

  @Override
  public int size() {
    return _size;
  }

  @Override
  public Iterator<Record> iterator() {
    return Arrays.stream(_records, 0, size()).iterator();
  }

  @Override
  public void finish(boolean sort) {
    super.finish(sort);
  }

  // Copied from IndexedTable, but always _hasOrderBy
  // TODO: extract common logic between this and IndexedTable to BaseTable
  @Override
  public void finish(boolean sort, boolean storeFinalResult) {
    if (storeFinalResult) {
      DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
      int numAggregationFunctions = _aggregationFunctions.length;
      for (int i = 0; i < numAggregationFunctions; i++) {
        columnDataTypes[i + _numKeyColumns] = _aggregationFunctions[i].getFinalResultColumnType();
      }
      int numThreadsExtractFinalResult = inferNumThreadsExtractFinalResult();
      // Submit task when the EXECUTOR_SERVICE is not overloaded
      if (numThreadsExtractFinalResult > 1) {
        // Multi-threaded final reduce
        List<Future<Void>> futures = new ArrayList<>(numThreadsExtractFinalResult);
        try {
          int chunkSize = (size() + numThreadsExtractFinalResult - 1) / numThreadsExtractFinalResult;
          for (int threadId = 0; threadId < numThreadsExtractFinalResult; threadId++) {
            int startIdx = threadId * chunkSize;
            int endIdx = Math.min(startIdx + chunkSize, size());
            if (startIdx < endIdx) {
              // Submit a task for processing a chunk of values
              futures.add(_executorService.submit(new TraceCallable<Void>() {
                @Override
                public Void callJob() {
                  for (int recordIdx = startIdx; recordIdx < endIdx; recordIdx++) {
                    Object[] values = _records[recordIdx].getValues();
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
        for (int idx = 0; idx < size(); idx++) {
          Record record = _records[idx];
          Object[] values = record.getValues();
          for (int i = 0; i < numAggregationFunctions; i++) {
            int colId = i + _numKeyColumns;
            values[colId] = _aggregationFunctions[i].extractFinalResult(values[colId]);
          }
        }
      }
    }
  }

  private int inferNumThreadsExtractFinalResult() {
    if (_numThreadsExtractFinalResult > 1) {
      return _numThreadsExtractFinalResult;
    }
    if (containsExpensiveAggregationFunctions()) {
      int parallelChunkSize = _chunkSizeExtractFinalResult;
      if (_records != null && size() > parallelChunkSize) {
        int estimatedThreads = (int) Math.ceil((double) size() / parallelChunkSize);
        if (estimatedThreads == 0) {
          return 1;
        }
        return Math.min(estimatedThreads, QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY);
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

  /// Not used. Instead, create a {@link SortedRecords} and initialize this with it
  @Override
  public boolean upsert(Record record) {
    throw new UnsupportedOperationException("method unused for SortedRecordTable");
  }

  @Override
  public boolean upsert(Key key, Record record) {
    throw new UnsupportedOperationException("method unused for SortedRecordTable");
  }
}
