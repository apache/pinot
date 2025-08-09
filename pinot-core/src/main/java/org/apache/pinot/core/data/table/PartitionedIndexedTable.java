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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.ws.rs.NotSupportedException;
import org.apache.arrow.util.Preconditions;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
import org.apache.pinot.core.util.trace.TraceCallable;


/**
 * IndexedTable wrapper for RadixPartitionedHashMap,
 * used for and stitching hash tables together in phase 2
 */
public class PartitionedIndexedTable extends IndexedTable {
  public PartitionedIndexedTable(DataSchema dataSchema, boolean hasFinalInput, QueryContext queryContext,
      int resultSize, int trimSize, int trimThreshold, RadixPartitionedHashMap map,
      ExecutorService executorService) {
    super(dataSchema, hasFinalInput, queryContext, resultSize, trimSize, trimThreshold, map,
        executorService);
  }

  public TwoLevelLinearProbingRecordHashMap getPartition(int i) {
    RadixPartitionedHashMap map = (RadixPartitionedHashMap) _lookupMap;
    return map.getPartition(i);
  }

  @Override
  public boolean upsert(Key key, Record record) {
    throw new NotSupportedException("should not finish on PartitionedIndexedTable");
  }

  @Override
  public void finish(boolean sort, boolean storeFinalResult) {
    // partitioned group by aggregate should only be used when there is an order by
    Preconditions.checkState(_hasOrderBy);
    RadixPartitionedHashMap map = (RadixPartitionedHashMap) _lookupMap;
    long startTimeNs = System.nanoTime();
    _topRecords = _tableResizer.getTopRecords(map, _resultSize, sort);
    long resizeTimeNs = System.nanoTime() - startTimeNs;
    _numResizes++;
    _resizeTimeNs += resizeTimeNs;

    assert !(_hasFinalInput && !storeFinalResult);
    if (storeFinalResult && !_hasFinalInput) {
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
          List<Record> topRecordsList = new ArrayList<>(_topRecords);
          int chunkSize = (topRecordsList.size() + numThreadsExtractFinalResult - 1) / numThreadsExtractFinalResult;
          for (int threadId = 0; threadId < numThreadsExtractFinalResult; threadId++) {
            int startIdx = threadId * chunkSize;
            int endIdx = Math.min(startIdx + chunkSize, topRecordsList.size());
            if (startIdx < endIdx) {
              // Submit a task for processing a chunk of values
              futures.add(_executorService.submit(new TraceCallable<Void>() {
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

  private int inferNumThreadsExtractFinalResult() {
    if (_numThreadsExtractFinalResult > 1) {
      return _numThreadsExtractFinalResult;
    }
    if (containsExpensiveAggregationFunctions()) {
      int parallelChunkSize = _chunkSizeExtractFinalResult;
      if (_topRecords != null && _topRecords.size() > parallelChunkSize) {
        int estimatedThreads = (int) Math.ceil((double) _topRecords.size() / parallelChunkSize);
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
}
