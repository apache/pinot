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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
import org.apache.pinot.core.util.trace.TraceCallable;


/**
 * Table used for merging of sorted group-by aggregation
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SortedRecordTable extends BaseTable {
  private final ExecutorService _executorService;
  private final int _resultSize;
  private final int _numKeyColumns;
  private final AggregationFunction[] _aggregationFunctions;

  protected Record[] _topRecords;

  private Record[] _records;
  private int _nextIdx;
  private final Comparator<Record> _comparator;
  private final int _numThreadsExtractFinalResult;
  private final int _chunkSizeExtractFinalResult;

  public SortedRecordTable(DataSchema dataSchema, QueryContext queryContext, int resultSize,
      ExecutorService executorService, Comparator<Record> comparator) {
    super(dataSchema);
    _numKeyColumns = queryContext.getGroupByExpressions().size();
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _executorService = executorService;
    _comparator = comparator;
    _resultSize = resultSize;
    _numThreadsExtractFinalResult = Math.min(queryContext.getNumThreadsExtractFinalResult(),
        Math.max(1, ResourceManager.DEFAULT_QUERY_RUNNER_THREADS));
    _chunkSizeExtractFinalResult = queryContext.getChunkSizeExtractFinalResult();
    _records = new Record[_resultSize];
    _nextIdx = 0;
  }

  public SortedRecordTable(SortedRecords sortedRecords, DataSchema dataSchema, QueryContext queryContext,
      int resultSize, ExecutorService executorService, Comparator<Record> comparator) {
    super(dataSchema);
    _numKeyColumns = queryContext.getGroupByExpressions().size();
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _executorService = executorService;
    _comparator = comparator;
    _resultSize = resultSize;
    _numThreadsExtractFinalResult = Math.min(queryContext.getNumThreadsExtractFinalResult(),
        Math.max(1, ResourceManager.DEFAULT_QUERY_RUNNER_THREADS));
    _chunkSizeExtractFinalResult = queryContext.getChunkSizeExtractFinalResult();
    _records = sortedRecords._records;
    _nextIdx = sortedRecords._size;
  }

  /// Only used when creating SortedRecordTable from unique, sorted segment groupby results
  @Override
  public boolean upsert(Record record) {
    if (_nextIdx == _resultSize) {
      // enough records
      return false;
    }
    _records[_nextIdx++] = record;
    return true;
  }

  @Override
  public boolean upsert(Key key, Record record) {
    throw new UnsupportedOperationException("method unused for SortedRecordTable");
  }

  /// Merge a segment result into self, saving an allocation of SortedRecordTable
  public SortedRecordTable mergeSortedGroupByResultBlock(GroupByResultsBlock block) {
    List<IntermediateRecord> segmentRecords = block.getIntermediateRecords();
    if (segmentRecords.isEmpty() || size() == 0) {
      segmentRecords.forEach(x -> upsert(x._record));
      return this;
    }
    mergeSegmentRecords(segmentRecords, segmentRecords.size());
    return this;
  }

  private void finalizeRecordMerge(Record[] records, int newIdx) {
    _records = records;
    _nextIdx = newIdx;
  }

  /// Merge in that._records, update _records _curIdx
  private void mergeSegmentRecords(List<IntermediateRecord> records2, int mj) {
    Record[] newRecords = new Record[_resultSize];
    int newNextIdx = 0;

    Record[] records1 = _records;

    int i = 0;
    int j = 0;
    int mi = size();

    while (i < mi && j < mj) {
      int cmp = _comparator.compare(records1[i], records2.get(j)._record);
      if (cmp < 0) {
        newRecords[newNextIdx++] = records1[i++];
      } else if (cmp == 0) {
        newRecords[newNextIdx++] = updateRecord(records1[i++], records2.get(j++)._record);
      } else {
        newRecords[newNextIdx++] = records2.get(j++)._record;
      }
      // if enough records
      if (newNextIdx == _resultSize) {
        finalizeRecordMerge(newRecords, newNextIdx);
        return;
      }
    }

    while (i < mi) {
      newRecords[newNextIdx++] = records1[i++];
      if (newNextIdx == _resultSize) {
        finalizeRecordMerge(newRecords, newNextIdx);
        return;
      }
    }

    while (j < mj) {
      newRecords[newNextIdx++] = records2.get(j++)._record;
      if (newNextIdx == _resultSize) {
        finalizeRecordMerge(newRecords, newNextIdx);
        return;
      }
    }

    finalizeRecordMerge(newRecords, newNextIdx);
  }

  private Record updateRecord(Record existingRecord, Record newRecord) {
    Object[] existingValues = existingRecord.getValues();
    Object[] newValues = newRecord.getValues();
    int numAggregations = _aggregationFunctions.length;
    int index = _numKeyColumns;
    for (int i = 0; i < numAggregations; i++, index++) {
      existingValues[index] = _aggregationFunctions[i].merge(existingValues[index], newValues[index]);
    }
    return existingRecord;
  }

  @Override
  public int size() {
    return _nextIdx;
  }

  @Override
  public Iterator<Record> iterator() {
    return Arrays.stream(_topRecords, 0, size()).iterator();
  }

  @Override
  public void finish(boolean sort) {
    super.finish(sort);
  }

  // copied from IndexedTable, but always _hasOrderBy
  // TODO: extract common logic between this and IndexedTable to BaseTable
  @Override
  public void finish(boolean sort, boolean storeFinalResult) {
    _topRecords = _records;
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
                    Object[] values = _topRecords[recordIdx].getValues();
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
          Record record = _topRecords[idx];
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
      if (_topRecords != null && size() > parallelChunkSize) {
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
}
