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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.NotSupportedException;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link Table} implementation for aggregating TableRecords based on combination of keys
 */
@NotThreadSafe
public class TwoLevelHashMapIndexedTable extends BaseTable {
  private static final Logger log = LoggerFactory.getLogger(TwoLevelHashMapIndexedTable.class);
  protected final ExecutorService _executorService;
  protected final TwoLevelLinearProbingRecordHashMap _lookupMap;
  protected final boolean _hasFinalInput;
  protected final int _resultSize;
  protected final int _numKeyColumns;
  protected final AggregationFunction[] _aggregationFunctions;
  protected final boolean _hasOrderBy;
  protected final TableResizer _tableResizer;
  protected final int _trimSize;
  protected final int _trimThreshold;
  protected final int _numThreadsExtractFinalResult;
  protected final int _chunkSizeExtractFinalResult;

  protected int _numResizes;
  protected long _resizeTimeNs;

  public TwoLevelHashMapIndexedTable(DataSchema dataSchema, boolean hasFinalInput, QueryContext queryContext,
      int resultSize,
      int trimSize, int trimThreshold, ExecutorService executorService) {
    super(dataSchema);
    Preconditions.checkArgument(resultSize >= 0, "Result size can't be negative");
    Preconditions.checkArgument(trimSize >= 0, "Trim size can't be negative");
    Preconditions.checkArgument(trimThreshold >= 0, "Trim threshold can't be negative");

    _executorService = executorService;
    _lookupMap = new TwoLevelLinearProbingRecordHashMap();
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
    _numThreadsExtractFinalResult = Math.min(queryContext.getNumThreadsExtractFinalResult(),
        Math.max(1, ResourceManager.DEFAULT_QUERY_RUNNER_THREADS));
    _chunkSizeExtractFinalResult = queryContext.getChunkSizeExtractFinalResult();
  }

  @Override
  public boolean upsert(Record record) {
    throw new NotSupportedException();
  }

  /**
   * Non thread safe implementation of upsert to insert {@link Record} into the {@link Table}
   */
  @Override
  public boolean upsert(Key key, Record record) {
    throw new NotSupportedException();
  }

  /**
   * Non thread safe implementation of upsert to insert {@link Record} into the {@link Table}
   * This method avoids creating new payload object and directly use the passed in
   * {@code intermediateRecord} as payload or update existing payload.
   */
  public boolean upsert(IntermediateRecord intermediateRecord) {
    if (_hasOrderBy) {
      addOrUpdateRecord(intermediateRecord);
      if (_lookupMap.size() >= _trimThreshold) {
        resizePutOnly();
      }
    } else {
      if (_lookupMap.size() < _resultSize) {
        addOrUpdateRecord(intermediateRecord);
      } else {
        updateExistingRecord(intermediateRecord);
      }
    }
    return true;
  }

  /**
   * Adds a record with new key or updates a record with existing key.
   * The function should update the {@code} IntermediateRecord in-place
   */
  protected void addOrUpdateRecord(IntermediateRecord intermediateRecord) {
    _lookupMap.compute(intermediateRecord,
        (v) -> v == null ? intermediateRecord : updateRecord(v, intermediateRecord._record));
  }

  /**
   * Updates a record with existing key. Record with new key will be ignored.
   * The function should update the {@code} IntermediateRecord in-place
   */
  protected void updateExistingRecord(IntermediateRecord intermediateRecord) {
    _lookupMap.computeIfPresent(intermediateRecord, (v) -> updateRecord(v, intermediateRecord._record));
  }

  /**
   * Resizes the lookup map based on the trim size.
   * Used only by {@link TwoLevelHashMapIndexedTable}
   */
  protected void resizePutOnly() {
    assert _hasOrderBy;
    long startTimeNs = System.nanoTime();
    _tableResizer.resizeRecordsMapPutOnly(_lookupMap, _trimSize);
    long resizeTimeNs = System.nanoTime() - startTimeNs;
    _numResizes++;
    _resizeTimeNs += resizeTimeNs;
  }

  ///  should only use after called {@code finish}
  @Override
  public int size() {
    return _lookupMap.size();
  }

  @Override
  public Iterator<Record> iterator() {
    throw new NotSupportedException();
  }

  /**
   * Sort and trim the {@code _lookupMap} the final time
   * Partitioned hash group by is only used when there are order by clause
   */
  @Override
  public void finish(boolean sort, boolean storeFinalResult) {
    Preconditions.checkState(_hasOrderBy);
    Preconditions.checkState(!sort);
    Preconditions.checkState(!storeFinalResult);
    resizePutOnly();
  }

  ///  update an existing {@code IntermediateRecord._record} with a new record
  private IntermediateRecord updateRecord(IntermediateRecord existingIntermdiaterecord, Record newRecord) {
    Record existingRecord = existingIntermdiaterecord._record;
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
    Preconditions.checkState(existingIntermdiaterecord._record == existingRecord);
    return existingIntermdiaterecord;
  }

  public int getNumResizes() {
    return _numResizes;
  }

  public boolean isTrimmed() {
    // single resize occurs on finish()
    // all other re-sizes are triggered by trim size and threshold
    return _numResizes > 1;
  }

  public TwoLevelLinearProbingRecordHashMap getLookupMap() {
    return _lookupMap;
  }

  public long getResizeTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(_resizeTimeNs);
  }
}
