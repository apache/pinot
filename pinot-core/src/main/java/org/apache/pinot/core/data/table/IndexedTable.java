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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Base implementation of Map-based Table for indexed lookup
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class IndexedTable extends BaseTable {
  protected final Map<Key, Record> _lookupMap;
  protected final int _resultSize;
  protected final int _numKeyColumns;
  protected final AggregationFunction[] _aggregationFunctions;
  protected final boolean _hasOrderBy;
  protected final TableResizer _tableResizer;
  protected final int _trimSize;
  protected final int _trimThreshold;

  protected Collection<Record> _topRecords;
  protected boolean _isResultTrimmed;
  private int _numResizes;
  private long _resizeTimeNs;

  /**
   * Constructor for the IndexedTable.
   *
   * @param dataSchema    Data schema of the table
   * @param queryContext  Query context
   * @param resultSize    Number of records to keep in the final result after calling {@link #finish(boolean, boolean)}
   * @param trimSize      Number of records to keep when trimming the table
   * @param trimThreshold Trim the table when the number of records exceeds the threshold
   * @param lookupMap     Map from keys to records
   */
  protected IndexedTable(DataSchema dataSchema, QueryContext queryContext, int resultSize, int trimSize,
      int trimThreshold, Map<Key, Record> lookupMap) {
    super(dataSchema);
    _lookupMap = lookupMap;
    _resultSize = resultSize;

    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    _numKeyColumns = groupByExpressions.size();
    _aggregationFunctions = queryContext.getAggregationFunctions();
    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    if (orderByExpressions != null) {
      // GROUP BY with ORDER BY
      _hasOrderBy = true;
      _tableResizer = new TableResizer(dataSchema, queryContext);
      // NOTE: trimSize is bounded by trimThreshold/2 to protect the server from using too much memory.
      // TODO: Re-evaluate it as it can lead to in-accurate results
      _trimSize = Math.min(trimSize, trimThreshold / 2);
      _trimThreshold = trimThreshold;
    } else {
      // GROUP BY without ORDER BY
      // NOTE: The indexed table stops accepting records once the map size reaches resultSize, and there is no
      //       resize/trim during upsert.
      _hasOrderBy = false;
      _tableResizer = null;
      _trimSize = Integer.MAX_VALUE;
      _trimThreshold = Integer.MAX_VALUE;
    }
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
    _lookupMap.compute(key, (k, v) -> {
      if (v == null) {
        return newRecord;
      } else {
        Object[] existingValues = v.getValues();
        Object[] newValues = newRecord.getValues();
        int aggNum = 0;
        for (int i = _numKeyColumns; i < _numColumns; i++) {
          existingValues[i] = _aggregationFunctions[aggNum++].merge(existingValues[i], newValues[i]);
        }
        return v;
      }
    });
  }

  /**
   * Updates a record with existing key. Record with new key will be ignored.
   */
  protected void updateExistingRecord(Key key, Record newRecord) {
    Record record = _lookupMap.computeIfPresent(key, (k, v) -> {
      Object[] existingValues = v.getValues();
      Object[] newValues = newRecord.getValues();
      int aggNum = 0;
      for (int i = _numKeyColumns; i < _numColumns; i++) {
        existingValues[i] = _aggregationFunctions[aggNum++].merge(existingValues[i], newValues[i]);
      }
      return v;
    });

    if (record == null) {
      // Record not present.
      _isResultTrimmed = true;
    }
  }

  /**
   * Resizes the lookup map based on the trim size.
   */
  protected void resize() {
    assert _hasOrderBy;
    _isResultTrimmed = _lookupMap.size() > _trimSize;
    long startTimeNs = System.nanoTime();
    _tableResizer.resizeRecordsMap(_lookupMap, _trimSize);
    long resizeTimeNs = System.nanoTime() - startTimeNs;
    _numResizes++;
    _resizeTimeNs += resizeTimeNs;
  }

  @Override
  public void finish(boolean sort, boolean storeFinalResult) {
    if (_hasOrderBy) {
      _isResultTrimmed = _lookupMap.size() > _resultSize;
      long startTimeNs = System.nanoTime();
      _topRecords = _tableResizer.getTopRecords(_lookupMap, _resultSize, sort);
      long resizeTimeNs = System.nanoTime() - startTimeNs;
      _numResizes++;
      _resizeTimeNs += resizeTimeNs;
    } else {
      _topRecords = _lookupMap.values();
    }
    // TODO: Directly return final result in _tableResizer.getTopRecords to avoid extracting final result multiple times
    if (storeFinalResult) {
      ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
      int numAggregationFunctions = _aggregationFunctions.length;
      for (int i = 0; i < numAggregationFunctions; i++) {
        columnDataTypes[i + _numKeyColumns] = _aggregationFunctions[i].getFinalResultColumnType();
      }
      for (Record record : _topRecords) {
        Object[] values = record.getValues();
        for (int i = 0; i < numAggregationFunctions; i++) {
          int colId = i + _numKeyColumns;
          values[colId] = _aggregationFunctions[i].extractFinalResult(values[colId]);
        }
      }
    }
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

  public boolean isResultTrimmed() {
    return _isResultTrimmed;
  }
}
