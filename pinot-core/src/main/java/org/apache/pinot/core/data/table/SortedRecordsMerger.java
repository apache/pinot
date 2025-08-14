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

import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GroupByUtils;


@SuppressWarnings({"unchecked", "rawtypes"})
public class SortedRecordsMerger {
  private final int _resultSize;
  private final Comparator<Record> _comparator;
  private final int _numKeyColumns;
  private final AggregationFunction[] _aggregationFunctions;
  private static final BiFunction<Object, Integer, Record> INTERMEDIATE_RECORD_EXTRACTOR =
      (Object intermediateRecords, Integer idx) ->
          ((List<IntermediateRecord>) intermediateRecords).get(idx)._record;
  private static final BiFunction<Object, Integer, Record> RECORD_ARRAY_EXTRACTOR =
      (Object records, Integer idx) -> ((Record[]) records)[idx];

  public SortedRecordsMerger(QueryContext queryContext, int resultSize, Comparator<Record> comparator) {
    assert queryContext.getGroupByExpressions() != null;
    _numKeyColumns = queryContext.getGroupByExpressions().size();
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _resultSize = resultSize;
    _comparator = comparator;
  }

  private void merge(SortedRecords left, Object right, int mj, BiFunction<Object, Integer, Record> rightExtractor) {
    int mi = left._size;
    Record[] records1 = left._records;
    Record[] newRecords = new Record[Math.min(left._size + mj, _resultSize)];
    int newNextIdx = 0;

    int i = 0;
    int j = 0;

    while (i < mi && j < mj) {
      int cmp = _comparator.compare(records1[i], rightExtractor.apply(right, j));
      if (cmp < 0) {
        newRecords[newNextIdx++] = records1[i++];
      } else if (cmp == 0) {
        newRecords[newNextIdx++] = updateRecord(records1[i++], rightExtractor.apply(right, j++));
      } else {
        newRecords[newNextIdx++] = rightExtractor.apply(right, j++);
      }
      // if enough records
      if (newNextIdx == _resultSize) {
        finalizeRecordMerge(left, newRecords, newNextIdx);
        return;
      }
    }

    while (i < mi) {
      newRecords[newNextIdx++] = records1[i++];
      if (newNextIdx == _resultSize) {
        finalizeRecordMerge(left, newRecords, newNextIdx);
        return;
      }
    }

    while (j < mj) {
      newRecords[newNextIdx++] = rightExtractor.apply(right, j++);
      if (newNextIdx == _resultSize) {
        finalizeRecordMerge(left, newRecords, newNextIdx);
        return;
      }
    }

    finalizeRecordMerge(left, newRecords, newNextIdx);
  }

  /// Merge a SortedRecords into another SortedRecords
  public SortedRecords mergeSortedRecordArray(SortedRecords left, SortedRecords right) {
    if (right._size == 0) {
      return left;
    }
    if (left._size == 0) {
      return right;
    }
    merge(left, right._records, right._size, RECORD_ARRAY_EXTRACTOR);
    return left;
  }

  /// Merge a GroupByResultsBlock into another SortedRecords
  public SortedRecords mergeGroupByResultsBlock(SortedRecords left, GroupByResultsBlock right) {
    List<IntermediateRecord> intermediateRecords = right.getIntermediateRecords();
    if (intermediateRecords.isEmpty()) {
      return left;
    }
    if (left._size == 0) {
      return GroupByUtils.getAndPopulateSortedRecords(right);
    }
    merge(left, intermediateRecords, intermediateRecords.size(), INTERMEDIATE_RECORD_EXTRACTOR);
    return left;
  }

  private void finalizeRecordMerge(SortedRecords sortedRecords, Record[] records, int newIdx) {
    sortedRecords._records = records;
    sortedRecords._size = newIdx;
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
}
