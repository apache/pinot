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
package org.apache.pinot.core.operator.filter;

import com.google.common.base.Preconditions;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.ScanBasedDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory.OfflineDictionaryBasedRangePredicateEvaluator;
import org.apache.pinot.core.segment.index.readers.RangeIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class RangeIndexBasedFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "RangeFilterOperator";

  private final OfflineDictionaryBasedRangePredicateEvaluator _predicateEvaluator;
  private final DataSource _dataSource;
  private final int _startDocId;
  private final int _endDocId;
  private final boolean _exclusive;

  public RangeIndexBasedFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int startDocId,
      int endDocId) {
    // NOTE:
    // Predicate that is always evaluated as true or false should not be passed into the BitmapBasedFilterOperator for
    // performance concern.
    // If predicate is always evaluated as true, use MatchAllFilterOperator; if predicate is always evaluated as false,
    // use EmptyFilterOperator.
    Preconditions.checkArgument(!predicateEvaluator.isAlwaysTrue() && !predicateEvaluator.isAlwaysFalse());

    // NOTE: Only dictionary-based evaluator is supported for now
    _predicateEvaluator = (OfflineDictionaryBasedRangePredicateEvaluator) predicateEvaluator;
    _dataSource = dataSource;
    _startDocId = startDocId;
    _endDocId = endDocId;
    _exclusive = predicateEvaluator.isExclusive();
  }

  @Override
  protected FilterBlock getNextBlock() {
    RangeIndexReader rangeIndexReader = (RangeIndexReader) _dataSource.getRangeIndex();
    assert rangeIndexReader != null;
    int firstRangeId = rangeIndexReader.findRangeId(_predicateEvaluator.getStartDictId());
    int lastRangeId = rangeIndexReader.findRangeId(_predicateEvaluator.getEndDictId() - 1);
    //Handle Matching Ranges - some ranges match fully but some partially
    //below code assumes first and last range always match partially which may not be the case always //todo: optimize it
    ImmutableRoaringBitmap docIdsToScan;
    if (firstRangeId == lastRangeId) {
      docIdsToScan = rangeIndexReader.getDocIds(firstRangeId);
    } else {
      docIdsToScan =
          ImmutableRoaringBitmap.or(rangeIndexReader.getDocIds(firstRangeId), rangeIndexReader.getDocIds(lastRangeId));
    }
    ScanBasedFilterOperator scanBasedFilterOperator =
        new ScanBasedFilterOperator(_predicateEvaluator, _dataSource, _startDocId, _endDocId);
    ScanBasedDocIdSet scanBasedDocIdSet = (ScanBasedDocIdSet) scanBasedFilterOperator.getNextBlock().getBlockDocIdSet();
    MutableRoaringBitmap docIds = scanBasedDocIdSet.iterator().applyAnd(docIdsToScan);

    //All the intermediate ranges will be full match
    for (int rangeId = firstRangeId + 1; rangeId < lastRangeId; rangeId++) {
      docIds.or(rangeIndexReader.getDocIds(rangeId));
    }
    return new FilterBlock(
        new BitmapDocIdSet(new ImmutableRoaringBitmap[]{docIds}, _startDocId, _endDocId, _exclusive) {

          @Override
          public long getNumEntriesScannedInFilter() {
            return scanBasedDocIdSet.getNumEntriesScannedInFilter();
          }
        });
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
