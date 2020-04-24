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
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.MVScanDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SVScanDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory;
import org.apache.pinot.core.segment.index.readers.RangeIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class RangeFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "RangeFilterOperator";

  private final PredicateEvaluator _predicateEvaluator;
  private final DataSource _dataSource;
  private final int _startDocId;
  private final int _endDocId;
  private final boolean _exclusive;

  public RangeFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int startDocId,
      int endDocId) {
    // NOTE:
    // Predicate that is always evaluated as true or false should not be passed into the BitmapBasedFilterOperator for
    // performance concern.
    // If predicate is always evaluated as true, use MatchAllFilterOperator; if predicate is always evaluated as false,
    // use EmptyFilterOperator.
    Preconditions.checkArgument(!predicateEvaluator.isAlwaysTrue() && !predicateEvaluator.isAlwaysFalse());

    _predicateEvaluator = predicateEvaluator;
    _dataSource = dataSource;
    _startDocId = startDocId;
    _endDocId = endDocId;
    _exclusive = predicateEvaluator.isExclusive();
  }

  @Override
  protected FilterBlock getNextBlock() {

    //only dictionary based is supported for now
    Preconditions.checkState(_predicateEvaluator instanceof RangePredicateEvaluatorFactory.OfflineDictionaryBasedRangePredicateEvaluator);

    RangePredicateEvaluatorFactory.OfflineDictionaryBasedRangePredicateEvaluator evaluator =
        (RangePredicateEvaluatorFactory.OfflineDictionaryBasedRangePredicateEvaluator) _predicateEvaluator;

    RangeIndexReader rangeIndexReader = (RangeIndexReader) _dataSource.getRangeIndex();
    int startRangeId = rangeIndexReader.findRangeId(evaluator.getStartDictId());
    int endRangeId = rangeIndexReader.findRangeId(evaluator.getEndDictId());
    //Handle Matching Ranges - some ranges match fully but some partially
    //below code assumes first and last range always match partially which may not be the case always //todo: optimize it
    MutableRoaringBitmap mutableRoaringBitmap = new MutableRoaringBitmap();
    mutableRoaringBitmap.or(rangeIndexReader.getDocIds(startRangeId));
    if (endRangeId != startRangeId) {
      mutableRoaringBitmap.or(rangeIndexReader.getDocIds(endRangeId));
    }
    final ScanBasedFilterOperator scanBasedFilterOperator =
        new ScanBasedFilterOperator(_predicateEvaluator, _dataSource, _startDocId, _endDocId);
    FilterBlockDocIdSet scanBlockDocIdSet = scanBasedFilterOperator.getNextBlock().getBlockDocIdSet();
    BlockDocIdIterator iterator = scanBlockDocIdSet.iterator();

    List<ImmutableRoaringBitmap> bitmapList = new ArrayList<>();
    if (_dataSource.getDataSourceMetadata().isSingleValue()) {
      bitmapList.add(((SVScanDocIdIterator) iterator).applyAnd(mutableRoaringBitmap));
    } else {
      bitmapList.add(((MVScanDocIdIterator) iterator).applyAnd(mutableRoaringBitmap));
    }

    //All the intermediate ranges will be full match
    for (int rangeId = startRangeId + 1; rangeId < endRangeId; rangeId++) {
      bitmapList.add(rangeIndexReader.getDocIds(rangeId));
    }
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[bitmapList.size()];
    bitmapList.toArray(bitmaps);
    return new FilterBlock(new BitmapDocIdSet(bitmaps, _startDocId, _endDocId, _exclusive) {

      @Override
      public long getNumEntriesScannedInFilter() {
        return scanBlockDocIdSet.getNumEntriesScannedInFilter();
      }
    });
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
