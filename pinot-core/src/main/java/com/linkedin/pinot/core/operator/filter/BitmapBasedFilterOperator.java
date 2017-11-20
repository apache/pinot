/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.filter;

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.blocks.BitmapBlock;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import java.util.ArrayList;
import java.util.List;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BitmapBasedFilterOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(BitmapBasedFilterOperator.class);
  private static final String OPERATOR_NAME = "BitmapBasedFilterOperator";

  private final int _startDocId;
  // TODO: change it to exclusive
  // Inclusive
  private final int _endDocId;
  private final boolean _exclusive;

  private PredicateEvaluator _predicateEvaluator;
  private DataSource _dataSource;
  private ImmutableRoaringBitmap[] _bitmaps;

  public BitmapBasedFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int startDocId,
      int endDocId) {
    _predicateEvaluator = predicateEvaluator;
    _dataSource = dataSource;
    _startDocId = startDocId;
    _endDocId = endDocId;
    _exclusive = predicateEvaluator.isExclusive();
  }

  public BitmapBasedFilterOperator(ImmutableRoaringBitmap[] bitmaps, int startDocId, int endDocId, boolean exclusive) {
    _bitmaps = bitmaps;
    _startDocId = startDocId;
    _endDocId = endDocId;
    _exclusive = exclusive;
  }

  @Override
  protected BaseFilterBlock getNextBlock() {
    if (_bitmaps != null) {
      return new BitmapBlock(_bitmaps, _startDocId, _endDocId, _exclusive);
    }

    int[] dictIds = _exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();

    // For realtime use case, it is possible that inverted index has not yet generated for the given dict id, so we
    // filter out null bitmaps
    InvertedIndexReader invertedIndex = _dataSource.getInvertedIndex();
    int length = dictIds.length;
    List<ImmutableRoaringBitmap> bitmaps = new ArrayList<>(length);
    for (int dictId : dictIds) {
      ImmutableRoaringBitmap bitmap = (ImmutableRoaringBitmap) invertedIndex.getDocIds(dictId);
      if (bitmap != null) {
        bitmaps.add(bitmap);
      }
    }

    // Log size diff to verify the fix
    int numBitmaps = bitmaps.size();
    if (numBitmaps != length) {
      LOGGER.info("Not all inverted indexes are generated, numDictIds: {}, numBitmaps: {}", length, numBitmaps);
    }

    return new BitmapBlock(bitmaps.toArray(new ImmutableRoaringBitmap[numBitmaps]), _startDocId, _endDocId, _exclusive);
  }

  @Override
  public boolean isResultEmpty() {
    return _predicateEvaluator != null && _predicateEvaluator.isAlwaysFalse();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
