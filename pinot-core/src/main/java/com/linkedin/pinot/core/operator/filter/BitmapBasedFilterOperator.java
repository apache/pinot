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

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.blocks.BitmapBlock;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import java.util.ArrayList;
import java.util.List;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BitmapBasedFilterOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(BitmapBasedFilterOperator.class);
  private static final String OPERATOR_NAME = "BitmapBasedFilterOperator";

  private final PredicateEvaluator predicateEvaluator;
  private final Predicate predicate;

  private DataSource dataSource;
  private BitmapBlock bitmapBlock;

  private int startDocId;

  private int endDocId;

  /**
   * @param predicate
   * @param dataSource
   * @param startDocId inclusive
   * @param endDocId inclusive
   */
  public BitmapBasedFilterOperator(Predicate predicate, DataSource dataSource, int startDocId, int endDocId) {
    this.predicate = predicate;
    this.predicateEvaluator = PredicateEvaluatorProvider.getPredicateFunctionFor(predicate, dataSource);
    this.dataSource = dataSource;
    this.startDocId = startDocId;
    this.endDocId = endDocId;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  protected BaseFilterBlock getNextBlock() {
    InvertedIndexReader invertedIndex = dataSource.getInvertedIndex();
    Block dataSourceBlock = dataSource.nextBlock();
    int[] dictionaryIds;
    boolean exclusion = false;
    switch (predicate.getType()) {
      case EQ:
      case IN:
      case RANGE:
        dictionaryIds = predicateEvaluator.getMatchingDictionaryIds();
        break;

      case NEQ:
      case NOT_IN:
        exclusion = true;
        dictionaryIds = predicateEvaluator.getNonMatchingDictionaryIds();
        break;
      case REGEXP_LIKE:
      default:
        throw new UnsupportedOperationException("Regex is not supported");
    }

    // For realtime use case, it is possible that inverted index has not yet generated for the given dict id, so we
    // filter out null bitmaps
    int length = dictionaryIds.length;
    List<ImmutableRoaringBitmap> bitmaps = new ArrayList<>(length);
    for (int dictionaryId : dictionaryIds) {
      ImmutableRoaringBitmap bitmap = (ImmutableRoaringBitmap) invertedIndex.getDocIds(dictionaryId);
      if (bitmap != null) {
        bitmaps.add(bitmap);
      }
    }

    // Log size diff to verify the fix
    int numBitmaps = bitmaps.size();
    if (numBitmaps != length) {
      LOGGER.info("Not all inverted indexes are generated, numDictIds: {}, numBitmaps: {}", length, numBitmaps);
    }

    bitmapBlock = new BitmapBlock(dataSource.getOperatorName(), dataSourceBlock.getMetadata(), startDocId, endDocId,
        bitmaps.toArray(new ImmutableRoaringBitmap[numBitmaps]), exclusion);
    return bitmapBlock;
  }

  @Override
  public boolean isResultEmpty() {
    return predicateEvaluator.alwaysFalse();
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
