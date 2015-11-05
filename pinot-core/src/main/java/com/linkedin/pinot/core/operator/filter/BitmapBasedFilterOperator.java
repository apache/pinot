/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.BaseFilterBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class BitmapBasedFilterOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(BitmapBasedFilterOperator.class);

  private DataSource dataSource;
  private BitmapBlock bitmapBlock;

  public BitmapBasedFilterOperator(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public BaseFilterBlock nextFilterBlock(BlockId BlockId) {
    Predicate predicate = getPredicate();
    InvertedIndexReader invertedIndex = dataSource.getInvertedIndex();
    Block dataSourceBlock = dataSource.nextBlock();
    Dictionary dictionary = dataSource.getDictionary();
    PredicateEvaluator evaluator = PredicateEvaluatorProvider.getPredicateFunctionFor(predicate, dictionary);
    int[] dictionaryIds = evaluator.getMatchingDictionaryIds();
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[dictionaryIds.length];
    for (int i = 0; i < dictionaryIds.length; i++) {
      bitmaps[i] = invertedIndex.getImmutable(dictionaryIds[i]);
    }
    bitmapBlock = new BitmapBlock(dataSourceBlock.getMetadata(), bitmaps);
    return bitmapBlock;
  }

  @Override
  public boolean close() {
    LOGGER.info("Time spent in BitmapBasedFilterOperator operator:{} is {}", this,
        bitmapBlock.bitmapDocIdSet.timeMeasure);
    return true;
  }

  public static class BitmapBlock extends BaseFilterBlock {

    private final ImmutableRoaringBitmap[] bitmaps;
    private BitmapDocIdSet bitmapDocIdSet;
    private BlockMetadata blockMetadata;

    public BitmapBlock(BlockMetadata blockMetadata, ImmutableRoaringBitmap[] bitmaps) {
      this.blockMetadata = blockMetadata;
      this.bitmaps = bitmaps;
    }

    @Override
    public BlockId getId() {
      return new BlockId(0);
    }

    @Override
    public boolean applyPredicate(Predicate predicate) {
      throw new UnsupportedOperationException("applypredicate not supported in " + this.getClass());
    }

    @Override
    public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
      bitmapDocIdSet = new BitmapDocIdSet(blockMetadata, bitmaps);
      return bitmapDocIdSet;
    }

    @Override
    public BlockValSet getBlockValueSet() {
      throw new UnsupportedOperationException("getBlockValueSet not supported in " + this.getClass());
    }

    @Override
    public BlockDocIdValueSet getBlockDocIdValueSet() {
      throw new UnsupportedOperationException("getBlockDocIdValueSet not supported in " + this.getClass());
    }

    @Override
    public BlockMetadata getMetadata() {
      throw new UnsupportedOperationException("getMetadata not supported in " + this.getClass());
    }

  }

}
