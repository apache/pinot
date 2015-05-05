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

import java.util.ArrayList;
import java.util.List;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import com.linkedin.pinot.core.operator.filter.utils.RangePredicateEvaluator;
import com.linkedin.pinot.core.segment.index.BitmapInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class BitmapBasedFilterOperator extends BaseFilterOperator {
  private static final Logger LOG = LoggerFactory.getLogger(BitmapBasedFilterOperator.class);

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
  public Block nextBlock(BlockId BlockId) {
    Predicate predicate = getPredicate();
    BitmapInvertedIndexReader invertedIndex = (BitmapInvertedIndexReader) dataSource.getInvertedIndex();
    Block dataSourceBlock = dataSource.nextBlock();
    Dictionary dictionary = dataSource.getDictionary();
    List<ImmutableRoaringBitmap> bitmapList = new ArrayList<ImmutableRoaringBitmap>();
    switch (predicate.getType()) {
      case EQ:
        final int valueToLookUP = dictionary.indexOf(((EqPredicate) predicate).getEqualsValue());
        if (valueToLookUP < 0) {
          bitmapList.add(new MutableRoaringBitmap());
        } else {
          bitmapList.add(invertedIndex.getImmutable(valueToLookUP));
        }
        break;
      case NEQ:
        final int neq = dictionary.indexOf(((NEqPredicate) predicate).getNotEqualsValue());
        //TODO:Better to create a MutableRoaringBitmap of all 1 bits and xor with the one for neq, but we dont the size ??
        for (int i = 0; i < dictionary.length(); i++) {
          if (i != neq) {
            bitmapList.add(invertedIndex.getImmutable(i));
          }
        }
        break;
      case IN:
        final String[] inValues = ((InPredicate) predicate).getInRange();
        for (final String value : inValues) {
          final int index = dictionary.indexOf(value);
          if (index >= 0) {
            bitmapList.add(invertedIndex.getImmutable(index));
          }
        }
        break;
      case NOT_IN:
        final String[] notInValues = ((NotInPredicate) predicate).getNotInRange();
        final List<Integer> notInIds = new ArrayList<Integer>();
        for (final String notInValue : notInValues) {
          notInIds.add(new Integer(dictionary.indexOf(notInValue)));
        }

        for (int i = 0; i < dictionary.length(); i++) {
          if (!notInIds.contains(new Integer(i))) {
            bitmapList.add(invertedIndex.getImmutable(i));
          }
        }
        break;
      case RANGE:

        int[] rangeStartEndIndex =
            RangePredicateEvaluator.get().evalStartEndIndex(dictionary, (RangePredicate) predicate);
        int rangeStartIndex = rangeStartEndIndex[0];
        int rangeEndIndex = rangeStartEndIndex[1];
        LOG.info("rangeStartIndex:{}, rangeEndIndex:{}", rangeStartIndex, rangeEndIndex);

        for (int i = rangeStartIndex; i <= rangeEndIndex; i++) {
          ImmutableRoaringBitmap immutable = invertedIndex.getImmutable(i);
          bitmapList.add(immutable);
        }
        break;
      case REGEX:
        throw new UnsupportedOperationException("Regex not supported");
    }
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[bitmapList.size()];
    bitmapList.toArray(bitmaps);
    bitmapBlock = new BitmapBlock(dataSourceBlock.getMetadata(), bitmaps);
    return bitmapBlock;
  }

  @Override
  public boolean close() {
    LOG.info("Time spent in BitmapBasedFilterOperator operator:{} is {}", this, bitmapBlock.bitmapDocIdSet.timeMeasure);
    return true;
  }

  public static class BitmapBlock implements Block {

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
    public BlockDocIdSet getBlockDocIdSet() {
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
