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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.SortedDocIdSet;
import com.linkedin.pinot.core.segment.index.BitmapInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.SortedInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class SortedInvertedIndexBasedFilterOperator extends BaseFilterOperator {

  private static final Logger LOG = LoggerFactory.getLogger(SortedInvertedIndexBasedFilterOperator.class);

  private DataSource dataSource;

  private SortedBlock sortedBlock;

  public SortedInvertedIndexBasedFilterOperator(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    Predicate predicate = getPredicate();
    final SortedInvertedIndexReader invertedIndex = (SortedInvertedIndexReader) dataSource.getInvertedIndex();
    Dictionary dictionary = dataSource.getDictionary();
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    List<Pair<Integer, Integer>> pairs = new ArrayList<Pair<Integer, Integer>>();
    switch (predicate.getType()) {
      case EQ:
        final int valueToLookUP = dictionary.indexOf(((EqPredicate) predicate).getEqualsValue());
        if (valueToLookUP >= 0) {
          int[] minMax = invertedIndex.getMinMaxRangeFor(valueToLookUP);
          pairs.add(ImmutablePair.of(minMax[0], minMax[1]));
        }
        break;
      case NEQ:
        final int neq = dictionary.indexOf(((NEqPredicate) predicate).getNotEqualsValue());
        for (int i = 0; i < dictionary.length(); i++) {
          if (i != neq) {
            int[] minMax = invertedIndex.getMinMaxRangeFor(i);
            pairs.add(ImmutablePair.of(minMax[0], minMax[1]));
          }
        }
        break;
      case IN:
        final String[] inValues = ((InPredicate) predicate).getInRange();
        for (final String value : inValues) {
          final int index = dictionary.indexOf(value);
          if (index >= 0) {
            int[] minMax = invertedIndex.getMinMaxRangeFor(index);
            pairs.add(ImmutablePair.of(minMax[0], minMax[1]));
          }
        }
        break;
      case NOT_IN:
        final String[] notInValues = ((NotInPredicate) predicate).getNotInRange();
        final List<Integer> notInIds = new ArrayList<Integer>();
        for (final String notInValue : notInValues) {
          notInIds.add(new Integer(dictionary.indexOf(notInValue)));
        }

        final MutableRoaringBitmap notINHolder = new MutableRoaringBitmap();

        for (int i = 0; i < dictionary.length(); i++) {
          if (!notInIds.contains(new Integer(i))) {
            int[] minMax = invertedIndex.getMinMaxRangeFor(i);
            pairs.add(ImmutablePair.of(minMax[0], minMax[1]));
          }
        }
        break;
      case RANGE:

        int rangeStartIndex = 0;
        int rangeEndIndex = 0;

        final boolean incLower = ((RangePredicate) predicate).includeLowerBoundary();
        final boolean incUpper = ((RangePredicate) predicate).includeUpperBoundary();
        final String lower = ((RangePredicate) predicate).getLowerBoundary();
        final String upper = ((RangePredicate) predicate).getUpperBoundary();

        if (lower.equals("*")) {
          rangeStartIndex = 0;
        } else {
          rangeStartIndex = dictionary.indexOf(lower);
        }

        if (upper.equals("*")) {
          rangeEndIndex = dictionary.length() - 1;
        } else {
          rangeEndIndex = dictionary.indexOf(upper);
        }
        if (rangeStartIndex < 0) {
          rangeStartIndex = -(rangeStartIndex + 1);
        } else if (!incLower && !lower.equals("*")) {
          rangeStartIndex += 1;
        }

        if (rangeEndIndex < 0) {
          rangeEndIndex = -(rangeEndIndex + 1);
          rangeEndIndex = Math.max(0, rangeEndIndex - 1);
        } else if (!incUpper && !upper.equals("*")) {
          rangeEndIndex -= 1;
        }

        for (int i = rangeStartIndex; i <= rangeEndIndex; i++) {
          int[] minMax = invertedIndex.getMinMaxRangeFor(i);
          pairs.add(ImmutablePair.of(minMax[0], minMax[1]));
        }
        break;
      case REGEX:
        throw new UnsupportedOperationException("Regex not supported");
    }
    sortedBlock = new SortedBlock(pairs);
    return sortedBlock;
  }

  @Override
  public boolean close() {
    LOG.info("Time spent in SortedInvertedIndexBasedFilterOperator operator:{} is {}", this, sortedBlock.sortedDocIdSet.timeMeasure);
    return true;
  }

  public static class SortedBlock implements Block {

    private List<Pair<Integer, Integer>> pairs;
    private SortedDocIdSet sortedDocIdSet;

    public SortedBlock(List<Pair<Integer, Integer>> pairs) {
      this.pairs = pairs;
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
      sortedDocIdSet = new SortedDocIdSet(pairs);
      return sortedDocIdSet;
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
