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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.BaseFilterBlock;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.docidsets.SortedDocIdSet;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import com.linkedin.pinot.core.segment.index.readers.SortedInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class SortedInvertedIndexBasedFilterOperator extends BaseFilterOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SortedInvertedIndexBasedFilterOperator.class);

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
  public BaseFilterBlock nextFilterBlock(BlockId BlockId) {
    Predicate predicate = getPredicate();
    final SortedInvertedIndexReader invertedIndex = (SortedInvertedIndexReader) dataSource.getInvertedIndex();
    Dictionary dictionary = dataSource.getDictionary();
    List<Pair<Integer, Integer>> pairs = new ArrayList<Pair<Integer, Integer>>();
    PredicateEvaluator evaluator = PredicateEvaluatorProvider.getPredicateFunctionFor(predicate, dictionary);
    int[] dictionaryIds = evaluator.getMatchingDictionaryIds();
    Arrays.sort(dictionaryIds);
    for (int i = 0; i < dictionaryIds.length; i++) {
      int[] minMax = invertedIndex.getMinMaxRangeFor(dictionaryIds[i]);
      pairs.add(ImmutablePair.of(minMax[0], minMax[1]));
    }
    LOGGER.debug("Creating a Sorted Block with pairs: {}", pairs);
    sortedBlock = new SortedBlock(pairs);
    return sortedBlock;
  }

  @Override
  public boolean close() {
    return true;
  }

  public static class SortedBlock extends BaseFilterBlock {

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
    public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
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
