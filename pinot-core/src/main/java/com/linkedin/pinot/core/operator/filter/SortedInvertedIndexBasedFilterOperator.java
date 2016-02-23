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

import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.SortedDocIdSet;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import com.linkedin.pinot.core.segment.index.readers.SortedInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class SortedInvertedIndexBasedFilterOperator extends BaseFilterOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SortedInvertedIndexBasedFilterOperator.class);

  private DataSource dataSource;

  private SortedBlock sortedBlock;

  private int startDocId;

  private int endDocId;

  /**
   * 
   * @param dataSource
   * @param startDocId inclusive
   * @param endDocId inclusive
   */
  public SortedInvertedIndexBasedFilterOperator(DataSource dataSource, int startDocId, int endDocId) {
    this.dataSource = dataSource;
    this.startDocId = startDocId;
    this.endDocId = endDocId;
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
    List<IntPair> pairs = new ArrayList<IntPair>();
    PredicateEvaluator evaluator = PredicateEvaluatorProvider.getPredicateFunctionFor(predicate, dictionary);
    int[] dictionaryIds = evaluator.getMatchingDictionaryIds();
    for (int i = 0; i < dictionaryIds.length; i++) {
      IntPair pair = invertedIndex.getMinMaxRangeFor(dictionaryIds[i]);
      //ensure that the pair is between (startDoc,endDoc) else trim/skip it accordingly
      if (startDocId <= pair.getLeft()  && pair.getRight() <= endDocId) {
        pairs.add(pair);
      } else {
        int newStart = Math.max(pair.getLeft(), startDocId);
        int newEnd = Math.min(pair.getRight(), endDocId);
        if (newStart <= newEnd) {
          pairs.add(Pairs.intPair(newStart, newEnd));
        }
      }
    }
    LOGGER.debug("Creating a Sorted Block with pairs: {}", pairs);
    sortedBlock = new SortedBlock(dataSource.getOperatorName(), pairs);
    return sortedBlock;
  }

  @Override
  public boolean close() {
    return true;
  }

  public static class SortedBlock extends BaseFilterBlock {

    private List<IntPair> pairs;
    private SortedDocIdSet sortedDocIdSet;
    private String datasourceName;

    public SortedBlock(String datasourceName, List<IntPair> pairs) {
      this.datasourceName = datasourceName;
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
      sortedDocIdSet = new SortedDocIdSet(datasourceName, pairs);
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
