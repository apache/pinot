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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    // At this point, we need to create a list of matching docId ranges. There are two kinds of operators:
    //
    // - "Additive" operators, such as EQ, IN and RANGE build up a list of ranges and merge overlapping/adjacent ones,
    //   clipping the ranges to [startDocId; endDocId]
    //
    // - "Subtractive" operators, such as NEQ and NOT IN build up a list of ranges that do not match and build a list of
    //   matching intervals by subtracting a list of non-matching intervals from the given range of
    //   [startDocId; endDocId]
    //
    // For now, we don't look at the cardinality of the column's dictionary, although we should do that if someone
    // specifies a very large list of IN/NOT IN predicates relative to the column's cardinality or a very large/small
    // range predicate relative to the cardinality. However, as adjacent ranges get merged before returning the final
    // list of ranges, the only drawback is that we use a lot of memory during the filter block evaluation.

    final int[] dictionaryIds;
    boolean additiveRanges = true;

    switch (predicate.getType()) {
      case EQ:
      case IN:
      case RANGE:
        dictionaryIds = evaluator.getMatchingDictionaryIds();
        break;
      case NEQ:
      case NOT_IN:
        additiveRanges = false;
        dictionaryIds = evaluator.getNonMatchingDictionaryIds();
        break;
      case REGEX:
        throw new RuntimeException("Regex is not supported");
      default:
        throw new RuntimeException("Unimplemented!");
    }

    if (0 < dictionaryIds.length) {
      // Sort the dictionaryIds in ascending order, so that their respective ranges are adjacent if their
      // dictionaryIds are adjacent
      Arrays.sort(dictionaryIds);

      IntPair lastPair = invertedIndex.getMinMaxRangeFor(dictionaryIds[0]);
      IntRanges.clip(lastPair, startDocId, endDocId);

      for (int i = 1; i < dictionaryIds.length; i++) {
        IntPair currentPair = invertedIndex.getMinMaxRangeFor(dictionaryIds[i]);
        IntRanges.clip(currentPair, startDocId, endDocId);

        // If the previous range is degenerate, just keep the current one
        if (IntRanges.isInvalid(lastPair)) {
          lastPair = currentPair;
          continue;
        }

        // If the current range is adjacent or overlaps with the previous range, merge it into the previous range,
        // otherwise add the previous range and keep the current one to be added
        if (IntRanges.rangesAreMergeable(lastPair, currentPair)) {
          IntRanges.mergeIntoFirst(lastPair, currentPair);
        } else {
          if (!IntRanges.isInvalid(lastPair)) {
            pairs.add(lastPair);
          }
          lastPair = currentPair;
        }
      }

      // Add the last range if it's valid
      if (!IntRanges.isInvalid(lastPair)) {
        pairs.add(lastPair);
      }
    }

    if (!additiveRanges) {
      // If the ranges are not additive ranges, our list of pairs is a list of "holes" in the [startDocId; endDocId]
      // range. We need to take this list of pairs and invert it. To do so, there are three cases:
      //
      // - No holes, in which case the final range is [startDocId; endDocId]
      // - One or more hole, in which case the final ranges are [startDocId; firstHoleStartDocId - 1] and
      //   [lastHoleEndDocId + 1; endDocId] and ranges in between other holes
      List<IntPair> newPairs = new ArrayList<>();
      if (pairs.isEmpty()) {
        newPairs.add(new IntPair(startDocId, endDocId));
      } else {
        // Add the first filled area (between startDocId and the first hole)
        IntPair firstHole = pairs.get(0);
        IntPair firstRange = new IntPair(startDocId, firstHole.getLeft() - 1);

        if (!IntRanges.isInvalid(firstRange)) {
          newPairs.add(firstRange);
        }

        // Add the filled areas between contiguous holes
        int pairCount = pairs.size();
        for (int i = 1; i < pairCount; i++) {
          IntPair previousHole = pairs.get(i - 1);
          IntPair currentHole = pairs.get(i);
          IntPair range = new IntPair(previousHole.getRight() + 1, currentHole.getLeft() - 1);
          if (!IntRanges.isInvalid(range)) {
            newPairs.add(range);
          }
        }

        // Add the last filled area (between the last hole and endDocId)
        IntPair lastHole = pairs.get(pairs.size() - 1);
        IntPair lastRange = new IntPair(lastHole.getRight() + 1, endDocId);

        if (!IntRanges.isInvalid(lastRange)) {
          newPairs.add(lastRange);
        }
      }

      pairs = newPairs;
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
