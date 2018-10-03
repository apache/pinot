/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.operator.blocks.FilterBlock;
import com.linkedin.pinot.core.operator.docidsets.SortedDocIdSet;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SortedInvertedIndexBasedFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "SortedInvertedIndexBasedFilterOperator";

  private final PredicateEvaluator _predicateEvaluator;
  private final DataSource _dataSource;
  private final int _startDocId;
  // TODO: change it to exclusive
  // Inclusive
  private final int _endDocId;

  public SortedInvertedIndexBasedFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource,
      int startDocId, int endDocId) {
    _predicateEvaluator = predicateEvaluator;
    _dataSource = dataSource;
    _startDocId = startDocId;
    _endDocId = endDocId;
  }

  @Override
  protected FilterBlock getNextBlock() {
    SortedIndexReader invertedIndex = (SortedIndexReader) _dataSource.getInvertedIndex();
    List<IntPair> pairs = new ArrayList<>();

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

    boolean exclusive = _predicateEvaluator.isExclusive();
    int[] dictIds = exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();

    if (0 < dictIds.length) {
      // Sort the dictIds in ascending order, so that their respective ranges are adjacent if their dictIds are adjacent
      Arrays.sort(dictIds);

      IntPair lastPair = invertedIndex.getDocIds(dictIds[0]);
      IntRanges.clip(lastPair, _startDocId, _endDocId);

      for (int i = 1; i < dictIds.length; i++) {
        IntPair currentPair = invertedIndex.getDocIds(dictIds[i]);
        IntRanges.clip(currentPair, _startDocId, _endDocId);

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

    if (exclusive) {
      // If the ranges are not additive ranges, our list of pairs is a list of "holes" in the [startDocId; endDocId]
      // range. We need to take this list of pairs and invert it. To do so, there are three cases:
      //
      // - No holes, in which case the final range is [startDocId; endDocId]
      // - One or more hole, in which case the final ranges are [startDocId; firstHoleStartDocId - 1] and
      //   [lastHoleEndDocId + 1; endDocId] and ranges in between other holes
      List<IntPair> newPairs = new ArrayList<>();
      if (pairs.isEmpty()) {
        newPairs.add(new IntPair(_startDocId, _endDocId));
      } else {
        // Add the first filled area (between startDocId and the first hole)
        IntPair firstHole = pairs.get(0);
        IntPair firstRange = new IntPair(_startDocId, firstHole.getLeft() - 1);

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
        IntPair lastRange = new IntPair(lastHole.getRight() + 1, _endDocId);

        if (!IntRanges.isInvalid(lastRange)) {
          newPairs.add(lastRange);
        }
      }

      pairs = newPairs;
    }

    return new FilterBlock(new SortedDocIdSet(_dataSource.getOperatorName(), pairs));
  }

  @Override
  public boolean isResultEmpty() {
    return _predicateEvaluator.isAlwaysFalse();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
