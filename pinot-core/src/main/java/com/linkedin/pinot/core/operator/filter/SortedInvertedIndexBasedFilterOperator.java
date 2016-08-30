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

import com.linkedin.pinot.common.utils.DocIdRange;
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
    List<DocIdRange> docIdRanges = new ArrayList<DocIdRange>();
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

    DocIdRange minMaxDocIdRange = new DocIdRange(startDocId, endDocId);

    if (0 < dictionaryIds.length) {
      // Sort the dictionaryIds in ascending order, so that their respective ranges are adjacent if their
      // dictionaryIds are adjacent
      Arrays.sort(dictionaryIds);

      DocIdRange previousDocIdRange = invertedIndex.getMinMaxRangeFor(dictionaryIds[0]);
      previousDocIdRange.clip(minMaxDocIdRange);

      for (int i = 1; i < dictionaryIds.length; i++) {
        DocIdRange currentDocIdRange = invertedIndex.getMinMaxRangeFor(dictionaryIds[i]);
        currentDocIdRange.clip(minMaxDocIdRange);

        // If the previous range is invalid, just keep the current one
        if (previousDocIdRange.isInvalid()) {
          previousDocIdRange = currentDocIdRange;
          continue;
        }

        // If the current range is adjacent or overlaps with the previous range, merge it into the previous range,
        // otherwise add the previous range and keep the current one to be added
        if (previousDocIdRange.rangeIsMergeable(currentDocIdRange)) {
          previousDocIdRange.mergeWithRange(currentDocIdRange);
        } else {
          if (!previousDocIdRange.isInvalid()) {
            docIdRanges.add(previousDocIdRange);
          }
          previousDocIdRange = currentDocIdRange;
        }
      }

      // Add the last range if it's valid
      if (!previousDocIdRange.isInvalid()) {
        docIdRanges.add(previousDocIdRange);
      }
    }

    if (!additiveRanges) {
      // If the ranges are not additive ranges, our list of pairs is a list of "holes" in the [startDocId; endDocId]
      // range. We need to take this list of pairs and invert it. To do so, there are three cases:
      //
      // - No holes, in which case the final range is [startDocId; endDocId]
      // - One or more hole, in which case the final ranges are [startDocId; firstHoleStartDocId - 1] and
      //   [lastHoleEndDocId + 1; endDocId] and ranges in between other holes
      List<DocIdRange> invertedDocIdRanges = new ArrayList<>();
      if (docIdRanges.isEmpty()) {
        invertedDocIdRanges.add(new DocIdRange(startDocId, endDocId));
      } else {
        // Add the first filled area (between startDocId and the first hole)
        DocIdRange firstHole = docIdRanges.get(0);
        DocIdRange firstRange = new DocIdRange(startDocId, firstHole.getStart() - 1);

        if (!firstRange.isInvalid()) {
          invertedDocIdRanges.add(firstRange);
        }

        // Add the filled areas between contiguous holes
        int pairCount = docIdRanges.size();
        for (int i = 1; i < pairCount; i++) {
          DocIdRange previousHole = docIdRanges.get(i - 1);
          DocIdRange currentHole = docIdRanges.get(i);
          DocIdRange range = new DocIdRange(previousHole.getEnd() + 1, currentHole.getStart() - 1);
          if (!range.isInvalid()) {
            invertedDocIdRanges.add(range);
          }
        }

        // Add the last filled area (between the last hole and endDocId)
        DocIdRange lastHole = docIdRanges.get(docIdRanges.size() - 1);
        DocIdRange lastRange = new DocIdRange(lastHole.getEnd() + 1, endDocId);

        if (!lastRange.isInvalid()) {
          invertedDocIdRanges.add(lastRange);
        }
      }

      docIdRanges = invertedDocIdRanges;
    }

    LOGGER.debug("Creating a Sorted Block with docIdRanges: {}", docIdRanges);
    sortedBlock = new SortedBlock(dataSource.getOperatorName(), docIdRanges);
    return sortedBlock;
  }

  @Override
  public boolean close() {
    return true;
  }

  public static class SortedBlock extends BaseFilterBlock {

    private List<DocIdRange> pairs;
    private SortedDocIdSet sortedDocIdSet;
    private String datasourceName;

    public SortedBlock(String datasourceName, List<DocIdRange> pairs) {
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
