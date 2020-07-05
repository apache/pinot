/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.filter;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.utils.Pairs.IntPair;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.segment.index.readers.SortedIndexReader;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.SortedDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory.OfflineDictionaryBasedRangePredicateEvaluator;


public class SortedIndexBasedFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "SortedIndexBasedFilterOperator";

  private final PredicateEvaluator _predicateEvaluator;
  private final SortedIndexReader<?> _sortedIndexReader;
  private final int _numDocs;

  SortedIndexBasedFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int numDocs) {
    _predicateEvaluator = predicateEvaluator;
    _sortedIndexReader = (SortedIndexReader<?>) dataSource.getInvertedIndex();
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    // At this point, we need to create a list of matching docIdRanges.
    //
    // There are two kinds of operators:
    // - "Additive" operators (EQ, IN, RANGE): Build up a list of matching docIdRanges with adjacent ones merged.
    // - "Subtractive" operators (NEQ, NOT IN): Build up a list of non-matching docIdRanges with adjacent ones merged,
    //   then subtract them from the range of [0, numDocs) to get a list of matching docIdRanges.

    if (_predicateEvaluator instanceof OfflineDictionaryBasedRangePredicateEvaluator) {
      // For RANGE predicate, use start/end document id to construct a new document id range
      OfflineDictionaryBasedRangePredicateEvaluator rangePredicateEvaluator =
          (OfflineDictionaryBasedRangePredicateEvaluator) _predicateEvaluator;
      int startDocId = _sortedIndexReader.getDocIds(rangePredicateEvaluator.getStartDictId()).getLeft();
      // NOTE: End dictionary id is exclusive in OfflineDictionaryBasedRangePredicateEvaluator.
      int endDocId = _sortedIndexReader.getDocIds(rangePredicateEvaluator.getEndDictId() - 1).getRight();
      return new FilterBlock(new SortedDocIdSet(Collections.singletonList(new IntPair(startDocId, endDocId))));
    } else {
      boolean exclusive = _predicateEvaluator.isExclusive();
      int[] dictIds =
          exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();
      int numDictIds = dictIds.length;
      // NOTE: PredicateEvaluator without matching/non-matching dictionary ids should not reach here.
      Preconditions.checkState(numDictIds > 0);
      if (numDictIds == 1) {
        IntPair docIdRange = _sortedIndexReader.getDocIds(dictIds[0]);
        if (exclusive) {
          // NOTE: docIdRange has inclusive start and end.
          List<IntPair> docIdRanges = new ArrayList<>(2);
          int firstDocId = docIdRange.getLeft();
          if (firstDocId > 0) {
            docIdRanges.add(new IntPair(0, firstDocId - 1));
          }
          int lastDocId = docIdRange.getRight();
          if (lastDocId < _numDocs - 1) {
            docIdRanges.add(new IntPair(lastDocId + 1, _numDocs - 1));
          }
          return new FilterBlock(new SortedDocIdSet(docIdRanges));
        } else {
          return new FilterBlock(new SortedDocIdSet(Collections.singletonList(docIdRange)));
        }
      } else {
        // Sort the dictIds in ascending order so that their respective docIdRanges are adjacent if they are adjacent
        Arrays.sort(dictIds);

        // Merge adjacent docIdRanges
        List<IntPair> docIdRanges = new ArrayList<>();
        IntPair lastDocIdRange = _sortedIndexReader.getDocIds(dictIds[0]);
        for (int i = 1; i < numDictIds; i++) {
          IntPair docIdRange = _sortedIndexReader.getDocIds(dictIds[i]);
          // NOTE: docIdRange has inclusive start and end.
          if (docIdRange.getLeft() == lastDocIdRange.getRight() + 1) {
            lastDocIdRange.setRight(docIdRange.getRight());
          } else {
            docIdRanges.add(lastDocIdRange);
            lastDocIdRange = docIdRange;
          }
        }
        docIdRanges.add(lastDocIdRange);

        if (exclusive) {
          // Invert the docIdRanges
          int numDocIdRanges = docIdRanges.size();
          List<IntPair> invertedDocIdRanges = new ArrayList<>(numDocIdRanges + 1);
          // NOTE: docIdRange has inclusive start and end.
          int firstDocId = docIdRanges.get(0).getLeft();
          if (firstDocId > 0) {
            invertedDocIdRanges.add(new IntPair(0, firstDocId - 1));
          }
          for (int i = 0; i < numDocIdRanges - 1; i++) {
            invertedDocIdRanges
                .add(new IntPair(docIdRanges.get(i).getRight() + 1, docIdRanges.get(i + 1).getLeft() - 1));
          }
          int lastDocId = docIdRanges.get(numDocIdRanges - 1).getRight();
          if (lastDocId < _numDocs - 1) {
            invertedDocIdRanges.add(new IntPair(lastDocId + 1, _numDocs - 1));
          }
          docIdRanges = invertedDocIdRanges;
        }

        return new FilterBlock(new SortedDocIdSet(docIdRanges));
      }
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
