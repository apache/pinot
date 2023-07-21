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
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.SortedDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory.SortedDictionaryBasedRangePredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.SortedIndexReader;
import org.apache.pinot.spi.utils.Pairs.IntPair;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class SortedIndexBasedFilterOperator extends NullHandlingSupportedSingleColumnLeafFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_SORTED_INDEX";

  private final PredicateEvaluator _predicateEvaluator;
  private final SortedIndexReader<?> _sortedIndexReader;

  SortedIndexBasedFilterOperator(QueryContext queryContext, PredicateEvaluator predicateEvaluator,
      DataSource dataSource, int numDocs) {
    super(queryContext, dataSource, numDocs);
    _predicateEvaluator = predicateEvaluator;
    _sortedIndexReader = (SortedIndexReader<?>) dataSource.getInvertedIndex();
  }

  @Override
  protected BlockDocIdSet getNextBlockWithoutNullHandling() {
    // At this point, we need to create a list of matching docIdRanges.
    //
    // There are two kinds of operators:
    // - "Additive" operators (EQ, IN, RANGE): Build up a list of matching docIdRanges with adjacent ones merged.
    // - "Subtractive" operators (NEQ, NOT IN): Build up a list of non-matching docIdRanges with adjacent ones merged,
    //   then subtract them from the range of [0, numDocs) to get a list of matching docIdRanges.

    if (_predicateEvaluator instanceof SortedDictionaryBasedRangePredicateEvaluator) {
      // For RANGE predicate, use start/end document id to construct a new document id range
      SortedDictionaryBasedRangePredicateEvaluator rangePredicateEvaluator =
          (SortedDictionaryBasedRangePredicateEvaluator) _predicateEvaluator;
      int startDocId = _sortedIndexReader.getDocIds(rangePredicateEvaluator.getStartDictId()).getLeft();
      // NOTE: End dictionary id is exclusive in OfflineDictionaryBasedRangePredicateEvaluator.
      int endDocId = _sortedIndexReader.getDocIds(rangePredicateEvaluator.getEndDictId() - 1).getRight();
      return new SortedDocIdSet(Collections.singletonList(new IntPair(startDocId, endDocId)));
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
          return new SortedDocIdSet(docIdRanges);
        } else {
          return new SortedDocIdSet(Collections.singletonList(docIdRange));
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

        return new SortedDocIdSet(docIdRanges);
      }
    }
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    int count = 0;
    boolean exclusive = _predicateEvaluator.isExclusive();
    if (_predicateEvaluator instanceof SortedDictionaryBasedRangePredicateEvaluator) {
      // For RANGE predicate, use start/end document id to construct a new document id range
      SortedDictionaryBasedRangePredicateEvaluator rangePredicateEvaluator =
          (SortedDictionaryBasedRangePredicateEvaluator) _predicateEvaluator;
      int startDocId = _sortedIndexReader.getDocIds(rangePredicateEvaluator.getStartDictId()).getLeft();
      // NOTE: End dictionary id is exclusive in OfflineDictionaryBasedRangePredicateEvaluator.
      int endDocId = _sortedIndexReader.getDocIds(rangePredicateEvaluator.getEndDictId() - 1).getRight();
      count = endDocId - startDocId + 1;
    } else {
      int[] dictIds =
          exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();
      int numDictIds = dictIds.length;
      // NOTE: PredicateEvaluator without matching/non-matching dictionary ids should not reach here.
      Preconditions.checkState(numDictIds > 0);
      if (numDictIds == 1) {
        IntPair docIdRange = _sortedIndexReader.getDocIds(dictIds[0]);
        count = docIdRange.getRight() - docIdRange.getLeft() + 1;
      } else {
        IntPair lastDocIdRange = _sortedIndexReader.getDocIds(dictIds[0]);
        for (int i = 1; i < numDictIds; i++) {
          IntPair docIdRange = _sortedIndexReader.getDocIds(dictIds[i]);
          if (docIdRange.getLeft() == lastDocIdRange.getRight() + 1) {
            lastDocIdRange.setRight(docIdRange.getRight());
          } else {
            count += lastDocIdRange.getRight() - lastDocIdRange.getLeft() + 1;
            lastDocIdRange = docIdRange;
          }
        }
        count += lastDocIdRange.getRight() - lastDocIdRange.getLeft() + 1;
      }
    }
    return exclusive ? _numDocs - count : count;
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    boolean exclusive = _predicateEvaluator.isExclusive();
    if (_predicateEvaluator instanceof SortedDictionaryBasedRangePredicateEvaluator) {
      // For RANGE predicate, use start/end document id to construct a new document id range
      SortedDictionaryBasedRangePredicateEvaluator rangePredicateEvaluator =
          (SortedDictionaryBasedRangePredicateEvaluator) _predicateEvaluator;
      int startDocId = _sortedIndexReader.getDocIds(rangePredicateEvaluator.getStartDictId()).getLeft();
      // NOTE: End dictionary id is exclusive in OfflineDictionaryBasedRangePredicateEvaluator.
      int endDocId = _sortedIndexReader.getDocIds(rangePredicateEvaluator.getEndDictId() - 1).getRight();
      bitmap.add(startDocId, endDocId + 1L);
    } else {
      int[] dictIds =
          exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();
      int numDictIds = dictIds.length;
      // NOTE: PredicateEvaluator without matching/non-matching dictionary ids should not reach here.
      Preconditions.checkState(numDictIds > 0);
      if (numDictIds == 1) {
        IntPair docIdRange = _sortedIndexReader.getDocIds(dictIds[0]);
        bitmap.add(docIdRange.getLeft(), docIdRange.getRight() + 1L);
      } else {
        IntPair lastDocIdRange = _sortedIndexReader.getDocIds(dictIds[0]);
        for (int i = 1; i < numDictIds; i++) {
          IntPair docIdRange = _sortedIndexReader.getDocIds(dictIds[i]);
          if (docIdRange.getLeft() == lastDocIdRange.getRight() + 1) {
            lastDocIdRange.setRight(docIdRange.getRight());
          } else {
            bitmap.add(lastDocIdRange.getLeft(), lastDocIdRange.getRight() + 1L);
            lastDocIdRange = docIdRange;
          }
        }
        bitmap.add(lastDocIdRange.getLeft(), lastDocIdRange.getRight() + 1L);
      }
    }
    return new BitmapCollection(_numDocs, exclusive, bitmap);
  }


  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:sorted_index");
    stringBuilder.append(",operator:").append(_predicateEvaluator.getPredicateType());
    stringBuilder.append(",predicate:").append(_predicateEvaluator.getPredicate().toString());
    return stringBuilder.append(')').toString();
  }
}
