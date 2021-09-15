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

import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory.DoubleRawValueBasedRangePredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory.FloatRawValueBasedRangePredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory.IntRawValueBasedRangePredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory.LongRawValueBasedRangePredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory.SortedDictionaryBasedRangePredicateEvaluator;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class RangeIndexBasedFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "RangeFilterOperator";

  // NOTE: Range index can only apply to dictionary-encoded columns for now
  // TODO: Support raw index columns
  private final PredicateEvaluator _rangePredicateEvaluator;
  private final DataSource _dataSource;
  private final int _numDocs;

  public RangeIndexBasedFilterOperator(PredicateEvaluator rangePredicateEvaluator, DataSource dataSource, int numDocs) {
    _rangePredicateEvaluator = rangePredicateEvaluator;
    _dataSource = dataSource;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    @SuppressWarnings("unchecked")
    RangeIndexReader<ImmutableRoaringBitmap> rangeIndexReader =
        (RangeIndexReader<ImmutableRoaringBitmap>) _dataSource.getRangeIndex();
    assert rangeIndexReader != null;

    ImmutableRoaringBitmap matches;
    // if the implementation cannot match the entire query exactly, it will
    // yield partial matches, which need to be verified by scanning. If it
    // can answer the query exactly, this will be null.
    ImmutableRoaringBitmap partialMatches;
    int firstRangeId;
    int lastRangeId;
    if (_rangePredicateEvaluator instanceof SortedDictionaryBasedRangePredicateEvaluator) {
      // NOTE: End dictionary id is exclusive in OfflineDictionaryBasedRangePredicateEvaluator.
      matches = rangeIndexReader.getMatchingDocIds(
          ((SortedDictionaryBasedRangePredicateEvaluator) _rangePredicateEvaluator).getStartDictId(),
          ((SortedDictionaryBasedRangePredicateEvaluator) _rangePredicateEvaluator).getEndDictId() - 1);
      partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(
          ((SortedDictionaryBasedRangePredicateEvaluator) _rangePredicateEvaluator).getStartDictId(),
          ((SortedDictionaryBasedRangePredicateEvaluator) _rangePredicateEvaluator).getEndDictId() - 1);
    } else {
      switch (_rangePredicateEvaluator.getDataType()) {
        case INT:
          matches = rangeIndexReader.getMatchingDocIds(
              ((IntRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).geLowerBound(),
              ((IntRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).getUpperBound());
          partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(
              ((IntRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).geLowerBound(),
              ((IntRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).getUpperBound());
          break;
        case LONG:
          matches = rangeIndexReader.getMatchingDocIds(
              ((LongRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).geLowerBound(),
              ((LongRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).getUpperBound());
          partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(
              ((LongRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).geLowerBound(),
              ((LongRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).getUpperBound());
          break;
        case FLOAT:
          matches = rangeIndexReader.getMatchingDocIds(
              ((FloatRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).geLowerBound(),
              ((FloatRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).getUpperBound());
          partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(
              ((FloatRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).geLowerBound(),
              ((FloatRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).getUpperBound());
          break;
        case DOUBLE:
          matches = rangeIndexReader.getMatchingDocIds(
              ((DoubleRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).geLowerBound(),
              ((DoubleRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).getUpperBound());
          partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(
              ((DoubleRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).geLowerBound(),
              ((DoubleRawValueBasedRangePredicateEvaluator) _rangePredicateEvaluator).getUpperBound());
          break;
        default:
          throw new IllegalStateException("String and Bytes data type not supported for Range Indexing");
      }
    }
    // this branch is likely until RangeIndexReader reimplemented and enabled by default
    if (null != partialMatches) {
      // Need to scan the first and last range as they might be partially matched
      ScanBasedFilterOperator scanBasedFilterOperator =
          new ScanBasedFilterOperator(_rangePredicateEvaluator, _dataSource, _numDocs);
      FilterBlockDocIdSet scanBasedDocIdSet = scanBasedFilterOperator.getNextBlock().getBlockDocIdSet();
      MutableRoaringBitmap docIds = ((ScanBasedDocIdIterator) scanBasedDocIdSet.iterator()).applyAnd(partialMatches);
      if (null != matches) {
        docIds.or(matches);
      }
      return new FilterBlock(new BitmapDocIdSet(docIds, _numDocs) {
        // Override this method to reflect the entries scanned
        @Override
        public long getNumEntriesScannedInFilter() {
          return scanBasedDocIdSet.getNumEntriesScannedInFilter();
        }
      });
    } else {
      return new FilterBlock(new BitmapDocIdSet(matches == null ? new MutableRoaringBitmap() : matches, _numDocs));
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
