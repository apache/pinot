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

import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.Operator;
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
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class RangeIndexBasedFilterOperator extends BaseFilterOperator {

  private static final String EXPLAIN_NAME = "FILTER_RANGE_INDEX";

  private final RangeEvaluator _rangeEvaluator;
  private final PredicateEvaluator _rangePredicateEvaluator;
  private final DataSource _dataSource;
  private final int _numDocs;

  @SuppressWarnings("unchecked")
  public RangeIndexBasedFilterOperator(PredicateEvaluator rangePredicateEvaluator, DataSource dataSource, int numDocs) {
    _rangePredicateEvaluator = rangePredicateEvaluator;
    _rangeEvaluator = RangeEvaluator.of((RangeIndexReader<ImmutableRoaringBitmap>) dataSource.getRangeIndex(),
        rangePredicateEvaluator);
    _dataSource = dataSource;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    if (_rangeEvaluator.isExact()) {
      ImmutableRoaringBitmap matches = _rangeEvaluator.getMatchingDocIds();
      recordFilter(matches);
      return new FilterBlock(new BitmapDocIdSet(matches, _numDocs));
    }
    return evaluateLegacyRangeFilter();
  }

  private FilterBlock evaluateLegacyRangeFilter() {
    ImmutableRoaringBitmap matches = _rangeEvaluator.getMatchingDocIds();
    // if the implementation cannot match the entire query exactly, it will
    // yield partial matches, which need to be verified by scanning. If it
    // can answer the query exactly, this will be null.
    ImmutableRoaringBitmap partialMatches = _rangeEvaluator.getPartiallyMatchingDocIds();
    // this branch is likely until RangeIndexReader reimplemented and enabled by default
    if (partialMatches == null) {
      return new FilterBlock(new BitmapDocIdSet(matches == null ? new MutableRoaringBitmap() : matches, _numDocs));
    }
    // TODO: support proper null handling in range index.
    // Need to scan the first and last range as they might be partially matched
    ScanBasedFilterOperator scanBasedFilterOperator =
        new ScanBasedFilterOperator(_rangePredicateEvaluator, _dataSource, _numDocs, false);
    FilterBlockDocIdSet scanBasedDocIdSet = scanBasedFilterOperator.getNextBlock().getBlockDocIdSet();
    MutableRoaringBitmap docIds = ((ScanBasedDocIdIterator) scanBasedDocIdSet.iterator()).applyAnd(partialMatches);
    if (matches != null) {
      docIds.or(matches);
    }
    recordFilter(matches);
    return new FilterBlock(new BitmapDocIdSet(docIds, _numDocs) {
      // Override this method to reflect the entries scanned
      @Override
      public long getNumEntriesScannedInFilter() {
        return scanBasedDocIdSet.getNumEntriesScannedInFilter();
      }
    });
  }

  @Override
  public boolean canOptimizeCount() {
    return _rangeEvaluator.isExact();
  }

  @Override
  public int getNumMatchingDocs() {
    return _rangeEvaluator.getNumMatchingDocs();
  }

  @Override
  public boolean canProduceBitmaps() {
    return _rangeEvaluator.isExact();
  }

  @Override
  public BitmapCollection getBitmaps() {
    return new BitmapCollection(_numDocs, false, _rangeEvaluator.getMatchingDocIds());
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME + "(indexLookUp:range_index"
        + ",operator:" + _rangePredicateEvaluator.getPredicateType()
        + ",predicate:" + _rangePredicateEvaluator.getPredicate().toString()
        + ')';
  }

  interface RangeEvaluator {
    static RangeEvaluator of(RangeIndexReader<ImmutableRoaringBitmap> rangeIndexReader,
        PredicateEvaluator predicateEvaluator) {
      if (predicateEvaluator instanceof SortedDictionaryBasedRangePredicateEvaluator) {
        return new IntRangeEvaluator(rangeIndexReader,
            (SortedDictionaryBasedRangePredicateEvaluator) predicateEvaluator);
      } else {
        switch (predicateEvaluator.getDataType()) {
          case INT:
            return new IntRangeEvaluator(rangeIndexReader,
                (IntRawValueBasedRangePredicateEvaluator) predicateEvaluator);
          case LONG:
            return new LongRangeEvaluator(rangeIndexReader,
                (LongRawValueBasedRangePredicateEvaluator) predicateEvaluator);
          case FLOAT:
            return new FloatRangeEvaluator(rangeIndexReader,
                (FloatRawValueBasedRangePredicateEvaluator) predicateEvaluator);
          case DOUBLE:
            return new DoubleRangeEvaluator(rangeIndexReader,
                (DoubleRawValueBasedRangePredicateEvaluator) predicateEvaluator);
          default:
            throw new IllegalStateException("String and Bytes data type not supported for Range Indexing");
        }
      }
    }

    ImmutableRoaringBitmap getMatchingDocIds();

    ImmutableRoaringBitmap getPartiallyMatchingDocIds();

    int getNumMatchingDocs();

    boolean isExact();
  }

  private static final class IntRangeEvaluator implements RangeEvaluator {
    final RangeIndexReader<ImmutableRoaringBitmap> _rangeIndexReader;
    final int _min;
    final int _max;

    private IntRangeEvaluator(RangeIndexReader<ImmutableRoaringBitmap> rangeIndexReader, int min, int max) {
      _rangeIndexReader = rangeIndexReader;
      _min = min;
      _max = max;
    }

    IntRangeEvaluator(RangeIndexReader<ImmutableRoaringBitmap> rangeIndexReader,
        IntRawValueBasedRangePredicateEvaluator predicateEvaluator) {
      this(rangeIndexReader, predicateEvaluator.getInclusiveLowerBound(), predicateEvaluator.getInclusiveUpperBound());
    }

    IntRangeEvaluator(RangeIndexReader<ImmutableRoaringBitmap> rangeIndexReader,
        SortedDictionaryBasedRangePredicateEvaluator predicateEvaluator) {
      // NOTE: End dictionary id is exclusive in OfflineDictionaryBasedRangePredicateEvaluator.
      this(rangeIndexReader, predicateEvaluator.getStartDictId(), predicateEvaluator.getEndDictId() - 1);
    }

    @Override
    public ImmutableRoaringBitmap getMatchingDocIds() {
      return _rangeIndexReader.getMatchingDocIds(_min, _max);
    }

    @Override
    public ImmutableRoaringBitmap getPartiallyMatchingDocIds() {
      return _rangeIndexReader.getPartiallyMatchingDocIds(_min, _max);
    }

    @Override
    public int getNumMatchingDocs() {
      return _rangeIndexReader.getNumMatchingDocs(_min, _max);
    }

    @Override
    public boolean isExact() {
      return _rangeIndexReader.isExact();
    }
  }

  private static final class LongRangeEvaluator implements RangeEvaluator {
    final RangeIndexReader<ImmutableRoaringBitmap> _rangeIndexReader;
    final long _min;
    final long _max;

    LongRangeEvaluator(RangeIndexReader<ImmutableRoaringBitmap> rangeIndexReader,
        LongRawValueBasedRangePredicateEvaluator predicateEvaluator) {
      _rangeIndexReader = rangeIndexReader;
      _min = predicateEvaluator.getInclusiveLowerBound();
      _max = predicateEvaluator.getInclusiveUpperBound();
    }

    @Override
    public ImmutableRoaringBitmap getMatchingDocIds() {
      return _rangeIndexReader.getMatchingDocIds(_min, _max);
    }

    @Override
    public ImmutableRoaringBitmap getPartiallyMatchingDocIds() {
      return _rangeIndexReader.getPartiallyMatchingDocIds(_min, _max);
    }

    @Override
    public int getNumMatchingDocs() {
      return _rangeIndexReader.getNumMatchingDocs(_min, _max);
    }

    @Override
    public boolean isExact() {
      return _rangeIndexReader.isExact();
    }
  }

  private static final class FloatRangeEvaluator implements RangeEvaluator {
    final RangeIndexReader<ImmutableRoaringBitmap> _rangeIndexReader;
    final float _min;
    final float _max;

    FloatRangeEvaluator(RangeIndexReader<ImmutableRoaringBitmap> rangeIndexReader,
        FloatRawValueBasedRangePredicateEvaluator predicateEvaluator) {
      _rangeIndexReader = rangeIndexReader;
      _min = predicateEvaluator.getInclusiveLowerBound();
      _max = predicateEvaluator.getInclusiveUpperBound();
    }

    @Override
    public ImmutableRoaringBitmap getMatchingDocIds() {
      return _rangeIndexReader.getMatchingDocIds(_min, _max);
    }

    @Override
    public ImmutableRoaringBitmap getPartiallyMatchingDocIds() {
      return _rangeIndexReader.getPartiallyMatchingDocIds(_min, _max);
    }

    @Override
    public int getNumMatchingDocs() {
      return _rangeIndexReader.getNumMatchingDocs(_min, _max);
    }

    @Override
    public boolean isExact() {
      return _rangeIndexReader.isExact();
    }
  }

  private static final class DoubleRangeEvaluator implements RangeEvaluator {
    final RangeIndexReader<ImmutableRoaringBitmap> _rangeIndexReader;
    final double _min;
    final double _max;

    DoubleRangeEvaluator(RangeIndexReader<ImmutableRoaringBitmap> rangeIndexReader,
        DoubleRawValueBasedRangePredicateEvaluator predicateEvaluator) {
      _rangeIndexReader = rangeIndexReader;
      _min = predicateEvaluator.getInclusiveLowerBound();
      _max = predicateEvaluator.getInclusiveUpperBound();
    }

    @Override
    public ImmutableRoaringBitmap getMatchingDocIds() {
      return _rangeIndexReader.getMatchingDocIds(_min, _max);
    }

    @Override
    public ImmutableRoaringBitmap getPartiallyMatchingDocIds() {
      return _rangeIndexReader.getPartiallyMatchingDocIds(_min, _max);
    }

    @Override
    public int getNumMatchingDocs() {
      return _rangeIndexReader.getNumMatchingDocs(_min, _max);
    }

    @Override
    public boolean isExact() {
      return _rangeIndexReader.isExact();
    }
  }

  private void recordFilter(ImmutableRoaringBitmap bitmap) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
      recording.setNumDocsMatchingAfterFilter(bitmap == null ? 0 : bitmap.getCardinality());
      recording.setColumnName(_dataSource.getDataSourceMetadata().getFieldSpec().getName());
      recording.setFilter(FilterType.INDEX, _rangePredicateEvaluator.getPredicateType().name());
      recording.setNumDocsScanned(_numDocs);
    }
  }
}
