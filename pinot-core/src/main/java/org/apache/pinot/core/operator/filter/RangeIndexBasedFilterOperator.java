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
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.traits.DoubleRange;
import org.apache.pinot.core.operator.filter.predicate.traits.DoubleValue;
import org.apache.pinot.core.operator.filter.predicate.traits.FloatRange;
import org.apache.pinot.core.operator.filter.predicate.traits.FloatValue;
import org.apache.pinot.core.operator.filter.predicate.traits.IntRange;
import org.apache.pinot.core.operator.filter.predicate.traits.IntValue;
import org.apache.pinot.core.operator.filter.predicate.traits.LongRange;
import org.apache.pinot.core.operator.filter.predicate.traits.LongValue;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class RangeIndexBasedFilterOperator extends BaseFilterOperator {

  private static final String EXPLAIN_NAME = "FILTER_RANGE_INDEX";

  private final RangeIndexReader<ImmutableRoaringBitmap> _rangeIndexReader;
  private final PredicateEvaluator _predicateEvaluator;
  private final DataSource _dataSource;
  private final FieldSpec.DataType _parameterType;

  static boolean canEvaluate(PredicateEvaluator predicateEvaluator, DataSource dataSource) {
    Predicate.Type type = predicateEvaluator.getPredicateType();
    RangeIndexReader<?> rangeIndex = dataSource.getRangeIndex();
    return rangeIndex != null && (type == Predicate.Type.RANGE || (type == Predicate.Type.EQ
        && dataSource.getRangeIndex().isExact()));
  }

  @SuppressWarnings("unchecked")
  public RangeIndexBasedFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int numDocs,
      boolean nullHandlingEnabled) {
    super(numDocs, nullHandlingEnabled);
    _predicateEvaluator = predicateEvaluator;
    _rangeIndexReader = (RangeIndexReader<ImmutableRoaringBitmap>) dataSource.getRangeIndex();
    _dataSource = dataSource;
    _parameterType = predicateEvaluator.isDictionaryBased() ? FieldSpec.DataType.INT : predicateEvaluator.getDataType();
  }

  @Override
  protected FilterBlock getNextBlock() {
    if (_rangeIndexReader.isExact()) {
      ImmutableRoaringBitmap matches = getMatchingDocIds();
      recordFilter(matches);
      return new FilterBlock(new BitmapDocIdSet(matches, _numDocs));
    }
    return evaluateLegacyRangeFilter();
  }

  private FilterBlock evaluateLegacyRangeFilter() {
    ImmutableRoaringBitmap matches = getMatchingDocIds();
    // if the implementation cannot match the entire query exactly, it will
    // yield partial matches, which need to be verified by scanning. If it
    // can answer the query exactly, this will be null.
    ImmutableRoaringBitmap partialMatches = getPartiallyMatchingDocIds();
    // this branch is likely until RangeIndexReader reimplemented and enabled by default
    if (partialMatches == null) {
      return new FilterBlock(new BitmapDocIdSet(matches == null ? new MutableRoaringBitmap() : matches, _numDocs));
    }
    // TODO: support proper null handling in range index.
    // Need to scan the first and last range as they might be partially matched
    ScanBasedFilterOperator scanBasedFilterOperator =
        new ScanBasedFilterOperator(_predicateEvaluator, _dataSource, _numDocs, false);
    BlockDocIdSet scanBasedDocIdSet = scanBasedFilterOperator.getNextBlock().getBlockDocIdSet();
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

  ImmutableRoaringBitmap getMatchingDocIds() {
    switch (_parameterType) {
      case INT:
        if (_predicateEvaluator instanceof IntValue) {
          return _rangeIndexReader.getMatchingDocIds(((IntValue) _predicateEvaluator).getInt());
        }
        IntRange intRange = (IntRange) _predicateEvaluator;
        return _rangeIndexReader.getMatchingDocIds(intRange.getInclusiveLowerBound(),
            intRange.getInclusiveUpperBound());
      case LONG:
        if (_predicateEvaluator instanceof LongValue) {
          return _rangeIndexReader.getMatchingDocIds(((LongValue) _predicateEvaluator).getLong());
        }
        LongRange longRange = (LongRange) _predicateEvaluator;
        return _rangeIndexReader.getMatchingDocIds(longRange.getInclusiveLowerBound(),
            longRange.getInclusiveUpperBound());
      case FLOAT:
        if (_predicateEvaluator instanceof FloatValue) {
          return _rangeIndexReader.getMatchingDocIds(((FloatValue) _predicateEvaluator).getFloat());
        }
        FloatRange floatRange = (FloatRange) _predicateEvaluator;
        return _rangeIndexReader.getMatchingDocIds(floatRange.getInclusiveLowerBound(),
            floatRange.getInclusiveUpperBound());
      case DOUBLE:
        if (_predicateEvaluator instanceof DoubleValue) {
          return _rangeIndexReader.getMatchingDocIds(((DoubleValue) _predicateEvaluator).getDouble());
        }
        DoubleRange doubleRange = (DoubleRange) _predicateEvaluator;
        return _rangeIndexReader.getMatchingDocIds(doubleRange.getInclusiveLowerBound(),
            doubleRange.getInclusiveUpperBound());
      default:
        throw unsupportedDataType(_parameterType);
    }
  }

  ImmutableRoaringBitmap getPartiallyMatchingDocIds() {
    assert !_rangeIndexReader.isExact();
    switch (_parameterType) {
      case INT:
        IntRange intRange = (IntRange) _predicateEvaluator;
        return _rangeIndexReader.getPartiallyMatchingDocIds(intRange.getInclusiveLowerBound(),
            intRange.getInclusiveUpperBound());
      case LONG:
        LongRange longRange = (LongRange) _predicateEvaluator;
        return _rangeIndexReader.getPartiallyMatchingDocIds(longRange.getInclusiveLowerBound(),
            longRange.getInclusiveUpperBound());
      case FLOAT:
        FloatRange floatRange = (FloatRange) _predicateEvaluator;
        return _rangeIndexReader.getPartiallyMatchingDocIds(floatRange.getInclusiveLowerBound(),
            floatRange.getInclusiveUpperBound());
      case DOUBLE:
        DoubleRange doubleRange = (DoubleRange) _predicateEvaluator;
        return _rangeIndexReader.getPartiallyMatchingDocIds(doubleRange.getInclusiveLowerBound(),
            doubleRange.getInclusiveUpperBound());
      default:
        throw unsupportedDataType(_parameterType);
    }
  }

  @Override
  public boolean canOptimizeCount() {
    return _rangeIndexReader.isExact();
  }

  @Override
  public int getNumMatchingDocs() {
    switch (_parameterType) {
      case INT:
        if (_predicateEvaluator instanceof IntValue) {
          return _rangeIndexReader.getNumMatchingDocs(((IntValue) _predicateEvaluator).getInt());
        }
        IntRange intRange = (IntRange) _predicateEvaluator;
        return _rangeIndexReader.getNumMatchingDocs(intRange.getInclusiveLowerBound(),
            intRange.getInclusiveUpperBound());
      case LONG:
        if (_predicateEvaluator instanceof LongValue) {
          return _rangeIndexReader.getNumMatchingDocs(((LongValue) _predicateEvaluator).getLong());
        }
        LongRange longRange = (LongRange) _predicateEvaluator;
        return _rangeIndexReader.getNumMatchingDocs(longRange.getInclusiveLowerBound(),
            longRange.getInclusiveUpperBound());
      case FLOAT:
        if (_predicateEvaluator instanceof FloatValue) {
          return _rangeIndexReader.getNumMatchingDocs(((FloatValue) _predicateEvaluator).getFloat());
        }
        FloatRange floatRange = (FloatRange) _predicateEvaluator;
        return _rangeIndexReader.getNumMatchingDocs(floatRange.getInclusiveLowerBound(),
            floatRange.getInclusiveUpperBound());
      case DOUBLE:
        if (_predicateEvaluator instanceof DoubleValue) {
          return _rangeIndexReader.getNumMatchingDocs(((DoubleValue) _predicateEvaluator).getDouble());
        }
        DoubleRange doubleRange = (DoubleRange) _predicateEvaluator;
        return _rangeIndexReader.getNumMatchingDocs(doubleRange.getInclusiveLowerBound(),
            doubleRange.getInclusiveUpperBound());
      default:
        throw unsupportedDataType(_parameterType);
    }
  }

  @Override
  public boolean canProduceBitmaps() {
    return _rangeIndexReader.isExact();
  }

  @Override
  public BitmapCollection getBitmaps() {
    return new BitmapCollection(_numDocs, false, getMatchingDocIds());
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME + "(indexLookUp:range_index" + ",operator:" + _predicateEvaluator.getPredicateType()
        + ",predicate:" + _predicateEvaluator.getPredicate().toString() + ')';
  }

  static RuntimeException unsupportedPredicateType(Predicate.Type type) {
    return new IllegalStateException("Range index cannot satisfy " + type);
  }

  static RuntimeException unsupportedDataType(FieldSpec.DataType dataType) {
    return new IllegalStateException("Range index does not support " + dataType);
  }

  private void recordFilter(ImmutableRoaringBitmap bitmap) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
      recording.setNumDocsMatchingAfterFilter(bitmap == null ? 0 : bitmap.getCardinality());
      recording.setColumnName(_dataSource.getDataSourceMetadata().getFieldSpec().getName());
      recording.setFilter(FilterType.INDEX, _predicateEvaluator.getPredicateType().name());
      recording.setNumDocsScanned(_numDocs);
    }
  }
}
