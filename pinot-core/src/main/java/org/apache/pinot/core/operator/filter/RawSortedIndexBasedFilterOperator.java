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

import com.google.common.base.CaseFormat;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.SortedDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.Pairs.IntPair;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Filter operator for sorted raw (non-dictionary-encoded) single-value columns. Uses binary search on the forward
 * index to find matching document id ranges, avoiding full column scans. For chunk-compressed forward indexes,
 * implements a two-level binary search: first at chunk boundaries to minimize chunk decompressions, then within the
 * target chunk.
 */
public class RawSortedIndexBasedFilterOperator extends BaseColumnFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_RAW_SORTED_INDEX";

  private final PredicateEvaluator _predicateEvaluator;
  @SuppressWarnings("rawtypes")
  private final ForwardIndexReader _forwardIndexReader;
  private final int _numDocsPerChunk;

  @SuppressWarnings("rawtypes")
  RawSortedIndexBasedFilterOperator(QueryContext queryContext, PredicateEvaluator predicateEvaluator,
      DataSource dataSource, int numDocs) {
    super(queryContext, dataSource, numDocs);
    _predicateEvaluator = predicateEvaluator;
    _forwardIndexReader = dataSource.getForwardIndex();
    _numDocsPerChunk = _forwardIndexReader.getNumDocsPerChunk();
  }

  /**
   * Functional interface for comparing a value at a docId against a target value.
   * Returns negative if value at docId < target, 0 if equal, positive if value > target.
   */
  @FunctionalInterface
  private interface ValueComparator {
    int compare(int docId);
  }

  @Override
  protected BlockDocIdSet getNextBlockWithoutNullHandling() {
    List<IntPair> docIdRanges = computeMatchingRanges();
    if (docIdRanges.isEmpty()) {
      return EmptyDocIdSet.getInstance();
    }
    return new SortedDocIdSet(docIdRanges);
  }

  @SuppressWarnings("unchecked")
  private List<IntPair> computeMatchingRanges() {
    Predicate predicate = _predicateEvaluator.getPredicate();
    Predicate.Type predicateType = predicate.getType();
    DataType dataType = _forwardIndexReader.getStoredType();

    try (ForwardIndexReaderContext context = _forwardIndexReader.createContext()) {
      switch (predicateType) {
        case EQ:
          return computeEqRanges(context, dataType, ((EqPredicate) predicate).getValue());
        case NOT_EQ:
          return invertRanges(computeEqRanges(context, dataType, ((NotEqPredicate) predicate).getValue()));
        case IN:
          return computeInRanges(context, dataType, ((InPredicate) predicate).getValues());
        case NOT_IN:
          return invertRanges(computeInRanges(context, dataType, ((NotInPredicate) predicate).getValues()));
        case RANGE:
          return computeRangeRanges(context, dataType, (RangePredicate) predicate);
        default:
          // For unsupported predicates (REGEXP_LIKE, etc.), fall back to full range and let caller scan
          return Collections.singletonList(new IntPair(0, _numDocs - 1));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to compute matching ranges for raw sorted column", e);
    }
  }

  private List<IntPair> computeEqRanges(ForwardIndexReaderContext context, DataType dataType, String valueStr) {
    ValueComparator cmp = buildComparator(context, dataType, valueStr);
    int firstDocId = lowerBound(cmp, 0, _numDocs - 1);
    if (firstDocId >= _numDocs) {
      return Collections.emptyList();
    }
    // Check if the value at firstDocId actually matches
    if (cmp.compare(firstDocId) != 0) {
      return Collections.emptyList();
    }
    int lastDocId = upperBound(cmp, firstDocId, _numDocs - 1);
    return Collections.singletonList(new IntPair(firstDocId, lastDocId));
  }

  private List<IntPair> computeInRanges(ForwardIndexReaderContext context, DataType dataType, List<String> values) {
    // Sort values to get ranges in order, then merge adjacent ranges
    String[] sortedValues = getSortedValues(dataType, values);
    List<IntPair> allRanges = new ArrayList<>();
    for (String valueStr : sortedValues) {
      List<IntPair> eqRanges = computeEqRanges(context, dataType, valueStr);
      allRanges.addAll(eqRanges);
    }
    return mergeAdjacentRanges(allRanges);
  }

  private List<IntPair> computeRangeRanges(ForwardIndexReaderContext context, DataType dataType,
      RangePredicate rangePredicate) {
    String lowerBound = rangePredicate.getLowerBound();
    String upperBound = rangePredicate.getUpperBound();
    boolean lowerUnbounded = lowerBound.equals(RangePredicate.UNBOUNDED);
    boolean upperUnbounded = upperBound.equals(RangePredicate.UNBOUNDED);
    boolean lowerInclusive = lowerUnbounded || rangePredicate.isLowerInclusive();
    boolean upperInclusive = upperUnbounded || rangePredicate.isUpperInclusive();

    int startDocId;
    if (lowerUnbounded) {
      startDocId = 0;
    } else {
      ValueComparator lowerCmp = buildComparator(context, dataType, lowerBound);
      if (lowerInclusive) {
        // Find first docId where value >= lowerBound
        startDocId = lowerBound(lowerCmp, 0, _numDocs - 1);
      } else {
        // Find first docId where value > lowerBound (i.e., first docId where value >= lowerBound that is NOT equal)
        startDocId = strictLowerBound(lowerCmp, 0, _numDocs - 1);
      }
    }

    if (startDocId >= _numDocs) {
      return Collections.emptyList();
    }

    int endDocId;
    if (upperUnbounded) {
      endDocId = _numDocs - 1;
    } else {
      ValueComparator upperCmp = buildComparator(context, dataType, upperBound);
      if (upperInclusive) {
        // Find last docId where value <= upperBound
        endDocId = upperBound(upperCmp, 0, _numDocs - 1);
      } else {
        // Find last docId where value < upperBound
        endDocId = strictUpperBound(upperCmp, 0, _numDocs - 1);
      }
    }

    if (endDocId < 0 || endDocId < startDocId) {
      return Collections.emptyList();
    }

    return Collections.singletonList(new IntPair(startDocId, endDocId));
  }

  /**
   * Inverts a list of docId ranges within [0, numDocs-1].
   */
  private List<IntPair> invertRanges(List<IntPair> ranges) {
    if (ranges.isEmpty()) {
      return Collections.singletonList(new IntPair(0, _numDocs - 1));
    }
    List<IntPair> inverted = new ArrayList<>(ranges.size() + 1);
    int prevEnd = -1;
    for (IntPair range : ranges) {
      if (range.getLeft() > prevEnd + 1) {
        inverted.add(new IntPair(prevEnd + 1, range.getLeft() - 1));
      }
      prevEnd = range.getRight();
    }
    if (prevEnd < _numDocs - 1) {
      inverted.add(new IntPair(prevEnd + 1, _numDocs - 1));
    }
    return inverted;
  }

  /**
   * Merges adjacent or overlapping ranges into a single range.
   */
  private static List<IntPair> mergeAdjacentRanges(List<IntPair> ranges) {
    if (ranges.size() <= 1) {
      return ranges;
    }
    List<IntPair> merged = new ArrayList<>();
    IntPair current = ranges.get(0);
    for (int i = 1; i < ranges.size(); i++) {
      IntPair next = ranges.get(i);
      if (next.getLeft() <= current.getRight() + 1) {
        current = new IntPair(current.getLeft(), Math.max(current.getRight(), next.getRight()));
      } else {
        merged.add(current);
        current = next;
      }
    }
    merged.add(current);
    return merged;
  }

  // --- Binary search methods ---

  /**
   * Finds the first docId in [low, high] where value >= target (comparator returns >= 0).
   * If no such docId exists, returns high + 1.
   * Uses two-level search when chunk info is available.
   */
  private int lowerBound(ValueComparator cmp, int low, int high) {
    if (_numDocsPerChunk > 0 && high - low > _numDocsPerChunk) {
      return chunkAwareLowerBound(cmp, low, high);
    }
    return simpleLowerBound(cmp, low, high);
  }

  /**
   * Finds the last docId in [low, high] where value <= target (comparator returns <= 0).
   * If no such docId exists, returns low - 1.
   */
  private int upperBound(ValueComparator cmp, int low, int high) {
    if (_numDocsPerChunk > 0 && high - low > _numDocsPerChunk) {
      return chunkAwareUpperBound(cmp, low, high);
    }
    return simpleUpperBound(cmp, low, high);
  }

  /**
   * Finds the first docId where value > target (strictly greater than).
   */
  private int strictLowerBound(ValueComparator cmp, int low, int high) {
    // Find the last docId where value == target, then return the next one
    int ub = upperBound(cmp, low, high);
    return ub + 1;
  }

  /**
   * Finds the last docId where value < target (strictly less than).
   */
  private int strictUpperBound(ValueComparator cmp, int low, int high) {
    // Find the first docId where value >= target, then return the one before
    int lb = lowerBound(cmp, low, high);
    return lb - 1;
  }

  private int simpleLowerBound(ValueComparator cmp, int low, int high) {
    int result = high + 1;
    while (low <= high) {
      int mid = low + (high - low) / 2;
      if (cmp.compare(mid) >= 0) {
        result = mid;
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    return result;
  }

  private int simpleUpperBound(ValueComparator cmp, int low, int high) {
    int result = low - 1;
    while (low <= high) {
      int mid = low + (high - low) / 2;
      if (cmp.compare(mid) <= 0) {
        result = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return result;
  }

  /**
   * Two-level binary search: coarse search at chunk boundaries, then fine search within the chunk.
   * Finds first docId where value >= target.
   */
  private int chunkAwareLowerBound(ValueComparator cmp, int low, int high) {
    // Coarse search: find the chunk containing the boundary
    int lowChunk = low / _numDocsPerChunk;
    int highChunk = Math.min(high / _numDocsPerChunk, (_numDocs - 1) / _numDocsPerChunk);

    int targetChunk = highChunk + 1;
    while (lowChunk <= highChunk) {
      int midChunk = lowChunk + (highChunk - lowChunk) / 2;
      int chunkFirstDocId = midChunk * _numDocsPerChunk;
      if (cmp.compare(chunkFirstDocId) >= 0) {
        targetChunk = midChunk;
        highChunk = midChunk - 1;
      } else {
        lowChunk = midChunk + 1;
      }
    }

    // Fine search within the target chunk and possibly the previous one
    int fineStart;
    if (targetChunk > 0) {
      fineStart = (targetChunk - 1) * _numDocsPerChunk;
    } else {
      fineStart = 0;
    }
    int fineEnd = Math.min((targetChunk + 1) * _numDocsPerChunk - 1, high);
    fineStart = Math.max(fineStart, low);
    fineEnd = Math.min(fineEnd, high);
    return simpleLowerBound(cmp, fineStart, fineEnd);
  }

  /**
   * Two-level binary search: coarse search at chunk boundaries, then fine search within the chunk.
   * Finds last docId where value <= target.
   */
  private int chunkAwareUpperBound(ValueComparator cmp, int low, int high) {
    // Coarse search: find the last chunk where first value <= target
    int lowChunk = low / _numDocsPerChunk;
    int highChunk = Math.min(high / _numDocsPerChunk, (_numDocs - 1) / _numDocsPerChunk);

    int targetChunk = lowChunk - 1;
    while (lowChunk <= highChunk) {
      int midChunk = lowChunk + (highChunk - lowChunk) / 2;
      int chunkFirstDocId = midChunk * _numDocsPerChunk;
      if (cmp.compare(chunkFirstDocId) <= 0) {
        targetChunk = midChunk;
        lowChunk = midChunk + 1;
      } else {
        highChunk = midChunk - 1;
      }
    }

    // Fine search within the target chunk and possibly the next one
    int fineStart = Math.max(targetChunk * _numDocsPerChunk, low);
    int fineEnd;
    if (targetChunk < (_numDocs - 1) / _numDocsPerChunk) {
      fineEnd = (targetChunk + 2) * _numDocsPerChunk - 1;
    } else {
      fineEnd = _numDocs - 1;
    }
    fineEnd = Math.min(fineEnd, high);
    return simpleUpperBound(cmp, fineStart, fineEnd);
  }

  // --- Value comparator builders ---

  @SuppressWarnings("unchecked")
  private ValueComparator buildComparator(ForwardIndexReaderContext context, DataType dataType, String valueStr) {
    switch (dataType) {
      case INT: {
        int target = Integer.parseInt(valueStr);
        return docId -> Integer.compare(_forwardIndexReader.getInt(docId, context), target);
      }
      case LONG: {
        long target = Long.parseLong(valueStr);
        return docId -> Long.compare(_forwardIndexReader.getLong(docId, context), target);
      }
      case FLOAT: {
        float target = Float.parseFloat(valueStr);
        return docId -> Float.compare(_forwardIndexReader.getFloat(docId, context), target);
      }
      case DOUBLE: {
        double target = Double.parseDouble(valueStr);
        return docId -> Double.compare(_forwardIndexReader.getDouble(docId, context), target);
      }
      case BIG_DECIMAL: {
        BigDecimal target = new BigDecimal(valueStr);
        return docId -> _forwardIndexReader.getBigDecimal(docId, context).compareTo(target);
      }
      case STRING: {
        return docId -> _forwardIndexReader.getString(docId, context).compareTo(valueStr);
      }
      case BYTES: {
        byte[] target = BytesUtils.toBytes(valueStr);
        return docId -> ByteArray.compare(_forwardIndexReader.getBytes(docId, context), target);
      }
      default:
        throw new IllegalStateException("Unsupported data type for raw sorted index: " + dataType);
    }
  }

  /**
   * Sorts the IN/NOT_IN values according to the column data type ordering, so that binary search results produce
   * ranges in docId order.
   */
  private String[] getSortedValues(DataType dataType, List<String> values) {
    String[] sorted = values.toArray(new String[0]);
    switch (dataType) {
      case INT:
        int[] intVals = new int[sorted.length];
        for (int i = 0; i < sorted.length; i++) {
          intVals[i] = Integer.parseInt(sorted[i]);
        }
        Arrays.sort(intVals);
        for (int i = 0; i < sorted.length; i++) {
          sorted[i] = Integer.toString(intVals[i]);
        }
        break;
      case LONG:
        long[] longVals = new long[sorted.length];
        for (int i = 0; i < sorted.length; i++) {
          longVals[i] = Long.parseLong(sorted[i]);
        }
        Arrays.sort(longVals);
        for (int i = 0; i < sorted.length; i++) {
          sorted[i] = Long.toString(longVals[i]);
        }
        break;
      case FLOAT:
        float[] floatVals = new float[sorted.length];
        for (int i = 0; i < sorted.length; i++) {
          floatVals[i] = Float.parseFloat(sorted[i]);
        }
        Arrays.sort(floatVals);
        for (int i = 0; i < sorted.length; i++) {
          sorted[i] = Float.toString(floatVals[i]);
        }
        break;
      case DOUBLE:
        double[] doubleVals = new double[sorted.length];
        for (int i = 0; i < sorted.length; i++) {
          doubleVals[i] = Double.parseDouble(sorted[i]);
        }
        Arrays.sort(doubleVals);
        for (int i = 0; i < sorted.length; i++) {
          sorted[i] = Double.toString(doubleVals[i]);
        }
        break;
      default:
        // String and others: natural string sort
        Arrays.sort(sorted);
        break;
    }
    return sorted;
  }

  // --- Count optimization and bitmap support ---

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public int getNumMatchingDocs() {
    List<IntPair> ranges = computeMatchingRanges();
    int count = 0;
    for (IntPair range : ranges) {
      count += range.getRight() - range.getLeft() + 1;
    }
    return count;
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public BitmapCollection getBitmaps() {
    List<IntPair> ranges = computeMatchingRanges();
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (IntPair range : ranges) {
      bitmap.add(range.getLeft(), range.getRight() + 1L);
    }
    return new BitmapCollection(_numDocs, false, bitmap);
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:raw_sorted_index");
    stringBuilder.append(",operator:").append(_predicateEvaluator.getPredicateType());
    stringBuilder.append(",predicate:").append(_predicateEvaluator.getPredicate().toString());
    return stringBuilder.append(')').toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putString("indexLookUp", "raw_sorted_index");
    attributeBuilder.putString("operator", _predicateEvaluator.getPredicateType().name());
    attributeBuilder.putString("predicate", _predicateEvaluator.getPredicate().toString());
  }
}
