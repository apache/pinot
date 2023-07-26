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
package org.apache.pinot.core.operator.dociditerators;

import java.util.OptionalInt;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * The {@code SVScanDocIdIterator} is the scan-based iterator for SVScanDocIdSet to scan a single-value column for the
 * matching document ids.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class SVScanDocIdIterator implements ScanBasedDocIdIterator {
  private final PredicateEvaluator _predicateEvaluator;
  private final ForwardIndexReader _reader;
  // TODO: Figure out a way to close the reader context
  //       ChunkReaderContext should be closed explicitly to release the off-heap buffer
  private final ForwardIndexReaderContext _readerContext;
  private final int _numDocs;
  private final ValueMatcher _valueMatcher;
  private final int[] _batch;
  private int _firstMismatch;
  private int _cursor;
  private final int _cardinality;

  private int _nextDocId = 0;
  private long _numEntriesScanned = 0L;

  public SVScanDocIdIterator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int numDocs,
      @Nullable NullValueVectorReader nullValueReader, int batchSize) {
    _batch = new int[batchSize];
    _predicateEvaluator = predicateEvaluator;
    _reader = dataSource.getForwardIndex();
    _readerContext = _reader.createContext();
    _numDocs = numDocs;
    ImmutableRoaringBitmap nullBitmap = nullValueReader != null ? nullValueReader.getNullBitmap() : null;
    if (nullBitmap != null && nullBitmap.isEmpty()) {
      nullBitmap = null;
    }
    _valueMatcher = getValueMatcher(nullBitmap);
    _cardinality = dataSource.getDataSourceMetadata().getCardinality();
  }

  // for testing
  public SVScanDocIdIterator(PredicateEvaluator predicateEvaluator, ForwardIndexReader reader, int numDocs,
      @Nullable NullValueVectorReader nullValueReader) {
    _batch = new int[BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE];
    _predicateEvaluator = predicateEvaluator;
    _reader = reader;
    _readerContext = reader.createContext();
    _numDocs = numDocs;
    ImmutableRoaringBitmap nullBitmap = nullValueReader != null ? nullValueReader.getNullBitmap() : null;
    if (nullBitmap != null && nullBitmap.isEmpty()) {
      nullBitmap = null;
    }
    _valueMatcher = getValueMatcher(nullBitmap);
    _cardinality = -1;
  }

  @Override
  public int next() {
    if (_cursor >= _firstMismatch) {
      int limit;
      int batchSize = 0;
      do {
        limit = Math.min(_numDocs - _nextDocId, _batch.length);
        if (limit > 0) {
          for (int i = 0; i < limit; i++) {
            _batch[i] = _nextDocId + i;
          }
          batchSize = _valueMatcher.matchValues(limit, _batch);
          _nextDocId += limit;
          _numEntriesScanned += limit;
        }
      } while (limit > 0 & batchSize == 0);
      _firstMismatch = batchSize;
      _cursor = 0;
      if (_firstMismatch == 0) {
        return Constants.EOF;
      }
    }
    return _batch[_cursor++];
  }

  @Override
  public int advance(int targetDocId) {
    _nextDocId = targetDocId;
    _firstMismatch = 0;
    while (_nextDocId < _numDocs) {
      int nextDocId = _nextDocId++;
      _numEntriesScanned++;
      if (_valueMatcher.doesValueMatch(nextDocId)) {
        return nextDocId;
      }
    }
    return Constants.EOF;
  }

  @Override
  public MutableRoaringBitmap applyAnd(BatchIterator docIdIterator, OptionalInt firstDoc, OptionalInt lastDoc) {
    if (!docIdIterator.hasNext()) {
      return new MutableRoaringBitmap();
    }
    RoaringBitmapWriter<MutableRoaringBitmap> result;
    if (firstDoc.isPresent() && lastDoc.isPresent()) {
      result = RoaringBitmapWriter.bufferWriter()
          .expectedRange(firstDoc.getAsInt(), lastDoc.getAsInt())
          .runCompress(false)
          .get();
    } else {
      result = RoaringBitmapWriter.bufferWriter()
          .runCompress(false)
          .get();
    }
    int[] buffer = new int[_batch.length];
    while (docIdIterator.hasNext()) {
      int limit = docIdIterator.nextBatch(buffer);
      if (limit > 0) {
        int firstMismatch = _valueMatcher.matchValues(limit, buffer);
        for (int i = 0; i < firstMismatch; i++) {
          result.add(buffer[i]);
        }
      }
      _numEntriesScanned += limit;
    }
    return result.get();
  }

  @Override
  public long getNumEntriesScanned() {
    return _numEntriesScanned;
  }

  /**
   * This is an approximation of probability calculation in
   * org.apache.pinot.controller.recommender.rules.utils.QueryInvertedSortedIndexRecommender#percentSelected
   */
  @Override
  public float getEstimatedCardinality(boolean isAndDocIdSet) {
    int numMatchingItems = _predicateEvaluator.getNumMatchingItems();
    if (numMatchingItems == Integer.MIN_VALUE || _cardinality < 0) {
      return ScanBasedDocIdIterator.super.getEstimatedCardinality(isAndDocIdSet);
    }
    numMatchingItems = numMatchingItems > 0 ? numMatchingItems : (numMatchingItems + _cardinality);
    return ((float) _cardinality) / numMatchingItems;
  }

  private ValueMatcher getValueMatcher(@Nullable ImmutableRoaringBitmap nullBitmap) {
    if (_reader.isDictionaryEncoded()) {
      return nullBitmap == null ? new DictIdMatcher() : new DictIdMatcherAndNullHandler(nullBitmap);
    } else {
      switch (_reader.getStoredType()) {
        case INT:
          return nullBitmap == null ? new IntMatcher() : new IntMatcherAndNullHandler(nullBitmap);
        case LONG:
          return nullBitmap == null ? new LongMatcher() : new LongMatcherAndNullHandler(nullBitmap);
        case FLOAT:
          return nullBitmap == null ? new FloatMatcher() : new FloatMatcherAndNullHandler(nullBitmap);
        case DOUBLE:
          return nullBitmap == null ? new DoubleMatcher() : new DoubleMatcherAndNullHandler(nullBitmap);
        case BIG_DECIMAL:
          return nullBitmap == null ? new BigDecimalMatcher() : new BigDecimalMatcherAndNullHandler(nullBitmap);
        case STRING:
          return nullBitmap == null ? new StringMatcher() : new StringMatcherAndNullHandler(nullBitmap);
        case BYTES:
          return nullBitmap == null ? new BytesMatcher() : new BytesMatcherAndNullHandler(nullBitmap);
        default:
          throw new UnsupportedOperationException();
      }
    }
  }

  private interface ValueMatcher {

    /**
     * Returns {@code true} if the value for the given document id matches the predicate, {@code false} Otherwise.
     */
    boolean doesValueMatch(int docId);

    /**
     * Filters out non matching values and compacts matching docIds in the start of the array.
     * @param limit how much of the input to read
     * @param docIds the docIds to match - may be modified by this method so take a copy if necessary.
     * @return the index in the array of the first non-matching element - all elements before this index match.
     */
    default int matchValues(int limit, int[] docIds) {
      int matchCount = 0;
      for (int i = 0; i < limit; i++) {
        int docId = docIds[i];
        if (doesValueMatch(docId)) {
          docIds[matchCount++] = docId;
        }
      }
      return matchCount;
    }
  }

  private static class MatcherUtils {
    public static int removeNullDocs(int[] docIds, int[] values, int limit, ImmutableRoaringBitmap nullBitmap) {
      assert !nullBitmap.isEmpty();
      int copyToIdx = 0;
      for (int i = 0; i < limit; i++) {
        if (!nullBitmap.contains(docIds[i])) {
          // Compact non-null entries into the prefix of the docIds and values arrays.
          docIds[copyToIdx] = docIds[i];
          values[copyToIdx++] = values[i];
        }
      }
      return copyToIdx;
    }

    public static int removeNullDocs(int[] docIds, long[] values, int limit, ImmutableRoaringBitmap nullBitmap) {
      assert !nullBitmap.isEmpty();
      int copyToIdx = 0;
      for (int i = 0; i < limit; i++) {
        if (!nullBitmap.contains(docIds[i])) {
          docIds[copyToIdx] = docIds[i];
          values[copyToIdx++] = values[i];
        }
      }
      return copyToIdx;
    }

    public static int removeNullDocs(int[] docIds, float[] values, int limit, ImmutableRoaringBitmap nullBitmap) {
      assert !nullBitmap.isEmpty();
      int copyToIdx = 0;
      for (int i = 0; i < limit; i++) {
        if (!nullBitmap.contains(docIds[i])) {
          docIds[copyToIdx] = docIds[i];
          values[copyToIdx++] = values[i];
        }
      }
      return copyToIdx;
    }

    public static int removeNullDocs(int[] docIds, double[] values, int limit, ImmutableRoaringBitmap nullBitmap) {
      assert !nullBitmap.isEmpty();
      int copyToIdx = 0;
      for (int i = 0; i < limit; i++) {
        if (!nullBitmap.contains(docIds[i])) {
          docIds[copyToIdx] = docIds[i];
          values[copyToIdx++] = values[i];
        }
      }
      return copyToIdx;
    }
  }

  private class DictIdMatcher implements ValueMatcher {

    private final int[] _buffer = new int[_batch.length];

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getDictId(docId, _readerContext));
    }

    @Override
    public int matchValues(int limit, int[] docIds) {
      _reader.readDictIds(docIds, limit, _buffer, _readerContext);
      return _predicateEvaluator.applySV(limit, docIds, _buffer);
    }
  }

  private class DictIdMatcherAndNullHandler implements ValueMatcher {

    private final int[] _buffer = new int[_batch.length];
    private final ImmutableRoaringBitmap _nullBitmap;

    public DictIdMatcherAndNullHandler(ImmutableRoaringBitmap nullBitmap) {
      _nullBitmap = nullBitmap;
    }

    @Override
    public boolean doesValueMatch(int docId) {
      // Any comparison (equality, inequality, or membership) with null results in false (similar to Presto) even if
      // the compared with value is null, and comparison is equality.
      // To consider nulls, use: IS NULL, or IS NOT NULL operators.
      if (_nullBitmap.contains(docId)) {
        return false;
      }
      return _predicateEvaluator.applySV(_reader.getDictId(docId, _readerContext));
    }

    @Override
    public int matchValues(int limit, int[] docIds) {
      _reader.readDictIds(docIds, limit, _buffer, _readerContext);
      int newLimit = MatcherUtils.removeNullDocs(docIds, _buffer, limit, _nullBitmap);
      return _predicateEvaluator.applySV(newLimit, docIds, _buffer);
    }
  }

  private class IntMatcher implements ValueMatcher {

    private final int[] _buffer = new int[_batch.length];

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getInt(docId, _readerContext));
    }

    @Override
    public int matchValues(int limit, int[] docIds) {
      _reader.readValuesSV(docIds, limit, _buffer, _readerContext);
      return _predicateEvaluator.applySV(limit, docIds, _buffer);
    }
  }

  private class IntMatcherAndNullHandler implements ValueMatcher {

    private final ImmutableRoaringBitmap _nullBitmap;
    private final int[] _buffer = new int[_batch.length];

    public IntMatcherAndNullHandler(ImmutableRoaringBitmap nullBitmap) {
      _nullBitmap = nullBitmap;
    }

    @Override
    public boolean doesValueMatch(int docId) {
      if (_nullBitmap.contains(docId)) {
        return false;
      }
      return _predicateEvaluator.applySV(_reader.getInt(docId, _readerContext));
    }

    @Override
    public int matchValues(int limit, int[] docIds) {
      _reader.readValuesSV(docIds, limit, _buffer, _readerContext);
      int newLimit = MatcherUtils.removeNullDocs(docIds, _buffer, limit, _nullBitmap);
      return _predicateEvaluator.applySV(newLimit, docIds, _buffer);
    }
  }

  private class LongMatcher implements ValueMatcher {

    private final long[] _buffer = new long[_batch.length];

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getLong(docId, _readerContext));
    }

    @Override
    public int matchValues(int limit, int[] docIds) {
      _reader.readValuesSV(docIds, limit, _buffer, _readerContext);
      return _predicateEvaluator.applySV(limit, docIds, _buffer);
    }
  }

  private class LongMatcherAndNullHandler implements ValueMatcher {

    private final ImmutableRoaringBitmap _nullBitmap;
    private final long[] _buffer = new long[_batch.length];

    public LongMatcherAndNullHandler(ImmutableRoaringBitmap nullBitmap) {
      _nullBitmap = nullBitmap;
    }

    @Override
    public boolean doesValueMatch(int docId) {
      if (_nullBitmap.contains(docId)) {
        return false;
      }
      return _predicateEvaluator.applySV(_reader.getLong(docId, _readerContext));
    }

    @Override
    public int matchValues(int limit, int[] docIds) {
      _reader.readValuesSV(docIds, limit, _buffer, _readerContext);
      int newLimit = MatcherUtils.removeNullDocs(docIds, _buffer, limit, _nullBitmap);
      return _predicateEvaluator.applySV(newLimit, docIds, _buffer);
    }
  }

  private class FloatMatcher implements ValueMatcher {

    private final float[] _buffer = new float[_batch.length];

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getFloat(docId, _readerContext));
    }

    @Override
    public int matchValues(int limit, int[] docIds) {
      _reader.readValuesSV(docIds, limit, _buffer, _readerContext);
      return _predicateEvaluator.applySV(limit, docIds, _buffer);
    }
  }

  private class FloatMatcherAndNullHandler implements ValueMatcher {

    private final ImmutableRoaringBitmap _nullBitmap;
    private final float[] _buffer = new float[_batch.length];

    public FloatMatcherAndNullHandler(ImmutableRoaringBitmap nullBitmap) {
      _nullBitmap = nullBitmap;
    }

    @Override
    public boolean doesValueMatch(int docId) {
      if (_nullBitmap.contains(docId)) {
        return false;
      }
      return _predicateEvaluator.applySV(_reader.getFloat(docId, _readerContext));
    }

    @Override
    public int matchValues(int limit, int[] docIds) {
      _reader.readValuesSV(docIds, limit, _buffer, _readerContext);
      int newLimit = MatcherUtils.removeNullDocs(docIds, _buffer, limit, _nullBitmap);
      return _predicateEvaluator.applySV(newLimit, docIds, _buffer);
    }
  }

  private class DoubleMatcher implements ValueMatcher {

    private final double[] _buffer = new double[_batch.length];

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getDouble(docId, _readerContext));
    }

    @Override
    public int matchValues(int limit, int[] docIds) {
      _reader.readValuesSV(docIds, limit, _buffer, _readerContext);
      return _predicateEvaluator.applySV(limit, docIds, _buffer);
    }
  }

  private class DoubleMatcherAndNullHandler implements ValueMatcher {

    private final ImmutableRoaringBitmap _nullBitmap;
    private final double[] _buffer = new double[_batch.length];

    public DoubleMatcherAndNullHandler(ImmutableRoaringBitmap nullBitmap) {
      _nullBitmap = nullBitmap;
    }

    @Override
    public boolean doesValueMatch(int docId) {
      if (_nullBitmap.contains(docId)) {
        return false;
      }
      return _predicateEvaluator.applySV(_reader.getDouble(docId, _readerContext));
    }

    @Override
    public int matchValues(int limit, int[] docIds) {
      _reader.readValuesSV(docIds, limit, _buffer, _readerContext);
      int newLimit = MatcherUtils.removeNullDocs(docIds, _buffer, limit, _nullBitmap);
      return _predicateEvaluator.applySV(newLimit, docIds, _buffer);
    }
  }

  private class BigDecimalMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getBigDecimal(docId, _readerContext));
    }
  }

  private class BigDecimalMatcherAndNullHandler implements ValueMatcher {

    private final ImmutableRoaringBitmap _nullBitmap;

    public BigDecimalMatcherAndNullHandler(ImmutableRoaringBitmap nullBitmap) {
      _nullBitmap = nullBitmap;
    }

    @Override
    public boolean doesValueMatch(int docId) {
      if (_nullBitmap.contains(docId)) {
        return false;
      }
      return _predicateEvaluator.applySV(_reader.getBigDecimal(docId, _readerContext));
    }
  }

  private class StringMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getString(docId, _readerContext));
    }
  }

  private class StringMatcherAndNullHandler implements ValueMatcher {

    private final ImmutableRoaringBitmap _nullBitmap;

    public StringMatcherAndNullHandler(ImmutableRoaringBitmap nullBitmap) {
      _nullBitmap = nullBitmap;
    }

    @Override
    public boolean doesValueMatch(int docId) {
      if (_nullBitmap.contains(docId)) {
        return false;
      }
      return _predicateEvaluator.applySV(_reader.getString(docId, _readerContext));
    }
  }

  private class BytesMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getBytes(docId, _readerContext));
    }
  }

  private class BytesMatcherAndNullHandler implements ValueMatcher {

    private final ImmutableRoaringBitmap _nullBitmap;

    public BytesMatcherAndNullHandler(ImmutableRoaringBitmap nullBitmap) {
      _nullBitmap = nullBitmap;
    }

    @Override
    public boolean doesValueMatch(int docId) {
      if (_nullBitmap.contains(docId)) {
        return false;
      }
      return _predicateEvaluator.applySV(_reader.getBytes(docId, _readerContext));
    }
  }
}
