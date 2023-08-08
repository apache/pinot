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
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.RoaringBitmapWriter;
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

  public SVScanDocIdIterator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int numDocs, int batchSize) {
    _batch = new int[batchSize];
    _predicateEvaluator = predicateEvaluator;
    _reader = dataSource.getForwardIndex();
    _readerContext = _reader.createContext();
    _numDocs = numDocs;
    _valueMatcher = getValueMatcher();
    _cardinality = dataSource.getDataSourceMetadata().getCardinality();
  }

  // for testing
  public SVScanDocIdIterator(PredicateEvaluator predicateEvaluator, ForwardIndexReader reader, int numDocs) {
    _batch = new int[BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE];
    _predicateEvaluator = predicateEvaluator;
    _reader = reader;
    _readerContext = reader.createContext();
    _numDocs = numDocs;
    _valueMatcher = getValueMatcher();
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

  private ValueMatcher getValueMatcher() {
    if (_reader.isDictionaryEncoded()) {
      return new DictIdMatcher();
    } else {
      switch (_reader.getStoredType()) {
        case INT:
          return new IntMatcher();
        case LONG:
          return new LongMatcher();
        case FLOAT:
          return new FloatMatcher();
        case DOUBLE:
          return new DoubleMatcher();
        case BIG_DECIMAL:
          return new BigDecimalMatcher();
        case STRING:
          return new StringMatcher();
        case BYTES:
          return new BytesMatcher();
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

  private class BigDecimalMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getBigDecimal(docId, _readerContext));
    }
  }

  private class StringMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getString(docId, _readerContext));
    }
  }

  private class BytesMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_reader.getBytes(docId, _readerContext));
    }
  }
}
