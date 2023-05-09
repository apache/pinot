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
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.utils.CommonConstants.Query.OptimizationConstants;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * The {@code MVScanDocIdIterator} is the scan-based iterator for MVScanDocIdSet to scan a multi-value column for the
 * matching document ids.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class MVScanDocIdIterator implements ScanBasedDocIdIterator {
  private final PredicateEvaluator _predicateEvaluator;
  private final ForwardIndexReader _reader;
  // TODO: Figure out a way to close the reader context
  private final ForwardIndexReaderContext _readerContext;
  private final int _numDocs;
  private final int _maxNumValuesPerMVEntry;
  private final ValueMatcher _valueMatcher;
  private final int _cardinality;

  private int _nextDocId = 0;
  private long _numEntriesScanned = 0L;

  public MVScanDocIdIterator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int numDocs) {
    _predicateEvaluator = predicateEvaluator;
    _reader = dataSource.getForwardIndex();
    _readerContext = _reader.createContext();
    _numDocs = numDocs;
    _maxNumValuesPerMVEntry = dataSource.getDataSourceMetadata().getMaxNumValuesPerMVEntry();
    _valueMatcher = getValueMatcher();
    _cardinality = dataSource.getDataSourceMetadata().getCardinality();
  }

  @Override
  public int next() {
    while (_nextDocId < _numDocs) {
      int nextDocId = _nextDocId++;
      // TODO: The performance can be improved by batching the docID lookups similar to how it's done in
      //       SVScanDocIdIterator
      boolean doesValueMatch = _valueMatcher.doesValueMatch(nextDocId);
      if (doesValueMatch) {
        return nextDocId;
      }
    }
    return Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    _nextDocId = targetDocId;
    return next();
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
    int[] buffer = new int[OPTIMAL_ITERATOR_BATCH_SIZE];
    while (docIdIterator.hasNext()) {
      int limit = docIdIterator.nextBatch(buffer);
      for (int i = 0; i < limit; i++) {
        int nextDocId = buffer[i];
        // TODO: The performance can be improved by batching the docID lookups similar to how it's done in
        //       SVScanDocIdIterator
        boolean doesValueMatch = _valueMatcher.doesValueMatch(nextDocId);
        if (doesValueMatch) {
          result.add(nextDocId);
        }
      }
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
    int avgNumValuesPerMVEntry =
        Math.max(_maxNumValuesPerMVEntry / OptimizationConstants.DEFAULT_AVG_MV_ENTRIES_DENOMINATOR, 1);

    numMatchingItems = numMatchingItems > 0 ? numMatchingItems * avgNumValuesPerMVEntry
        : (numMatchingItems * avgNumValuesPerMVEntry + _cardinality);

    numMatchingItems = Math.max(Math.min(_cardinality, numMatchingItems), 0);

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
        case STRING:
          return new StringMatcher();
        case BYTES:
          return new BytesMatcher();
        default:
          throw new UnsupportedOperationException("MV Scan not supported for raw MV columns of type "
              + _reader.getStoredType());
      }
    }
  }

  private interface ValueMatcher {

    /**
     * Returns {@code true} if the value for the given document id matches the predicate, {@code false} Otherwise.
     */
    boolean doesValueMatch(int docId);
  }

  private class DictIdMatcher implements ValueMatcher {

    private final int[] _buffer = new int[_maxNumValuesPerMVEntry];

    @Override
    public boolean doesValueMatch(int docId) {
      int length = _reader.getDictIdMV(docId, _buffer, _readerContext);
      _numEntriesScanned += length;
      return _predicateEvaluator.applyMV(_buffer, length);
    }
  }

  private class IntMatcher implements ValueMatcher {

    private final int[] _buffer = new int[_maxNumValuesPerMVEntry];

    @Override
    public boolean doesValueMatch(int docId) {
      int length = _reader.getIntMV(docId, _buffer, _readerContext);
      _numEntriesScanned += length;
      return _predicateEvaluator.applyMV(_buffer, length);
    }
  }

  private class LongMatcher implements ValueMatcher {

    private final long[] _buffer = new long[_maxNumValuesPerMVEntry];

    @Override
    public boolean doesValueMatch(int docId) {
      int length = _reader.getLongMV(docId, _buffer, _readerContext);
      _numEntriesScanned += length;
      return _predicateEvaluator.applyMV(_buffer, length);
    }
  }

  private class FloatMatcher implements ValueMatcher {

    private final float[] _buffer = new float[_maxNumValuesPerMVEntry];

    @Override
    public boolean doesValueMatch(int docId) {
      int length = _reader.getFloatMV(docId, _buffer, _readerContext);
      _numEntriesScanned += length;
      return _predicateEvaluator.applyMV(_buffer, length);
    }
  }

  private class DoubleMatcher implements ValueMatcher {

    private final double[] _buffer = new double[_maxNumValuesPerMVEntry];

    @Override
    public boolean doesValueMatch(int docId) {
      int length = _reader.getDoubleMV(docId, _buffer, _readerContext);
      _numEntriesScanned += length;
      return _predicateEvaluator.applyMV(_buffer, length);
    }
  }

  private class StringMatcher implements ValueMatcher {

    private final String[] _buffer = new String[_maxNumValuesPerMVEntry];

    @Override
    public boolean doesValueMatch(int docId) {
      int length = _reader.getStringMV(docId, _buffer, _readerContext);
      _numEntriesScanned += length;
      return _predicateEvaluator.applyMV(_buffer, length);
    }
  }

  private class BytesMatcher implements ValueMatcher {

    private final byte[][] _buffer = new byte[_maxNumValuesPerMVEntry][];

    @Override
    public boolean doesValueMatch(int docId) {
      int length = _reader.getBytesMV(docId, _buffer, _readerContext);
      _numEntriesScanned += length;
      return _predicateEvaluator.applyMV(_buffer, length);
    }
  }
}
