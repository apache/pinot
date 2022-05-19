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

import java.math.BigDecimal;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.Constants;
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
  private final ForwardIndexReader _forwardIndexReader;
  // TODO: Figure out a way to close the reader context
  //       ChunkReaderContext should be closed explicitly to release the off-heap buffer
  private final ForwardIndexReaderContext _readerContext;
  private final int _numDocs;
  private final ValueMatcher _valueMatcher;

  private int _nextDocId = 0;
  private long _numEntriesScanned = 0L;

  public SVScanDocIdIterator(PredicateEvaluator predicateEvaluator, ForwardIndexReader forwardIndexReader,
      NullValueVectorReader nullValueReader, int numDocs) {
    _predicateEvaluator = predicateEvaluator;
    _forwardIndexReader = forwardIndexReader;
    _readerContext = forwardIndexReader.createContext();
    _numDocs = numDocs;
    _valueMatcher = getValueMatcher(nullValueReader);
  }

  public SVScanDocIdIterator(PredicateEvaluator predicateEvaluator, ForwardIndexReader forwardIndexReader,
      int numDocs) {
    _predicateEvaluator = predicateEvaluator;
    _forwardIndexReader = forwardIndexReader;
    _readerContext = forwardIndexReader.createContext();
    _numDocs = numDocs;
    _valueMatcher = getValueMatcher(null);
  }

  @Override
  public int next() {
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
  public int advance(int targetDocId) {
    _nextDocId = targetDocId;
    return next();
  }

  @Override
  public MutableRoaringBitmap applyAnd(ImmutableRoaringBitmap docIds) {
    if (docIds.isEmpty()) {
      return new MutableRoaringBitmap();
    }
    RoaringBitmapWriter<MutableRoaringBitmap> result = RoaringBitmapWriter.bufferWriter()
        .expectedRange(docIds.first(), docIds.last()).runCompress(false).get();
    BatchIterator docIdIterator = docIds.getBatchIterator();
    int[] buffer = new int[OPTIMAL_ITERATOR_BATCH_SIZE];
    while (docIdIterator.hasNext()) {
      int limit = docIdIterator.nextBatch(buffer);
      for (int i = 0; i < limit; i++) {
        int nextDocId = buffer[i];
        _numEntriesScanned++;
        if (_valueMatcher.doesValueMatch(nextDocId)) {
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

  private ValueMatcher getValueMatcher(NullValueVectorReader nullValueReader) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      return new DictIdMatcher();
    } else {
      switch (_forwardIndexReader.getValueType()) {
        case INT:
          return new IntMatcher(nullValueReader);
        case LONG:
          return new LongMatcher();
        case FLOAT:
          return new FloatMatcher();
        case DOUBLE:
          return new DoubleMatcher();
        case BIG_DECIMAL:
          return new BigDecimalMatcher(nullValueReader);
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
  }

  private class DictIdMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_forwardIndexReader.getDictId(docId, _readerContext));
    }
  }

  // todo(nhejazi): null handling has been implemented for int/BigDecimal data types. Implement for other data types.
  private class IntMatcher implements ValueMatcher {
    private final NullValueVectorReader _nullValueReader;

    public IntMatcher(NullValueVectorReader nullValueReader) {
      _nullValueReader = nullValueReader;
    }

    @Override
    public boolean doesValueMatch(int docId) {
      if (_nullValueReader != null && _nullValueReader.isNull(docId)) {
        return _predicateEvaluator.applySV((Integer) null);
      }
      return _predicateEvaluator.applySV(_forwardIndexReader.getInt(docId, _readerContext));
    }
  }

  private class LongMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_forwardIndexReader.getLong(docId, _readerContext));
    }
  }

  private class FloatMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_forwardIndexReader.getFloat(docId, _readerContext));
    }
  }

  private class DoubleMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_forwardIndexReader.getDouble(docId, _readerContext));
    }
  }

  private class BigDecimalMatcher implements ValueMatcher {
    private final NullValueVectorReader _nullValueReader;

    public BigDecimalMatcher(NullValueVectorReader nullValueReader) {
      _nullValueReader = nullValueReader;
    }

    @Override
    public boolean doesValueMatch(int docId) {
      if (_nullValueReader != null && _nullValueReader.isNull(docId)) {
        return _predicateEvaluator.applySV((BigDecimal) null);
      }
      return _predicateEvaluator.applySV(_forwardIndexReader.getBigDecimal(docId, _readerContext));
    }
  }

  private class StringMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_forwardIndexReader.getString(docId, _readerContext));
    }
  }

  private class BytesMatcher implements ValueMatcher {

    @Override
    public boolean doesValueMatch(int docId) {
      return _predicateEvaluator.applySV(_forwardIndexReader.getBytes(docId, _readerContext));
    }
  }
}
