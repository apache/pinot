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

import org.apache.pinot.core.common.BlockSingleValIterator;
import org.apache.pinot.core.common.Constants;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * The {@code SVScanDocIdIterator} is the scan-based iterator for SVScanDocIdSet to scan a single-value column for the
 * matching document ids.
 */
public final class SVScanDocIdIterator implements ScanBasedDocIdIterator {
  private final PredicateEvaluator _predicateEvaluator;
  private final BlockSingleValIterator _valueIterator;
  private final int _numDocs;
  private final ValueMatcher _valueMatcher;

  private int _nextDocId = 0;
  private long _numEntriesScanned = 0L;

  public SVScanDocIdIterator(PredicateEvaluator predicateEvaluator, BlockSingleValIterator valueIterator, int numDocs) {
    _predicateEvaluator = predicateEvaluator;
    _valueIterator = valueIterator;
    _numDocs = numDocs;
    _valueMatcher = getValueMatcher();
  }

  @Override
  public int next() {
    while (_nextDocId < _numDocs) {
      int nextDocId = _nextDocId++;
      _numEntriesScanned++;
      if (_valueMatcher.doesNextValueMatch()) {
        return nextDocId;
      }
    }
    return Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    _nextDocId = targetDocId;
    _valueIterator.skipTo(targetDocId);
    return next();
  }

  @Override
  public MutableRoaringBitmap applyAnd(ImmutableRoaringBitmap docIds) {
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    IntIterator docIdIterator = docIds.getIntIterator();
    int nextDocId;
    while (docIdIterator.hasNext() && (nextDocId = docIdIterator.next()) < _numDocs) {
      _valueIterator.skipTo(nextDocId);
      _numEntriesScanned++;
      if (_valueMatcher.doesNextValueMatch()) {
        result.add(nextDocId);
      }
    }
    return result;
  }

  @Override
  public long getNumEntriesScanned() {
    return _numEntriesScanned;
  }

  private ValueMatcher getValueMatcher() {
    switch (_predicateEvaluator.getDataType()) {
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
        throw new UnsupportedOperationException();
    }
  }

  private static abstract class ValueMatcher {

    abstract boolean doesNextValueMatch();
  }

  private class IntMatcher extends ValueMatcher {

    @Override
    boolean doesNextValueMatch() {
      return _predicateEvaluator.applySV(_valueIterator.nextIntVal());
    }
  }

  private class LongMatcher extends ValueMatcher {

    @Override
    boolean doesNextValueMatch() {
      return _predicateEvaluator.applySV(_valueIterator.nextLongVal());
    }
  }

  private class FloatMatcher extends ValueMatcher {

    @Override
    boolean doesNextValueMatch() {
      return _predicateEvaluator.applySV(_valueIterator.nextFloatVal());
    }
  }

  private class DoubleMatcher extends ValueMatcher {

    @Override
    boolean doesNextValueMatch() {
      return _predicateEvaluator.applySV(_valueIterator.nextDoubleVal());
    }
  }

  private class StringMatcher extends ValueMatcher {

    @Override
    boolean doesNextValueMatch() {
      return _predicateEvaluator.applySV(_valueIterator.nextStringVal());
    }
  }

  private class BytesMatcher extends ValueMatcher {

    @Override
    boolean doesNextValueMatch() {
      return _predicateEvaluator.applySV(_valueIterator.nextBytesVal());
    }
  }
}
