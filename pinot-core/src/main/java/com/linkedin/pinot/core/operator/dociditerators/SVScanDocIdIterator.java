/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.dociditerators;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class SVScanDocIdIterator implements ScanBasedDocIdIterator {
  private int _currentDocId = -1;
  private final BlockSingleValIterator _valueIterator;
  private int _startDocId;
  private int _endDocId;
  private PredicateEvaluator _evaluator;
  private String _datasourceName;
  private int _numEntriesScanned = 0;
  private final ValueMatcher _valueMatcher;

  public SVScanDocIdIterator(String datasourceName, BlockValSet blockValSet, BlockMetadata blockMetadata,
      PredicateEvaluator evaluator) {
    _datasourceName = datasourceName;
    _evaluator = evaluator;
    _valueIterator = (BlockSingleValIterator) blockValSet.iterator();

    if (evaluator.isAlwaysFalse()) {
      _currentDocId = Constants.EOF;
      setStartDocId(Constants.EOF);
      setEndDocId(Constants.EOF);
    } else {
      setStartDocId(blockMetadata.getStartDocId());
      setEndDocId(blockMetadata.getEndDocId());
    }

    if (evaluator.isDictionaryBased()) {
      _valueMatcher = new IntMatcher(); // Match using dictionary id's that are integers.
    } else {
      _valueMatcher = getValueMatcherForType(blockMetadata.getDataType());
    }
    _valueMatcher.setEvaluator(evaluator);
  }

  /**
   * After setting the startDocId, next calls will always return from &gt;=startDocId
   *
   * @param startDocId Start doc id
   */
  public void setStartDocId(int startDocId) {
    _currentDocId = startDocId - 1;
    _valueIterator.skipTo(startDocId);
    _startDocId = startDocId;
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds
   * endDocId
   *
   * @param endDocId End doc id
   */
  public void setEndDocId(int endDocId) {
    _endDocId = endDocId;
  }

  @Override
  public boolean isMatch(int docId) {
    if (_currentDocId == Constants.EOF) {
      return false;
    }
    _valueIterator.skipTo(docId);
    _numEntriesScanned++;
    return _valueMatcher.doesCurrentEntryMatch(_valueIterator);
  }

  @Override
  public int advance(int targetDocId) {
    if (_currentDocId == Constants.EOF) {
      return _currentDocId;
    }
    if (targetDocId < _startDocId) {
      targetDocId = _startDocId;
    } else if (targetDocId > _endDocId) {
      _currentDocId = Constants.EOF;
    }
    if (_currentDocId >= targetDocId) {
      return _currentDocId;
    } else {
      _currentDocId = targetDocId - 1;
      _valueIterator.skipTo(targetDocId);
      return next();
    }
  }

  @Override
  public int next() {
    if (_currentDocId == Constants.EOF) {
      return Constants.EOF;
    }
    while (_valueIterator.hasNext() && _currentDocId < _endDocId) {
      _currentDocId = _currentDocId + 1;
      _numEntriesScanned++;
      if (_valueMatcher.doesCurrentEntryMatch(_valueIterator)) {
        return _currentDocId;
      }
    }
    _currentDocId = Constants.EOF;
    return Constants.EOF;
  }

  @Override
  public int currentDocId() {
    return _currentDocId;
  }

  @Override
  public String toString() {
    return SVScanDocIdIterator.class.getSimpleName() + "[" + _datasourceName + "]";
  }

  @Override
  public MutableRoaringBitmap applyAnd(MutableRoaringBitmap answer) {
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    if (_evaluator.isAlwaysFalse()) {
      return result;
    }
    IntIterator intIterator = answer.getIntIterator();
    int docId = -1;
    while (intIterator.hasNext() && docId < _endDocId) {
      docId = intIterator.next();
      if (docId >= _startDocId) {
        _valueIterator.skipTo(docId);
        _numEntriesScanned++;
        if (_valueMatcher.doesCurrentEntryMatch(_valueIterator)) {
          result.add(docId);
        }
      }
    }
    return result;
  }

  @Override
  public int getNumEntriesScanned() {
    return _numEntriesScanned;
  }

  /**
   * Helper method to get value matcher for a given data type.
   * @param dataType data type for which to get the value matcher
   * @return value matcher for the data type.
   */
  private static ValueMatcher getValueMatcherForType(FieldSpec.DataType dataType) {

    switch (dataType) {
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

      default:
        throw new UnsupportedOperationException("Index without dictionary not supported for data type: " + dataType);
    }
  }

  private static abstract class ValueMatcher {
    protected PredicateEvaluator _evaluator;

    public void setEvaluator(PredicateEvaluator evaluator) {
      _evaluator = evaluator;
    }

    abstract boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator);
  }

  private static class IntMatcher extends ValueMatcher {

    @Override
    public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
      return _evaluator.applySV(valueIterator.nextIntVal());
    }
  }

  private static class LongMatcher extends ValueMatcher {

    @Override
    public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
      return _evaluator.applySV(valueIterator.nextLongVal());
    }
  }

  private static class FloatMatcher extends ValueMatcher {

    @Override
    public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
      return _evaluator.applySV(valueIterator.nextFloatVal());
    }
  }

  private static class DoubleMatcher extends ValueMatcher {

    @Override
    public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
      return _evaluator.applySV(valueIterator.nextDoubleVal());
    }
  }

  private static class StringMatcher extends ValueMatcher {

    @Override
    public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
      return _evaluator.applySV(valueIterator.nextStringVal());
    }
  }
}
