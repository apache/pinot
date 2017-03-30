/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import javax.annotation.Nullable;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;

public class SVScanDocIdIterator implements ScanBasedDocIdIterator {
  int currentDocId = -1;
  BlockSingleValIterator valueIterator;
  private int startDocId;
  private int endDocId;
  protected PredicateEvaluator evaluator;
  private String datasourceName;
  private int _numEntriesScanned = 0;
  ValueMatcher valueMatcher;

  public SVScanDocIdIterator(String datasourceName, BlockValSet blockValSet, BlockMetadata blockMetadata, PredicateEvaluator evaluator) {
    this.datasourceName = datasourceName;
    this.evaluator = evaluator;
    valueIterator = (BlockSingleValIterator) blockValSet.iterator();

    if (evaluator.alwaysFalse()) {
      currentDocId = Constants.EOF;
      setStartDocId(Constants.EOF);
      setEndDocId(Constants.EOF);
    } else {
      setStartDocId(blockMetadata.getStartDocId());
      setEndDocId(blockMetadata.getEndDocId());
    }

    if (blockMetadata.hasDictionary()) {
      valueMatcher = new IntMatcher();
    } else {
      valueMatcher = getValueMatcherForType(blockMetadata.getDataType());
    }
    valueMatcher.setEvaluator(evaluator);
  }

  /**
   * After setting the startDocId, next calls will always return from &gt;=startDocId
   *
   * @param startDocId
   */
  public void setStartDocId(int startDocId) {
    currentDocId = startDocId - 1;
    valueIterator.skipTo(startDocId);
    this.startDocId = startDocId;
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds
   * endDocId
   *
   * @param endDocId
   */
  public void setEndDocId(int endDocId) {
    this.endDocId = endDocId;
  }

  @Override
  public boolean isMatch(int docId) {
    if (currentDocId == Constants.EOF) {
      return false;
    }
    valueIterator.skipTo(docId);
    _numEntriesScanned++;
    // int dictIdForCurrentDoc = valueIterator.nextIntVal();
    // return evaluator.apply(dictIdForCurrentDoc);
    return valueMatcher.doesCurrentEntryMatch(valueIterator);
  }

  @Override
  public int advance(int targetDocId) {
    if (currentDocId == Constants.EOF) {
      return currentDocId;
    }
    if (targetDocId < startDocId) {
      targetDocId = startDocId;
    } else if (targetDocId > endDocId) {
      currentDocId = Constants.EOF;
    }
    if (currentDocId >= targetDocId) {
      return currentDocId;
    } else {
      currentDocId = targetDocId - 1;
      valueIterator.skipTo(targetDocId);
      return next();
    }
  }

  @Override
  public int next() {
    if (currentDocId == Constants.EOF) {
      return Constants.EOF;
    }
    while (valueIterator.hasNext() && currentDocId < endDocId) {
      currentDocId = currentDocId + 1;
      _numEntriesScanned++;
      if (valueMatcher.doesCurrentEntryMatch(valueIterator)) {
        return currentDocId;
      }
      // int dictIdForCurrentDoc = valueIterator.nextIntVal();
      // if (evaluator.apply(dictIdForCurrentDoc)) {
      // // System.out.println("Returning deom " + this +"doc Id:"+ currentDocId + " dictId:"+
      // // dictIdForCurrentDoc);
      // return currentDocId;
      // }
    }
    currentDocId = Constants.EOF;
    return Constants.EOF;
  }

  @Override
  public int currentDocId() {
    return currentDocId;
  }

  @Override
  public String toString() {
    return SVScanDocIdIterator.class.getSimpleName() + "[" + datasourceName + "]";
  }

  @Override
  public MutableRoaringBitmap applyAnd(MutableRoaringBitmap answer) {
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    if (evaluator.alwaysFalse()) {
      return result;
    }
    IntIterator intIterator = answer.getIntIterator();
    int docId = -1;
    while (intIterator.hasNext() && docId < endDocId) {
      docId = intIterator.next();
      if (docId >= startDocId) {
        valueIterator.skipTo(docId);
        _numEntriesScanned++;
        // if (evaluator.apply(valueIterator.nextIntVal())) {
        // result.add(docId);
        // }
        if (valueMatcher.doesCurrentEntryMatch(valueIterator)) {
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
  @Nullable
  private static ValueMatcher getValueMatcherForType(FieldSpec.DataType dataType) {
    ValueMatcher valueMatcher;

    switch (dataType) {
      case INT:
        valueMatcher = new IntMatcher();
        break;

      case LONG:
        valueMatcher = new LongMatcher();
        break;

      case FLOAT:
        valueMatcher = new FloatMatcher();
        break;

      case DOUBLE:
        valueMatcher = new DoubleMatcher();
        break;

      case STRING:
        valueMatcher = new StringMatcher();
        break;

      default:
        throw new UnsupportedOperationException("Index without dictionary not supported for data type: " + dataType);

    }
    return valueMatcher;
  }
}

abstract class ValueMatcher {
  protected PredicateEvaluator evaluator;

  public void setEvaluator(PredicateEvaluator evaluator) {
    this.evaluator = evaluator;
  }

  abstract boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator);
}

class IntMatcher extends ValueMatcher {

  @Override
  public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
    return evaluator.apply(valueIterator.nextIntVal());
  }
}

class LongMatcher extends ValueMatcher {

  @Override
  public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
    return evaluator.apply(valueIterator.nextLongVal());
  }
}

class FloatMatcher extends ValueMatcher {

  @Override
  public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
    return evaluator.apply(valueIterator.nextFloatVal());
  }
}

class DoubleMatcher extends ValueMatcher {

  @Override
  public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
    return evaluator.apply(valueIterator.nextDoubleVal());
  }
}

class StringMatcher extends ValueMatcher {

  @Override
  public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
    return evaluator.apply(valueIterator.nextStringVal());
  }
}
