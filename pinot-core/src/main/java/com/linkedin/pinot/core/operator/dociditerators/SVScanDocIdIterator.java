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
      switch (blockMetadata.getDataType()) {
      case BOOLEAN:
        break;
      case BYTE:
        break;
      case BYTE_ARRAY:
        break;
      case CHAR:
        break;
      case CHAR_ARRAY:
        break;
      case DOUBLE:
        break;
      case DOUBLE_ARRAY:
        break;
      case FLOAT:
        break;
      case FLOAT_ARRAY:
        break;
      case INT:
        break;
      case INT_ARRAY:
        break;
      case LONG:
        break;
      case LONG_ARRAY:
        break;
      case OBJECT:
        break;
      case SHORT:
        break;
      case SHORT_ARRAY:
        break;
      case STRING:
        valueMatcher = new StringMatcher();
        break;
      case STRING_ARRAY:
        break;
      default:
        break;

      }
      valueMatcher.setEvaluator(evaluator);

    }
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

class StringMatcher extends ValueMatcher {

  @Override
  public boolean doesCurrentEntryMatch(BlockSingleValIterator valueIterator) {
    return evaluator.apply(valueIterator.nextStringVal());
  }
}