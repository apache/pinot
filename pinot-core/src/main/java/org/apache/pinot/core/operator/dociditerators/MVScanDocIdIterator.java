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

import java.util.Arrays;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.BlockMultiValIterator;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Constants;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class MVScanDocIdIterator implements ScanBasedDocIdIterator {
  BlockMultiValIterator valueIterator;
  int currentDocId = -1;
  final int[] intArray;
  private int startDocId;
  private int endDocId;
  private PredicateEvaluator evaluator;

  private String datasourceName;
  private MutableInt _numEntriesScanned = new MutableInt(0);

  public MVScanDocIdIterator(String datasourceName, BlockValSet blockValSet, BlockMetadata blockMetadata,
      PredicateEvaluator evaluator) {
    this.datasourceName = datasourceName;
    this.evaluator = evaluator;
    if (evaluator.isAlwaysFalse()) {
      this.intArray = new int[0];
      setStartDocId(Constants.EOF);
      setEndDocId(Constants.EOF);
      currentDocId = Constants.EOF;
    } else {
      this.intArray = new int[blockMetadata.getMaxNumberOfMultiValues()];
      Arrays.fill(intArray, 0);
      setStartDocId(blockMetadata.getStartDocId());
      setEndDocId(blockMetadata.getEndDocId());
    }
    valueIterator = (BlockMultiValIterator) blockValSet.iterator();
  }

  /**
   * After setting the startDocId, next calls will always return from &gt;=startDocId
   * @param startDocId
   */
  public void setStartDocId(int startDocId) {
    this.startDocId = startDocId;
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds
   * endDocId
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
    int length = valueIterator.nextIntVal(intArray);
    return evaluator.applyMV(intArray, length, _numEntriesScanned);
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
      int next = next();
      return next;
    }
  }

  @Override
  public int next() {
    if (currentDocId == Constants.EOF) {
      return currentDocId;
    }
    while (valueIterator.hasNext() && currentDocId < endDocId) {
      currentDocId = currentDocId + 1;
      int length = valueIterator.nextIntVal(intArray);
      if (evaluator.applyMV(intArray, length, _numEntriesScanned)) {
        return currentDocId;
      }
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
    return MVScanDocIdIterator.class.getSimpleName() + "[" + datasourceName + "]";
  }

  @Override
  public MutableRoaringBitmap applyAnd(MutableRoaringBitmap answer) {

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    if (evaluator.isAlwaysFalse()) {
      return result;
    }
    IntIterator intIterator = answer.getIntIterator();
    int docId = -1, length;
    while (intIterator.hasNext() && docId < endDocId) {
      docId = intIterator.next();
      if (docId >= startDocId) {
        valueIterator.skipTo(docId);
        length = valueIterator.nextIntVal(intArray);
        if (evaluator.applyMV(intArray, length, _numEntriesScanned)) {
          result.add(docId);
        }
      }
    }
    return result;
  }

  @Override
  public int getNumEntriesScanned() {
    return _numEntriesScanned.intValue();
  }
}
