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

import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.utils.Pairs.IntPair;

import static com.google.common.base.Preconditions.checkArgument;


/// The `SortedDocIdIterator` is the iterator for SortedDocIdSet to iterate over a list of matching document id
/// ranges from a sorted column.
public final class SortedDocIdIterator implements BlockDocIdIterator {
  private final List<IntPair> _docIdRanges;
  private final int _numRanges;

  private int _currentRangeId = 0;
  private int _nextDocId;

  public SortedDocIdIterator(List<IntPair> docIdRanges) {
    _docIdRanges = docIdRanges;
    _numRanges = _docIdRanges.size();
    _nextDocId = docIdRanges.get(0).getLeft();
  }

  public List<IntPair> getDocIdRanges() {
    return _docIdRanges;
  }

  @Override
  public int next() {
    IntPair currentRange = _docIdRanges.get(_currentRangeId);
    if (_nextDocId <= currentRange.getRight()) {
      // Next document id is within the current range
      return _nextDocId++;
    }
    if (_currentRangeId < _numRanges - 1) {
      // Move to the next range
      _currentRangeId++;
      _nextDocId = _docIdRanges.get(_currentRangeId).getLeft();
      return _nextDocId++;
    } else {
      return Constants.EOF;
    }
  }

  @Override
  public int nextBatch(int[] output, int maxDocs) {
    checkArgument(maxDocs > 0 && maxDocs <= output.length);
    int size = 0;
    while (size < maxDocs) {
      IntPair currentRange = _docIdRanges.get(_currentRangeId);
      if (_nextDocId > currentRange.getRight()) {
        if (_currentRangeId == _numRanges - 1) {
          break;
        }
        currentRange = _docIdRanges.get(++_currentRangeId);
        _nextDocId = currentRange.getLeft();
      }
      int nextDocId = _nextDocId;
      int numDocs = Math.min(maxDocs - size, currentRange.getRight() - nextDocId + 1);
      for (int i = 0; i < numDocs; i++) {
        output[size + i] = nextDocId + i;
      }
      size += numDocs;
      _nextDocId += numDocs;
    }
    return size;
  }

  @Override
  public int advance(int targetDocId) {
    IntPair currentRange = _docIdRanges.get(_currentRangeId);
    if (targetDocId <= currentRange.getRight()) {
      // Target document id is within the current range
      _nextDocId = Math.max(targetDocId, currentRange.getLeft());
      return _nextDocId++;
    }
    while (_currentRangeId < _numRanges - 1) {
      // Move to the range that contains the target document id
      _currentRangeId++;
      currentRange = _docIdRanges.get(_currentRangeId);
      if (targetDocId <= currentRange.getRight()) {
        _nextDocId = Math.max(targetDocId, currentRange.getLeft());
        return _nextDocId++;
      }
    }
    return Constants.EOF;
  }
}
