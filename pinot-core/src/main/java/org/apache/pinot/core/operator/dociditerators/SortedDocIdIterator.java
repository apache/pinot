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


/**
 * The {@code SortedDocIdIterator} is the iterator for SortedDocIdSet to iterate over a list of matching document id
 * ranges from a sorted column.
 */
public abstract class SortedDocIdIterator implements BlockDocIdIterator {
  protected final List<IntPair> _docIdRanges;
  protected final int _numRanges;

  protected int _currentRangeId;
  protected int _nextDocId;

  private SortedDocIdIterator(List<IntPair> docIdRanges) {
    _docIdRanges = docIdRanges;
    _numRanges = _docIdRanges.size();
    if (_numRanges == 0) {
      throw new IllegalArgumentException("Document ID ranges cannot be empty");
    }
    _nextDocId = docIdRanges.get(0).getLeft();
  }

  public static SortedDocIdIterator create(List<IntPair> docIdRanges, boolean ascending) {
    return ascending ? new Asc(docIdRanges) : new Desc(docIdRanges);
  }

  public List<IntPair> getDocIdRanges() {
    return _docIdRanges;
  }

  private static class Asc extends SortedDocIdIterator {
    private Asc(List<IntPair> docIdRanges) {
      super(docIdRanges);
      _nextDocId = docIdRanges.get(0).getLeft();
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

  private static class Desc extends SortedDocIdIterator {
    private Desc(List<IntPair> docIdRanges) {
      super(docIdRanges);
      _currentRangeId = _numRanges - 1;
      _nextDocId = docIdRanges.get(_currentRangeId).getRight();
    }

    @Override
    public int next() {
      IntPair currentRange = _docIdRanges.get(_currentRangeId);
      if (_nextDocId >= currentRange.getLeft()) {
        // Next document id is within the current range
        return _nextDocId--;
      }
      if (_currentRangeId > 0) {
        // Move to the next range
        _currentRangeId--;
        _nextDocId = _docIdRanges.get(_currentRangeId).getRight();
        return _nextDocId--;
      } else {
        return Constants.EOF;
      }
    }

    @Override
    public int advance(int targetDocId) {
      assert targetDocId <= _nextDocId : "Cannot advance to a larger document id in descending order";
      IntPair currentRange = _docIdRanges.get(_currentRangeId);
      if (targetDocId >= currentRange.getLeft()) {
        // Target document id is within the current range
        _nextDocId = Math.min(targetDocId, currentRange.getRight());
        return _nextDocId--;
      }
      while (_currentRangeId > 0) {
        // Move to the range that contains the target document id
        _currentRangeId--;
        currentRange = _docIdRanges.get(_currentRangeId);
        if (targetDocId >= currentRange.getLeft()) {
          _nextDocId = Math.min(targetDocId, currentRange.getRight());
          return _nextDocId--;
        }
      }
      return Constants.EOF;
    }
  }
}
