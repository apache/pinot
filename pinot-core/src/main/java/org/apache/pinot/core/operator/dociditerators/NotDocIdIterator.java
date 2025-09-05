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

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;


/**
 * The iterator performs a linear pass through the underlying child iterator and returns
 * the complement of the result set.
 */
public abstract class NotDocIdIterator implements BlockDocIdIterator {
  protected final BlockDocIdIterator _childDocIdIterator;
  protected final int _numDocs;

  private NotDocIdIterator(BlockDocIdIterator childDocIdIterator, int numDocs) {
    _childDocIdIterator = childDocIdIterator;
    _numDocs = numDocs;
  }

  public static NotDocIdIterator create(BlockDocIdIterator childDocIdIterator, int numDocs, boolean ascending) {
    return ascending ? new Asc(childDocIdIterator, numDocs) : new Desc(childDocIdIterator, numDocs);
  }

  private static final class Asc extends NotDocIdIterator {
    private int _nextDocId;
    private int _nextNonMatchingDocId;

    public Asc(BlockDocIdIterator childDocIdIterator, int numDocs) {
      super(childDocIdIterator, numDocs);
      int currentDocIdFromChildIterator = childDocIdIterator.next();

      _nextNonMatchingDocId = currentDocIdFromChildIterator == Constants.EOF ? numDocs : currentDocIdFromChildIterator;
    }

    @Override
    public int next() {
      if (_nextDocId >= _numDocs) {
        return Constants.EOF;
      }
      while (_nextDocId == _nextNonMatchingDocId) {
        _nextDocId++;
        int nextNonMatchingDocId = _childDocIdIterator.next();
        _nextNonMatchingDocId = nextNonMatchingDocId == Constants.EOF ? _numDocs : nextNonMatchingDocId;
      }
      if (_nextDocId >= _numDocs) {
        return Constants.EOF;
      }
      return _nextDocId++;
    }

    @Override
    public int advance(int targetDocId) {
      _nextDocId = targetDocId;
      if (targetDocId > _nextNonMatchingDocId) {
        int nextNonMatchingDocId = _childDocIdIterator.advance(targetDocId);
        _nextNonMatchingDocId = nextNonMatchingDocId == Constants.EOF ? _numDocs : nextNonMatchingDocId;
      }
      return next();
    }
  }

  private static final class Desc extends NotDocIdIterator {
    private int _nextDocId;
    private int _nextNonMatchingDocId;
    public Desc(BlockDocIdIterator childDocIdIterator, int numDocs) {
      super(childDocIdIterator, numDocs);

      int currentDocIdFromChildIterator = childDocIdIterator.next();
      _nextNonMatchingDocId = currentDocIdFromChildIterator == Constants.EOF ? numDocs : currentDocIdFromChildIterator;
      _nextDocId = numDocs - 1;
    }

    @Override
    public int next() {
      if (_nextDocId < 0) {
        return Constants.EOF;
      }
      while (_nextDocId == _nextNonMatchingDocId) {
        _nextDocId--;
        int nextNonMatchingDocId = _childDocIdIterator.next();
        _nextNonMatchingDocId = nextNonMatchingDocId == Constants.EOF ? 0 : nextNonMatchingDocId;
      }
      if (_nextDocId < 0) {
        return Constants.EOF;
      }
      return _nextDocId--;
    }

    @Override
    public int advance(int targetDocId) {
      _nextDocId = targetDocId;
      if (targetDocId < _nextNonMatchingDocId) {
        int nextNonMatchingDocId = _childDocIdIterator.advance(targetDocId);
        _nextNonMatchingDocId = nextNonMatchingDocId == Constants.EOF ? 0 : nextNonMatchingDocId;
      }
      return next();
    }
  }
}
