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
import org.roaringbitmap.PeekableIntIterator;


/// Returns the candidate document ids that the child does not match, lazily: `NOT(child) AND candidates` is
/// `candidates MINUS (child AND candidates)`. The child iterator must already be restricted to the candidates (see
/// [org.apache.pinot.core.common.BlockDocIdSet#restrictedIterator]), so it only evaluates candidate documents, and it
/// is only advanced as far as the candidates that this iterator returns.
///
/// Closes the child when the candidates end, also when the child has not reached its end. Not thread-safe.
public final class RestrictedNotDocIdIterator implements BlockDocIdIterator {
  private final PeekableIntIterator _candidateDocIdIterator;
  private final BlockDocIdIterator _childDocIdIterator;

  // The next document id that the child matches, at or after the last candidate checked; -1 before the first call
  private int _nextChildDocId = -1;
  private boolean _closed;

  /// @param candidateDocIdIterator the candidate document ids, in ascending order and below the number of documents
  /// @param childDocIdIterator the child, restricted to the same candidates
  public RestrictedNotDocIdIterator(PeekableIntIterator candidateDocIdIterator,
      BlockDocIdIterator childDocIdIterator) {
    _candidateDocIdIterator = candidateDocIdIterator;
    _childDocIdIterator = childDocIdIterator;
  }

  @Override
  public int next() {
    while (_candidateDocIdIterator.hasNext()) {
      int candidateDocId = _candidateDocIdIterator.next();
      if (_nextChildDocId < candidateDocId) {
        int childDocId = _childDocIdIterator.advance(candidateDocId);
        // A child at its end never matches again
        _nextChildDocId = childDocId == Constants.EOF ? Integer.MAX_VALUE : childDocId;
      }
      if (_nextChildDocId != candidateDocId) {
        return candidateDocId;
      }
    }
    close();
    return Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    _candidateDocIdIterator.advanceIfNeeded(targetDocId);
    return next();
  }

  @Override
  public void close() {
    if (!_closed) {
      _closed = true;
      _childDocIdIterator.close();
    }
  }
}
