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


/// The iterator performs a linear pass through the underlying child iterator and returns
/// the complement of the result set.
public class NotDocIdIterator implements BlockDocIdIterator {
  private final BlockDocIdIterator _childDocIdIterator;
  private final int _numDocs;
  private int _nextDocId;
  private int _nextNonMatchingDocId;
  private boolean _closed;

  public NotDocIdIterator(BlockDocIdIterator childDocIdIterator, int numDocs) {
    _childDocIdIterator = childDocIdIterator;
    _numDocs = numDocs;
    _nextNonMatchingDocId = childDocIdIterator.next();
  }

  @Override
  public int next() {
    if (_closed) {
      return Constants.EOF;
    }
    while (_nextDocId < _numDocs && _nextDocId == _nextNonMatchingDocId) {
      _nextDocId++;
      _nextNonMatchingDocId = _childDocIdIterator.next();
    }
    if (_nextDocId >= _numDocs) {
      close();
      return Constants.EOF;
    }
    return _nextDocId++;
  }

  @Override
  public int advance(int targetDocId) {
    if (_closed) {
      return Constants.EOF;
    }
    _nextDocId = targetDocId;
    if (_nextNonMatchingDocId != Constants.EOF && targetDocId > _nextNonMatchingDocId) {
      _nextNonMatchingDocId = _childDocIdIterator.advance(targetDocId);
    }
    return next();
  }

  @Override
  public void close() {
    if (!_closed) {
      _closed = true;
      // Scans close themselves when they reach EOF.
      if (_nextNonMatchingDocId != Constants.EOF) {
        _childDocIdIterator.close();
      }
    }
  }
}
