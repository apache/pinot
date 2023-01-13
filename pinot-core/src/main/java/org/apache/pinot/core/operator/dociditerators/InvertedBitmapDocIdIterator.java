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

import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class InvertedBitmapDocIdIterator implements BitmapBasedDocIdIterator {
  private final ImmutableRoaringBitmap _docIds;
  private final PeekableIntIterator _docIdIterator;
  private final int _lastDoc;
  private int _nextDocId;
  private int _nextNonMatchingDocId;

  public InvertedBitmapDocIdIterator(ImmutableRoaringBitmap docIds, int numDocs) {
    _docIds = docIds;
    _docIdIterator = docIds.getIntIterator();
    _nextDocId = 0;

    int currentDocIdFromChildIterator = _docIdIterator.next();
    _nextNonMatchingDocId = currentDocIdFromChildIterator == Constants.EOF ? numDocs : currentDocIdFromChildIterator;
    _lastDoc = numDocs - 1;
  }

  @Override
  public int next() {
    while (_nextDocId == _nextNonMatchingDocId && _docIdIterator.hasNext()) {
      _nextDocId++;
      int nextNonMatchingDocId = _docIdIterator.next();
      _nextNonMatchingDocId = nextNonMatchingDocId == Constants.EOF ? _lastDoc : nextNonMatchingDocId;
    }
    if (_nextDocId >= _lastDoc) {
      return Constants.EOF;
    }
    return _nextDocId++;
  }

  @Override
  public int advance(int targetDocId) {
    _nextDocId = targetDocId;
    if (targetDocId > _nextNonMatchingDocId) {
      _docIdIterator.advanceIfNeeded(targetDocId);
      _nextNonMatchingDocId = _docIdIterator.hasNext() ? _docIdIterator.next() : _lastDoc;
    }
    return next();
  }

  @Override
  public ImmutableRoaringBitmap getDocIds() {
    return _docIds;
  }

  @Override
  public boolean isInverted() {
    return true;
  }
}
