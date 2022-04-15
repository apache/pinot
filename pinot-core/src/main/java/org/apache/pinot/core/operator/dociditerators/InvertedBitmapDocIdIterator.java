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
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class InvertedBitmapDocIdIterator extends BaseBitmapDocIdIterator {

  private int _next;
  private ImmutableRoaringBitmap _complement;

  public InvertedBitmapDocIdIterator(ImmutableRoaringBitmap docIds, int numDocs) {
    super(docIds, numDocs);
    nextBatch();
  }

  @Override
  public int next() {
    if (_next < _batch[_cursor]) {
      return _next++;
    }
    while (_next++ == _batch[_cursor++] & _next < _numDocs) {
      if (_cursor == _limit && !nextBatch()) {
        _cursor = 0;
        _batch[0] = _numDocs;
      }
    }
    return _next == _numDocs ? Constants.EOF : _next++;
  }

  @Override
  public int advance(int targetDocId) {
    _next = (int) _docIds.nextAbsentValue(targetDocId);
    _docIdIterator.advanceIfNeeded(_next);
    _cursor = _limit;
    nextBatch();
    return next();
  }

  @Override
  public ImmutableRoaringBitmap getDocIds() {
    if (_complement == null) {
      _complement = ImmutableRoaringBitmap.flip(_docIds, 0L, _numDocs);
    }
    return _complement;
  }
}
