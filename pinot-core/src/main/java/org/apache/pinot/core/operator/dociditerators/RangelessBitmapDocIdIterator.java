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


/**
 * The {@code RangelessBitmapDocIdIterator} is the bitmap-based iterator to iterate on a bitmap of matching document
 * ids. Comparing to the BitmapDocIdIterator, it does not have an explicit bound {@code [0, numDocs)} for the iteration,
 * but purely rely on the document ids stored in the bitmap.
 */
public final class RangelessBitmapDocIdIterator implements BitmapBasedDocIdIterator {
  private final ImmutableRoaringBitmap _docIds;
  private final PeekableIntIterator _docIdIterator;

  public RangelessBitmapDocIdIterator(ImmutableRoaringBitmap docIds) {
    _docIds = docIds;
    _docIdIterator = docIds.getIntIterator();
  }

  @Override
  public ImmutableRoaringBitmap getDocIds() {
    return _docIds;
  }

  @Override
  public int next() {
    if (hasNext()) {
      return _docIdIterator.next();
    } else {
      return Constants.EOF;
    }
  }

  @Override
  public int advance(int targetDocId) {
    _docIdIterator.advanceIfNeeded(targetDocId);
    return next();
  }

  public boolean hasNext() {
    return _docIdIterator.hasNext();
  }
}
