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
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * The {@code RangelessBitmapDocIdIterator} is the bitmap-based iterator to iterate on a bitmap of matching document
 * ids. Comparing to the {@link BitmapDocIdIterator}, it does not have an explicit bound {@code [0, numDocs)} for the
 * iteration, but purely rely on the document ids stored in the bitmap.
 */
public abstract class RangelessBitmapDocIdIterator implements BitmapBasedDocIdIterator {
  protected final ImmutableRoaringBitmap _docIds;

  private RangelessBitmapDocIdIterator(ImmutableRoaringBitmap docIds) {
    _docIds = docIds;
  }

  public static RangelessBitmapDocIdIterator create(ImmutableRoaringBitmap docIds, boolean ascending) {
    return ascending ? new Asc(docIds) : new Desc(docIds);
  }

  @Override
  public ImmutableRoaringBitmap getDocIds() {
    return _docIds;
  }

  private static class Asc extends RangelessBitmapDocIdIterator {
    protected final PeekableIntIterator _docIdIterator;

    private Asc(ImmutableRoaringBitmap docIds) {
      super(docIds);
      _docIdIterator = docIds.getIntIterator();
    }

    @Override
    public int next() {
      if (_docIdIterator.hasNext()) {
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
  }

  private static class Desc extends RangelessBitmapDocIdIterator {
    private final IntIterator _reverseIterator;

    private Desc(ImmutableRoaringBitmap docIds) {
      super(docIds);
      _reverseIterator = docIds.getReverseIntIterator();
    }

    @Override
    public int next() {
      if (_reverseIterator.hasNext()) {
        return _reverseIterator.next();
      }
      return Constants.EOF;
    }

    @Override
    public int advance(int targetDocId) {
      int next = next();
      while (next > targetDocId) {
        assert next != Constants.EOF : "This code assumes that Constants.EOF is < 0";
        next = next();
      }
      return next;
    }
  }
}
