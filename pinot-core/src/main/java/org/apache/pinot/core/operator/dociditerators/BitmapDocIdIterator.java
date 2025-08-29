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
 * The {@code BitmapDocIdIterator} is the bitmap-based iterator to iterate on a bitmap of matching document ids.
 */
public abstract class BitmapDocIdIterator implements BitmapBasedDocIdIterator {
  protected final ImmutableRoaringBitmap _docIds;
  protected final int _numDocs;

  private BitmapDocIdIterator(ImmutableRoaringBitmap docIds, int numDocs) {
    _docIds = docIds;
    _numDocs = numDocs;
  }

  public static BitmapDocIdIterator create(ImmutableRoaringBitmap docIds, int numDocs, boolean ascending) {
    return ascending ? new Asc(docIds, numDocs) : new Desc(docIds, numDocs);
  }

  public abstract boolean isAscending();

  @Override
  public ImmutableRoaringBitmap getDocIds() {
    return _docIds;
  }

  private static final class Asc extends BitmapDocIdIterator {
    private final PeekableIntIterator _docIdIterator;

    private Asc(ImmutableRoaringBitmap docIds, int numDocs) {
      super(docIds, numDocs);
      _docIdIterator = docIds.getIntIterator();
    }

    @Override
    public int next() {
      if (_docIdIterator.hasNext()) {
        int docId = _docIdIterator.next();
        if (docId < _numDocs) {
          return docId;
        }
      }
      return Constants.EOF;
    }

    @Override
    public int advance(int targetDocId) {
      _docIdIterator.advanceIfNeeded(targetDocId);
      return next();
    }

    @Override
    public boolean isAscending() {
      return true;
    }
  }

  private static final class Desc extends BitmapDocIdIterator {
    private final IntIterator _docIdIterator;

    private Desc(ImmutableRoaringBitmap docIds, int numDocs) {
      super(docIds, numDocs);
      _docIdIterator = docIds.getReverseIntIterator();
    }

    @Override
    public int next() {
      while (_docIdIterator.hasNext()) {
        int docId = _docIdIterator.next();
        if (docId >= _numDocs) { // this should not happen very often
          continue;
        }
        return docId;
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

    @Override
    public boolean isAscending() {
      return false;
    }
  }
}
