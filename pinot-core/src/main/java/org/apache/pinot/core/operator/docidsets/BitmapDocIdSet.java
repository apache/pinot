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
package org.apache.pinot.core.operator.docidsets;

import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public final class BitmapDocIdSet implements BlockDocIdSet {
  private final BitmapDocIdIterator _iterator;
  private final long _numEntriesScannedInFilter;

  /// Returns a doc id set over the given documents, or [EmptyDocIdSet] when there is none.
  public static BlockDocIdSet create(ImmutableRoaringBitmap docIds, int numDocs) {
    return docIds.isEmpty() ? EmptyDocIdSet.unscanned() : new BitmapDocIdSet(docIds, numDocs, 0L);
  }

  /// Returns a doc id set over the given documents found by scanning `numEntriesScannedInFilter` entries, or
  /// [EmptyDocIdSet] when there is none.
  public static BlockDocIdSet create(ImmutableRoaringBitmap docIds, int numDocs, long numEntriesScannedInFilter) {
    return docIds.isEmpty()
        ? new EmptyDocIdSet(numEntriesScannedInFilter)
        : new BitmapDocIdSet(docIds, numDocs, numEntriesScannedInFilter);
  }

  /// Returns a doc id set over the documents of the given iterator, or [EmptyDocIdSet] when there is none.
  public static BlockDocIdSet create(BitmapDocIdIterator iterator) {
    return iterator.getDocIds().isEmpty() ? EmptyDocIdSet.unscanned() : new BitmapDocIdSet(iterator);
  }

  private BitmapDocIdSet(ImmutableRoaringBitmap docIds, int numDocs, long numEntriesScannedInFilter) {
    _iterator = new BitmapDocIdIterator(docIds, numDocs);
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
  }

  private BitmapDocIdSet(BitmapDocIdIterator iterator) {
    _iterator = iterator;
    _numEntriesScannedInFilter = 0L;
  }

  @Override
  public BitmapDocIdIterator iterator() {
    return _iterator;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }
}
