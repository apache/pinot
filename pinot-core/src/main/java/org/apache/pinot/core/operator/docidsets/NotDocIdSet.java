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

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.NotDocIdIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class NotDocIdSet implements BlockDocIdSet {
  private final BlockDocIdSet _childDocIdSet;
  private final int _numDocs;

  public NotDocIdSet(BlockDocIdSet childDocIdSet, int numDocs) {
    _childDocIdSet = childDocIdSet;
    _numDocs = numDocs;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new NotDocIdIterator(_childDocIdSet.iterator(), _numDocs);
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return _childDocIdSet.getNumEntriesScannedInFilter();
  }

  @Override
  public boolean isApplyAndDeferrable() {
    return true;
  }

  @Override
  public void release() {
    _childDocIdSet.release();
  }

  @Override
  public ImmutableRoaringBitmap applyAnd(ImmutableRoaringBitmap docIds) {
    if (docIds.isEmpty()) {
      return new MutableRoaringBitmap();
    }
    // NOTE: The candidate set can carry document ids beyond numDocs, because BitmapDocIdIterator#getDocIds() hands out
    //       the raw bitmap while its next() stops at numDocs. An intersection cannot introduce such ids, but a
    //       complement can, so the bound NotDocIdIterator applies has to be applied here too.
    ImmutableRoaringBitmap boundedDocIds = bound(docIds, _numDocs);
    if (boundedDocIds.isEmpty()) {
      return new MutableRoaringBitmap();
    }
    // Within the candidate set, NOT(child) is the candidates the child does not match, so the child only has to be
    // evaluated on the candidates: (NOT child) AND docIds == docIds MINUS (child AND docIds).
    return ImmutableRoaringBitmap.andNot(boundedDocIds, _childDocIdSet.applyAnd(boundedDocIds));
  }

  /// Drops the document ids that are not below `numDocs`, without copying when there are none.
  static ImmutableRoaringBitmap bound(ImmutableRoaringBitmap docIds, int numDocs) {
    if (docIds.isEmpty() || docIds.last() < numDocs) {
      return docIds;
    }
    MutableRoaringBitmap boundedDocIds = docIds.toMutableRoaringBitmap();
    boundedDocIds.remove(numDocs, 0x1_0000_0000L);
    return boundedDocIds;
  }
}
