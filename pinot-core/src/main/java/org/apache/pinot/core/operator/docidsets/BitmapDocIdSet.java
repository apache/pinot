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


public class BitmapDocIdSet implements BlockDocIdSet {
  private final BitmapDocIdIterator _iterator;

  public BitmapDocIdSet(ImmutableRoaringBitmap docIds, int numDocs) {
    _iterator = new BitmapDocIdIterator(docIds, numDocs);
  }

  public BitmapDocIdSet(BitmapDocIdIterator iterator) {
    _iterator = iterator;
  }

  @Override
  public BitmapDocIdIterator iterator() {
    return _iterator;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return 0L;
  }
}
