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

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public abstract class BaseBitmapDocIdIterator implements BitmapBasedDocIdIterator {

  public static BitmapBasedDocIdIterator of(ImmutableRoaringBitmap docIds, int numDocs, boolean inverted) {
    return inverted ? new InvertedBitmapDocIdIterator(docIds, numDocs) : new BitmapDocIdIterator(docIds, numDocs);
  }

  protected final ImmutableRoaringBitmap _docIds;
  protected final BatchIterator _docIdIterator;
  protected final int[] _batch;
  protected final int _numDocs;

  protected int _limit;
  protected int _cursor;

  protected BaseBitmapDocIdIterator(ImmutableRoaringBitmap docIds, int numDocs) {
    _docIds = docIds;
    _docIdIterator = docIds.getBatchIterator();
    _batch = new int[256];
    _numDocs = numDocs;
  }

  protected boolean nextBatch() {
    if (!_docIdIterator.hasNext()) {
      return false;
    }
    _limit = _docIdIterator.nextBatch(_batch);
    _cursor = 0;
    return _limit > 0;
  }
}
