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
import org.apache.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class BitmapDocIdSet implements FilterBlockDocIdSet {
  private final ImmutableRoaringBitmap _bitmap;
  private int _startDocId;
  // Inclusive
  private int _endDocId;

  public BitmapDocIdSet(ImmutableRoaringBitmap[] bitmaps, int startDocId, int endDocId, boolean exclusive) {
    int numBitmaps = bitmaps.length;
    if (numBitmaps > 1) {
      MutableRoaringBitmap orBitmap = MutableRoaringBitmap.or(bitmaps);
      if (exclusive) {
        orBitmap.flip(startDocId, endDocId + 1);
      }
      _bitmap = orBitmap;
    } else if (numBitmaps == 1) {
      if (exclusive) {
        MutableRoaringBitmap bitmap = ImmutableRoaringBitmap.flip(bitmaps[0], startDocId, endDocId + 1);
        _bitmap = bitmap;
      } else {
        _bitmap = bitmaps[0];
      }
    } else {
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      if (exclusive) {
        bitmap.add(startDocId, endDocId + 1);
      }
      _bitmap = bitmap;
    }

    _startDocId = startDocId;
    _endDocId = endDocId;
  }

  @Override
  public int getMinDocId() {
    return _startDocId;
  }

  @Override
  public int getMaxDocId() {
    return _endDocId;
  }

  @Override
  public void setStartDocId(int startDocId) {
    _startDocId = startDocId;
  }

  @Override
  public void setEndDocId(int endDocId) {
    _endDocId = endDocId;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    // No value scanned when filtering with bitmap inverted index.
    return 0L;
  }

  @Override
  public BlockDocIdIterator iterator() {
    BitmapDocIdIterator bitmapDocIdIterator = new BitmapDocIdIterator(_bitmap.getIntIterator());
    bitmapDocIdIterator.setStartDocId(_startDocId);
    bitmapDocIdIterator.setEndDocId(_endDocId);
    return bitmapDocIdIterator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getRaw() {
    return (T) _bitmap;
  }
}
