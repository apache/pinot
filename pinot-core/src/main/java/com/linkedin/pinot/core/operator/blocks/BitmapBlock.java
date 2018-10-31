/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.blocks;

import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class BitmapBlock extends BaseFilterBlock {
  private final ImmutableRoaringBitmap[] _bitmaps;
  private final int _startDocId;
  // Inclusive
  private final int _endDocId;
  private final boolean _exclusive;

  public BitmapBlock(ImmutableRoaringBitmap[] bitmaps, int startDocId, int endDocId, boolean exclusive) {
    _bitmaps = bitmaps;
    _startDocId = startDocId;
    _endDocId = endDocId;
    _exclusive = exclusive;
  }

  @Override
  public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
    return new BitmapDocIdSet(_bitmaps, _startDocId, _endDocId, _exclusive);
  }
}