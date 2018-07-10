/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.docidsets;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.operator.dociditerators.BitmapDocIdIterator;

public final class BitmapBasedBlockDocIdSet implements BlockDocIdSet {
  private final ImmutableRoaringBitmap bitmap;

  public BitmapBasedBlockDocIdSet(ImmutableRoaringBitmap bitmap) {
    this.bitmap = bitmap;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new BitmapDocIdIterator(bitmap.getIntIterator());
  }

  @Override
  public <T> T getRaw() {
    return null;
  }

}
