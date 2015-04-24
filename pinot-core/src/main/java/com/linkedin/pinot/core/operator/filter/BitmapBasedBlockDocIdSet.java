/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.filter;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.segment.index.block.BlockUtils;


public class BitmapBasedBlockDocIdSet implements BlockDocIdSet {

  private final MutableRoaringBitmap bit;

  public BitmapBasedBlockDocIdSet(MutableRoaringBitmap bitmap) {
    bit = bitmap;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return BlockUtils.getBLockDocIdSetBackedByBitmap(bit).iterator();
  }

  @Override
  public Object getRaw() {
    return bit;
  }
}
