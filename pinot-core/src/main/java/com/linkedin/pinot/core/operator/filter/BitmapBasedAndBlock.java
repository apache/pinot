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

import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.filter.utils.BitmapUtils;


public class BitmapBasedAndBlock implements Block {

  private static Logger LOGGER = Logger.getLogger(BitmapBasedAndBlock.class);

  private final Block[] blocks;

  int[] intersection;

  public BitmapBasedAndBlock(Block[] blocks) {
    this.blocks = blocks;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return false;
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException("Not support getId() in BitmapBasedAndBlock");
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException("Not support getMetadata() in BitmapBasedAndBlock");
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException("Not support getBlockValueSet() in BitmapBasedAndBlock");
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException("Not support getBlockDocIdValueSet() in BitmapBasedAndBlock");
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    final ImmutableRoaringBitmap[] bitMapArray = new ImmutableRoaringBitmap[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      bitMapArray[i] = (ImmutableRoaringBitmap) blocks[i].getBlockDocIdSet().getRaw();
    }
    MutableRoaringBitmap answer = BitmapUtils.fastBitmapsAnd(bitMapArray);
    return new BitmapBasedBlockDocIdSet(answer);

  }

}
