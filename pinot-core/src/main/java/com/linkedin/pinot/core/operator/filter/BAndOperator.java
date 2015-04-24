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

import java.util.List;

import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.IntBlockDocIdSet;
import com.linkedin.pinot.core.operator.filter.utils.BitmapUtils;


/**
 * Boolean AND operator thats takes in two are more operators.
 *
 */
public class BAndOperator implements Operator {
  private static Logger LOGGER = Logger.getLogger(BAndOperator.class);

  private final Operator[] operators;

  public BAndOperator(Operator left, Operator right) {
    operators = new Operator[] { left, right };
  }

  public BAndOperator(Operator... operators) {
    this.operators = operators;
  }

  public BAndOperator(List<Operator> operators) {
    this.operators = new Operator[operators.size()];
    operators.toArray(this.operators);
  }

  @Override
  public boolean open() {
    for (final Operator operator : operators) {
      operator.open();
    }
    return true;
  }

  @Override
  public boolean close() {
    for (final Operator operator : operators) {
      operator.close();
    }
    return true;
  }

  @Override
  public Block nextBlock() {
    final Block[] blocks = new Block[operators.length];
    int i = 0;
    boolean isAnyBlockEmpty = false;
    for (final Operator operator : operators) {
      final Block nextBlock = operator.nextBlock();
      if (nextBlock == null) {
        isAnyBlockEmpty = true;
      }
      blocks[i++] = nextBlock;
    }
    if (isAnyBlockEmpty) {
      return null;
    }
    return new AndBlock(blocks);
  }

  /**
   * Does not support accessing a specific block
   */
  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

}

class AndBlock implements Block {
  private static Logger LOGGER = Logger.getLogger(AndBlock.class);

  private final Block[] blocks;

  int[] intersection;

  public AndBlock(Block[] blocks) {
    this.blocks = blocks;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return false;
  }

  @Override
  public BlockId getId() {
    return null;
  }

  @Override
  public BlockMetadata getMetadata() {
    return null;
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return null;
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {

    return null;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    final ImmutableRoaringBitmap[] bitMapArray = new ImmutableRoaringBitmap[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      bitMapArray[i] = (ImmutableRoaringBitmap) blocks[i].getBlockDocIdSet().getRaw();
    }
    MutableRoaringBitmap answer = BitmapUtils.fastAnd(bitMapArray);
    return new IntBlockDocIdSet(answer);
  }
}
