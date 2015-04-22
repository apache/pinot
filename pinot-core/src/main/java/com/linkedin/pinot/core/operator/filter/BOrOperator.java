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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.IntBlockDocIdSet;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;


public class BOrOperator implements Operator {
  private static Logger LOGGER = LoggerFactory.getLogger(ServerQueryExecutorV1Impl.class);

  private final Operator[] operators;

  public BOrOperator(Operator left, Operator right) {
    operators = new Operator[] { left, right };
  }

  public BOrOperator(Operator... operators) {
    this.operators = operators;
  }

  public BOrOperator(List<Operator> operators) {
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
    return new OrBlock(blocks);
  }

  @Override
  public Block nextBlock(BlockId BlockId) {

    return null;
  }

}

class OrBlock implements Block {
  private static Logger LOGGER = LoggerFactory.getLogger(OrBlock.class);

  private final Block[] blocks;

  int[] union;

  public OrBlock(Block[] blocks) {
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
    long start = System.currentTimeMillis();
    final ImmutableRoaringBitmap[] bitMapArray = new ImmutableRoaringBitmap[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      bitMapArray[i] = (ImmutableRoaringBitmap) blocks[i].getBlockDocIdSet().getRaw();
    }
    MutableRoaringBitmap answer;
    if (blocks.length == 1) {
      answer = new MutableRoaringBitmap();
      answer.or(bitMapArray[0]);
    } else if (blocks.length == 2) {
      answer = ImmutableRoaringBitmap.or(bitMapArray[0], bitMapArray[1]);
    } else {
      //if we have more than 2 bitmaps to intersect, re order them so that we use the bitmaps according to the number of bits set to 1
      PriorityQueue<ImmutableRoaringBitmap> pq =
          new PriorityQueue<ImmutableRoaringBitmap>(blocks.length, new Comparator<ImmutableRoaringBitmap>() {
            @Override
            public int compare(ImmutableRoaringBitmap a, ImmutableRoaringBitmap b) {
              return a.getSizeInBytes() - b.getSizeInBytes();
            }
          });
      for (int srcId = 0; srcId < blocks.length; srcId++) {
        pq.add(bitMapArray[srcId]);
      }
      ImmutableRoaringBitmap x1 = pq.poll();
      ImmutableRoaringBitmap x2 = pq.poll();
      answer = ImmutableRoaringBitmap.or(x1, x2);
      while (pq.size() > 0) {
        answer.or(pq.poll());
      }
    }
    //turn this on manually if we want to compare optimized and unoptimized version
    boolean validate = false;
    if (validate) {
      final MutableRoaringBitmap bit =
          ((ImmutableRoaringBitmap) blocks[0].getBlockDocIdSet().getRaw()).toMutableRoaringBitmap();
      for (int srcId = 1; srcId < blocks.length; srcId++) {
        final MutableRoaringBitmap bitToAndWith =
            ((ImmutableRoaringBitmap) blocks[srcId].getBlockDocIdSet().getRaw()).toMutableRoaringBitmap();
        bit.or(bitToAndWith);
      }
      if (!answer.equals(bit)) {
        LOGGER.error("Optimized result differs from unoptimized solution, \n\t optimized: " + answer
            + " \n\t unoptimized: " + bit);
      }
    }
    long end = System.currentTimeMillis();
    LOGGER.info("And operator took: " + (end - start));
    return new IntBlockDocIdSet(answer);
  }

}
