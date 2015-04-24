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

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;


public class BBitmapOnlyOrOperator implements Operator {
  private static final Logger LOGGER = Logger.getLogger(BBitmapOnlyOrOperator.class);

  private final Operator[] operators;

  public BBitmapOnlyOrOperator(Operator left, Operator right) {
    operators = new Operator[] { left, right };
  }

  public BBitmapOnlyOrOperator(Operator... operators) {
    this.operators = operators;
  }

  public BBitmapOnlyOrOperator(List<Operator> operators) {
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
    return new BitmapBasedOrBlock(blocks);
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException("Not support nextBlock(BlockId BlockId) in BBitmapOnlyOrOperator");
  }

}
