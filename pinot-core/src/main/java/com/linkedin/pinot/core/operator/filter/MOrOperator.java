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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;


/**
 * Or operator thats takes in more operators.
 *
 */
public class MOrOperator implements Operator {
  private static Logger LOGGER = Logger.getLogger(MOrOperator.class);

  private final Operator[] operators;

  public MOrOperator(List<Operator> operators) {
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
    for (final Operator operator : operators) {
      final Block nextBlock = operator.nextBlock();
      if (nextBlock != null) {
        blocks[i++] = nextBlock;
      }
    }
    return new ScanBasedOrBlock(blocks);
  }

  /**
   * Does not support accessing a specific block
   */
  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException("Not support nextBlock(BlockId BlockId) in MOrOperator");
  }

}
