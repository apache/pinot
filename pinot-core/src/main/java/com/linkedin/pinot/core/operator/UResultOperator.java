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
package com.linkedin.pinot.core.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.block.query.InstanceResponseBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;


/**
 * UResultOperator now only take one argument, wrap the operator to InstanceResponseBlock.
 * For now it's always MCombineOperator.
 *
 * @author xiafu
 *
 */
public class UResultOperator implements Operator {

  private static final Logger LOGGER = LoggerFactory.getLogger(UResultOperator.class);

  private final Operator _operator;

  public UResultOperator(Operator combinedOperator) {
    _operator = combinedOperator;
  }

  @Override
  public boolean open() {
    _operator.open();
    return true;
  }

  @Override
  public Block nextBlock() {
    long start = System.currentTimeMillis();
    InstanceResponseBlock instanceResponseBlock = new InstanceResponseBlock(_operator.nextBlock());
    long end = System.currentTimeMillis();
    LOGGER.debug("Time spent in UResultOperator:" + (end - start));
    return instanceResponseBlock;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean close() {
    _operator.close();
    return true;
  }

}
