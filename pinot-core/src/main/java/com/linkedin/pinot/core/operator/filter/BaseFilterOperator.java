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

import com.linkedin.pinot.core.common.BaseFilterBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.BaseOperator;


/**
 * Base Operator for all filter operators. ResultBlock is initialized in the planning phase
 *
 */
public abstract class BaseFilterOperator extends BaseOperator {

  private FilterResultBlock resultBlock;
  private Predicate predicate;
  private int nextBlockCallCounter = 0;

  public void setInputBlock(FilterResultBlock block) {
    this.resultBlock = block;
  }

  public FilterResultBlock getResultBlock() {
    return resultBlock;
  }

  public void setPredicate(Predicate predicate) {
    this.predicate = predicate;
  }

  public Predicate getPredicate() {
    return predicate;
  }

  @Override
  public final BaseFilterBlock getNextBlock() {
    return getNextBlock(new BlockId(0));
  }

  @Override
  public final BaseFilterBlock getNextBlock(BlockId blockId) {
    if (nextBlockCallCounter > 0) {
      return null;
    }
    Block nextBlock = nextFilterBlock(new BlockId(0));
    nextBlockCallCounter = nextBlockCallCounter + 1;
    return (BaseFilterBlock) nextBlock;
  }

  @Override
  public final String getOperatorName() {
    return "BaseFilterOperator";
  }

  public abstract BaseFilterBlock nextFilterBlock(BlockId blockId);
}
