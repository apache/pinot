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

import com.linkedin.pinot.core.common.*;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.blocks.CompositeBaseFilterBlock;

import java.util.ArrayList;
import java.util.List;

public class CompositeStarTreeOperator extends BaseFilterOperator {
  private final List<StarTreeOperator> operators;

  public CompositeStarTreeOperator(List<StarTreeOperator> operators) {
    this.operators = operators;
  }

  @Override
  public BaseFilterBlock nextFilterBlock(BlockId blockId) {
    List<BaseFilterBlock> blocks = new ArrayList<>(operators.size());
    for (StarTreeOperator operator : operators) {
      blocks.add(operator.getNextBlock(blockId));
    }
    return new CompositeBaseFilterBlock(blocks);
  }

  @Override
  public boolean open() {
    for (StarTreeOperator operator : operators) {
      operator.open();
    }
    return true;
  }

  @Override
  public boolean close() {
    for (StarTreeOperator operator : operators) {
      operator.close();
    }
    return true;
  }
}
