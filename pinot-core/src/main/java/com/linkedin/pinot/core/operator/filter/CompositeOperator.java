/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.blocks.CompositeBaseFilterBlock;
import java.util.ArrayList;
import java.util.List;

public class CompositeOperator extends BaseFilterOperator{
  private static final String OPERATOR_NAME = "CompositeOperator";
  private List<BaseFilterOperator> operators;

  public CompositeOperator(List<BaseFilterOperator> operators) {
    this.operators = operators;
  }

  @Override
  public boolean open() {
    return false;
  }

  @Override
  public boolean close() {
    return false;
  }

  @Override
  protected BaseFilterBlock getNextBlock() {
    List<BaseFilterBlock> blocks = new ArrayList<>();
    for(Operator operator:operators){
      blocks.add((BaseFilterBlock) operator.nextBlock());
    }
    return new CompositeBaseFilterBlock(blocks);
  }

  @Override
  public boolean isResultEmpty() {
    for (BaseFilterOperator operator : operators) {
      if (!operator.isResultEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
