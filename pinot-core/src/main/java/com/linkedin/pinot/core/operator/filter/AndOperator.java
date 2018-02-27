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

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.blocks.AndBlock;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import java.util.ArrayList;
import java.util.List;


public class AndOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "AndOperator";

  private List<BaseFilterOperator> operators;
  private AndBlock andBlock;

  public AndOperator(List<BaseFilterOperator> operators) {
    this.operators = operators;
  }

  @Override
  protected BaseFilterBlock getNextBlock() {
    List<FilterBlockDocIdSet> blockDocIdSets = new ArrayList<FilterBlockDocIdSet>();
    for (Operator operator : operators) {
      Block block = operator.nextBlock();
      FilterBlockDocIdSet blockDocIdSet = (FilterBlockDocIdSet) block.getBlockDocIdSet();
      blockDocIdSets.add(blockDocIdSet);
    }
    andBlock = new AndBlock(blockDocIdSets);
    return andBlock;
  }

  @Override
  public boolean isResultEmpty() {
    for (BaseFilterOperator operator : operators) {
      if (operator.isResultEmpty()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
