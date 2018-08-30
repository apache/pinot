/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.operator.blocks.FilterBlock;
import com.linkedin.pinot.core.operator.docidsets.AndBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import java.util.ArrayList;
import java.util.List;


public class AndFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "AndFilterOperator";

  private final List<BaseFilterOperator> _filterOperators;

  public AndFilterOperator(List<BaseFilterOperator> filterOperators) {
    _filterOperators = filterOperators;
  }

  @Override
  protected FilterBlock getNextBlock() {
    List<FilterBlockDocIdSet> filterBlockDocIdSets = new ArrayList<>(_filterOperators.size());
    for (BaseFilterOperator filterOperator : _filterOperators) {
      filterBlockDocIdSets.add(filterOperator.nextBlock().getBlockDocIdSet());
    }
    return new FilterBlock(new AndBlockDocIdSet(filterBlockDocIdSets));
  }

  @Override
  public boolean isResultEmpty() {
    for (BaseFilterOperator filterOperator : _filterOperators) {
      if (filterOperator.isResultEmpty()) {
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
