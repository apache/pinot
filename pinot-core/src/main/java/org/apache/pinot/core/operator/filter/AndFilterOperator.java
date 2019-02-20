/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.filter;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.AndBlockDocIdSet;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;


public class AndFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "AndFilterOperator";

  private final List<BaseFilterOperator> _filterOperators;

  AndFilterOperator(List<BaseFilterOperator> filterOperators) {
    // NOTE:
    // EmptyFilterOperator and MatchAllFilterOperator should not be passed into the AndFilterOperator for performance
    // concern.
    // If there is any EmptyFilterOperator inside AndFilterOperator, the whole AndFilterOperator is equivalent to a
    // EmptyFilterOperator; MatchAllFilterOperator should be ignored in AndFilterOperator.
    // After removing the MatchAllFilterOperators, if there is no child filter operator left, use
    // MatchAllFilterOperator; if there is only one child filter operator left, use the child filter operator directly.
    // These checks should be performed before constructing the AndFilterOperator.
    for (BaseFilterOperator filterOperator : filterOperators) {
      Preconditions.checkArgument(!filterOperator.isResultEmpty() && !filterOperator.isResultMatchingAll());
    }

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
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
