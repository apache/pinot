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
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.core.operator.docidsets.OrBlockDocIdSet;


public class OrFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "OrFilterOperator";

  private List<BaseFilterOperator> _filterOperators;

  OrFilterOperator(List<BaseFilterOperator> filterOperators) {
    // NOTE:
    // EmptyFilterOperator and MatchAllFilterOperator should not be passed into the OrFilterOperator for performance
    // concern.
    // If there is any MatchAllFilterOperator inside OrFilterOperator, the whole OrFilterOperator is equivalent to a
    // MatchAllFilterOperator; EmptyFilterOperator should be ignored in OrFilterOperator.
    // After removing the EmptyFilterOperator, if there is no child filter operator left, use EmptyFilterOperator;
    // if there is only one child filter operator left, use the child filter operator directly.
    // These checks should be performed before constructing the OrFilterOperator.
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
    return new FilterBlock(new OrBlockDocIdSet(filterBlockDocIdSets));
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
