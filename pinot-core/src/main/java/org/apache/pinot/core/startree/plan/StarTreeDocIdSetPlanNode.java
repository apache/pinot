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
package org.apache.pinot.core.startree.plan;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


public class StarTreeDocIdSetPlanNode implements PlanNode {
  private final StarTreeFilterPlanNode _starTreeFilterPlanNode;

  public StarTreeDocIdSetPlanNode(StarTreeV2 starTreeV2, Map<String, List<PredicateEvaluator>> predicateEvaluatorsMap,
      @Nullable Set<String> groupByColumns, @Nullable Map<String, String> debugOptions) {
    _starTreeFilterPlanNode =
        new StarTreeFilterPlanNode(starTreeV2, predicateEvaluatorsMap, groupByColumns, debugOptions);
  }

  @Override
  public DocIdSetOperator run() {
    return new DocIdSetOperator(_starTreeFilterPlanNode.run(), DocIdSetPlanNode.MAX_DOC_PER_CALL);
  }
}
