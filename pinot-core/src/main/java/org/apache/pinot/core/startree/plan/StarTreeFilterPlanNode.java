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
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.core.startree.operator.StarTreeFilterOperator;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


public class StarTreeFilterPlanNode implements PlanNode {
  private final StarTreeV2 _starTreeV2;
  private final Map<String, List<CompositePredicateEvaluator>> _predicateEvaluatorsMap;
  private final Set<String> _groupByColumns;
  private final Map<String, String> _debugOptions;
  public StarTreeFilterPlanNode(StarTreeV2 starTreeV2, Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap,
      @Nullable Set<String> groupByColumns, @Nullable Map<String, String> debugOptions) {
    _starTreeV2 = starTreeV2;
    _predicateEvaluatorsMap = predicateEvaluatorsMap;
    _groupByColumns = groupByColumns;
    _debugOptions = debugOptions;
  }

  @Override
  public StarTreeFilterOperator run() {
    return new StarTreeFilterOperator(_starTreeV2, _predicateEvaluatorsMap, _groupByColumns, _debugOptions);
  }
}
