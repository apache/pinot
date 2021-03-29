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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


public class StarTreeProjectionPlanNode implements PlanNode {
  private final Map<String, DataSource> _dataSourceMap;
  private final StarTreeDocIdSetPlanNode _starTreeDocIdSetPlanNode;

  public StarTreeProjectionPlanNode(StarTreeV2 starTreeV2, Set<String> projectionColumns,
      Map<String, List<PredicateEvaluator>> predicateEvaluatorsMap, @Nullable Set<String> groupByColumns,
      @Nullable Map<String, String> debugOptions) {
    _dataSourceMap = new HashMap<>();
    for (String projectionColumn : projectionColumns) {
      _dataSourceMap.put(projectionColumn, starTreeV2.getDataSource(projectionColumn));
    }
    _starTreeDocIdSetPlanNode =
        new StarTreeDocIdSetPlanNode(starTreeV2, predicateEvaluatorsMap, groupByColumns, debugOptions);
  }

  @Override
  public ProjectionOperator run() {
    return new ProjectionOperator(_dataSourceMap, _starTreeDocIdSetPlanNode.run());
  }
}
