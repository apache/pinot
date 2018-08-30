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
package com.linkedin.pinot.core.startree.plan;

import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.operator.ProjectionOperator;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.startree.v2.StarTreeV2;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StarTreeProjectionPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeProjectionPlanNode.class);

  private final Map<String, DataSource> _dataSourceMap;
  private final StarTreeDocIdSetPlanNode _starTreeDocIdSetPlanNode;

  public StarTreeProjectionPlanNode(StarTreeV2 starTreeV2, Set<String> projectionColumns,
      @Nullable FilterQueryTree rootFilterNode, @Nullable Set<String> groupByColumns,
      @Nullable Map<String, String> debugOptions) {
    _dataSourceMap = new HashMap<>(projectionColumns.size());
    for (String projectionColumn : projectionColumns) {
      _dataSourceMap.put(projectionColumn, starTreeV2.getDataSource(projectionColumn));
    }
    _starTreeDocIdSetPlanNode = new StarTreeDocIdSetPlanNode(starTreeV2, rootFilterNode, groupByColumns, debugOptions);
  }

  @Override
  public ProjectionOperator run() {
    return new ProjectionOperator(_dataSourceMap, _starTreeDocIdSetPlanNode.run());
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "StarTree Projection Plan Node:");
    LOGGER.debug(prefix + "Operator: ProjectionOperator");
    LOGGER.debug(prefix + "Argument 0: Data Sources - " + _dataSourceMap.keySet());
    LOGGER.debug(prefix + "Argument 1: StarTreeDocIdSetPlanNode -");
    _starTreeDocIdSetPlanNode.showTree(prefix + "    ");
  }
}
