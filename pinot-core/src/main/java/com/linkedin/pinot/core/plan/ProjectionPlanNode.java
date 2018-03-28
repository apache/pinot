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
package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.ProjectionOperator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ProjectionPlanNode takes care of a map from column name to its corresponding
 * data source.
 */
public class ProjectionPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionPlanNode.class);

  private final DocIdSetPlanNode _docIdSetPlanNode;
  private final Map<String, DataSourcePlanNode> _dataSourcePlanNodeMap;

  public ProjectionPlanNode(@Nonnull IndexSegment indexSegment, @Nonnull Set<String> columns,
      @Nonnull DocIdSetPlanNode docIdSetPlanNode) {
    _docIdSetPlanNode = docIdSetPlanNode;
    _dataSourcePlanNodeMap = new HashMap<>(columns.size());
    for (String column : columns) {
      _dataSourcePlanNodeMap.put(column, new DataSourcePlanNode(indexSegment, column));
    }
  }

  @Override
  public ProjectionOperator run() {
    Map<String, DataSource> dataSourceMap = new HashMap<>(_dataSourcePlanNodeMap.size());
    for (Map.Entry<String, DataSourcePlanNode> entry : _dataSourcePlanNodeMap.entrySet()) {
      dataSourceMap.put(entry.getKey(), entry.getValue().run());
    }
    return new ProjectionOperator(dataSourceMap, _docIdSetPlanNode.run());
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Operator: ProjectionOperator");
    LOGGER.debug(prefix + "Argument 0: DocIdSet - ");
    _docIdSetPlanNode.showTree(prefix + "    ");
    int i = 0;
    for (String column : _dataSourcePlanNodeMap.keySet()) {
      LOGGER.debug(prefix + "Argument " + (i + 1) + ": DataSourceOperator");
      _dataSourcePlanNodeMap.get(column).showTree(prefix + "    ");
      i++;
    }
  }
}
