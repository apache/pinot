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
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.startree.operator.StarTreeFilterOperator;
import com.linkedin.pinot.core.startree.v2.StarTreeV2;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StarTreeFilterPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeFilterPlanNode.class);

  private final StarTreeV2 _starTreeV2;
  private final FilterQueryTree _rootFilterNode;
  private final Set<String> _groupByColumns;
  private final Map<String, String> _debugOptions;

  public StarTreeFilterPlanNode(StarTreeV2 starTreeV2, @Nullable FilterQueryTree rootFilterNode,
      @Nullable Set<String> groupByColumns, @Nullable Map<String, String> debugOptions) {
    _starTreeV2 = starTreeV2;
    _rootFilterNode = rootFilterNode;
    _groupByColumns = groupByColumns;
    _debugOptions = debugOptions;
  }

  @Override
  public StarTreeFilterOperator run() {
    return new StarTreeFilterOperator(_starTreeV2, _rootFilterNode, _groupByColumns, _debugOptions);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "StarTree Filter Plan Node:");
    LOGGER.debug(prefix + "Operator: StarTreeFilterOperator");
    LOGGER.debug(prefix + "Argument 0: Filters - " + _rootFilterNode);
    LOGGER.debug(prefix + "Argument 1: Group-by Columns - " + _groupByColumns);
  }
}
