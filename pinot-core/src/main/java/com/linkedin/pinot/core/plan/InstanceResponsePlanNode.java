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

import com.linkedin.pinot.core.operator.UResultOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code></code>
 */
public class InstanceResponsePlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceResponsePlanNode.class);

  private final CombinePlanNode _combinePlanNode;

  public InstanceResponsePlanNode(CombinePlanNode combinePlanNode) {
    _combinePlanNode = combinePlanNode;
  }

  @Override
  public UResultOperator run() {
    long start = System.currentTimeMillis();
    UResultOperator uResultOperator = new UResultOperator(_combinePlanNode.run());
    long end = System.currentTimeMillis();
    LOGGER.debug("InstanceResponsePlanNode.run took: {}ms", end - start);
    return uResultOperator;
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Instance Level Inter-Segments Query Plan Node:");
    LOGGER.debug(prefix + "Operator: UResultOperator");
    LOGGER.debug(prefix + "Argument 0: Combine -");
    _combinePlanNode.showTree(prefix + "    ");
  }
}
