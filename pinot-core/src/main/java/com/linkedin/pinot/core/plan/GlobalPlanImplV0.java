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
package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.operator.InstanceResponseOperator;
import com.linkedin.pinot.core.operator.blocks.InstanceResponseBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GlobalPlan for a query applied to all the pruned segments.
 *
 *
 */
public class GlobalPlanImplV0 implements Plan {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalPlanImplV0.class);

  private final InstanceResponsePlanNode _instanceResponsePlanNode;

  public GlobalPlanImplV0(InstanceResponsePlanNode instanceResponsePlanNode) {
    _instanceResponsePlanNode = instanceResponsePlanNode;
  }

  @Override
  public DataTable execute() {
    long startTime = System.currentTimeMillis();
    InstanceResponseOperator instanceResponseOperator = _instanceResponsePlanNode.run();
    long endTime1 = System.currentTimeMillis();
    LOGGER.debug("InstanceResponsePlanNode.run() took: {}ms", endTime1 - startTime);
    InstanceResponseBlock instanceResponseBlock = instanceResponseOperator.nextBlock();
    long endTime2 = System.currentTimeMillis();
    LOGGER.debug("InstanceResponseOperator.nextBlock() took: {}ms", endTime2 - endTime1);
    return instanceResponseBlock.getInstanceResponseDataTable();
  }

  @Override
  public void print() {
    _instanceResponsePlanNode.showTree("");
  }
}
