/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.block.query.InstanceResponseBlock;
import com.linkedin.pinot.core.operator.UResultOperator;


/**
 * GlobalPlan for a query applied to all the pruned segments.
 *
 *
 */
public class GlobalPlanImplV0 extends Plan {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalPlanImplV0.class);

  private InstanceResponsePlanNode _rootNode;
  private DataTable _instanceResponseDataTable;

  public GlobalPlanImplV0(InstanceResponsePlanNode rootNode) {
    _rootNode = rootNode;
  }

  @Override
  public void print() {
    _rootNode.showTree("");
  }

  @Override
  public PlanNode getRoot() {
    return _rootNode;
  }

  @Override
  public void execute() {
    long startTime = System.currentTimeMillis();
    PlanNode root = getRoot();
    UResultOperator operator = (UResultOperator) root.run();
    long endTime1 = System.currentTimeMillis();
    LOGGER.info("InstanceResponsePlanNode.run took:" + (endTime1 - startTime));
    InstanceResponseBlock instanceResponseBlock = (InstanceResponseBlock) operator.nextBlock();
    long endTime2 = System.currentTimeMillis();
    LOGGER.info("UResultOperator took :" + (endTime2 - endTime1));
    _instanceResponseDataTable = instanceResponseBlock.getInstanceResponseDataTable();
    long endTime3 = System.currentTimeMillis();
    LOGGER.info("Converting to InstanceResponseBlock to DataTable took :" + (endTime3 - endTime2));
    long endTime = System.currentTimeMillis();
    _instanceResponseDataTable.getMetadata().put("timeUsedMs", "" + (endTime - startTime));
  }

  @Override
  public DataTable getInstanceResponse() {
    return _instanceResponseDataTable;
  }

}
