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
package org.apache.pinot.query.planner.logical;

import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.BasePlanNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;


/**
 * TODO: A placeholder class for literal values coming after SubPlan execution.
 * Expected to have drastic change in the future.
 */
public class LiteralValueNode extends BasePlanNode {

  private DataTable _dataTable;

  public LiteralValueNode(DataSchema dataSchema) {
    super(-1, dataSchema, null, List.of());
  }

  public void setDataTable(DataTable dataTable) {
    _dataTable = dataTable;
  }

  public DataTable getDataTable() {
    return _dataTable;
  }

  @Override
  public String explain() {
    return "LITERAL_VALUE";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    throw new UnsupportedOperationException("LiteralValueNode visit is not supported yet");
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    if (!inputs.isEmpty()) {
      throw new IllegalArgumentException("LiteralValueNode should not have any inputs");
    }
    return this;
  }
}
