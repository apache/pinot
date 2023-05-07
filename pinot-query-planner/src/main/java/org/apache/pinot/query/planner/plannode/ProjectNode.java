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
package org.apache.pinot.query.planner.plannode;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class ProjectNode extends AbstractPlanNode {
  @ProtoProperties
  private List<RexExpression> _projects;

  public ProjectNode(int planFragmentId) {
    super(planFragmentId);
  }
  public ProjectNode(int currentStageId, DataSchema dataSchema, List<RexNode> projects) {
    super(currentStageId, dataSchema);
    _projects = projects.stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
  }

  public List<RexExpression> getProjects() {
    return _projects;
  }

  @Override
  public String explain() {
    return "PROJECT";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitProject(this, context);
  }
}
