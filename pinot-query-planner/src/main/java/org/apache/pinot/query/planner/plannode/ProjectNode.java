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
import java.util.Objects;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class ProjectNode extends BasePlanNode {
  private final List<RexExpression> _projects;

  public ProjectNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<RexExpression> projects) {
    super(stageId, dataSchema, nodeHint, inputs);
    _projects = projects;
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

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new ProjectNode(_stageId, _dataSchema, _nodeHint, inputs, _projects);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ProjectNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ProjectNode that = (ProjectNode) o;
    return Objects.equals(_projects, that._projects);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _projects);
  }
}
