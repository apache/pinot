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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;


/**
 * The {@link PlanNode} version for the {@link org.apache.pinot.core.plan.PinotExplainedRelNode}.
 *
 * Remember that {@link PlanNode} are just the serializable and deserializable version of a
 * {@link org.apache.calcite.rel.RelNode}.
 */
public class ExplainedNode extends BasePlanNode {

  private final String _title;
  private final Map<String, Plan.ExplainNode.AttributeValue> _attributes;

  public ExplainedNode(int stageId, DataSchema dataSchema, @Nullable NodeHint nodeHint, PlanNode input,
      String title, Map<String, Plan.ExplainNode.AttributeValue> attributes) {
    this(stageId, dataSchema, nodeHint, Collections.singletonList(input), title, attributes);
  }

  public ExplainedNode(int stageId, DataSchema dataSchema, @Nullable NodeHint nodeHint, List<PlanNode> inputs,
      String title, Map<String, Plan.ExplainNode.AttributeValue> attributes) {
    super(stageId, dataSchema, nodeHint, inputs);
    _title = title;
    _attributes = attributes;
  }

  public String getTitle() {
    return _title;
  }

  public Map<String, Plan.ExplainNode.AttributeValue> getAttributes() {
    return _attributes;
  }

  @Override
  public String explain() {
    return _title;
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitExplained(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new ExplainedNode(_stageId, _dataSchema, _nodeHint, inputs, _title, _attributes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExplainedNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ExplainedNode that = (ExplainedNode) o;
    return Objects.equals(_title, that._title) && Objects.equals(_attributes, that._attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _title, _attributes);
  }
}
