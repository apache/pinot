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
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;


public abstract class BasePlanNode implements PlanNode {
  protected int _stageId;
  protected final DataSchema _dataSchema;
  protected final NodeHint _nodeHint;
  protected final List<PlanNode> _inputs;

  public BasePlanNode(int stageId, DataSchema dataSchema, @Nullable NodeHint nodeHint, List<PlanNode> inputs) {
    _stageId = stageId;
    _dataSchema = dataSchema;
    _nodeHint = nodeHint != null ? nodeHint : new NodeHint(Map.of());
    _inputs = inputs;
  }

  @Override
  public int getStageId() {
    return _stageId;
  }

  @Override
  public void setStageId(int stageId) {
    _stageId = stageId;
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  @Override
  public NodeHint getNodeHint() {
    return _nodeHint;
  }

  @Override
  public List<PlanNode> getInputs() {
    return _inputs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BasePlanNode)) {
      return false;
    }
    BasePlanNode that = (BasePlanNode) o;
    return _stageId == that._stageId && Objects.equals(_inputs, that._inputs) && Objects.equals(
        _dataSchema, that._dataSchema) && Objects.equals(_nodeHint, that._nodeHint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_stageId, _inputs, _dataSchema, _nodeHint);
  }
}
