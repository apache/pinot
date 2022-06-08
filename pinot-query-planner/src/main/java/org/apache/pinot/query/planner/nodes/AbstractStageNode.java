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
package org.apache.pinot.query.planner.nodes;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.query.planner.nodes.serde.ProtoSerializable;
import org.apache.pinot.query.planner.nodes.serde.ProtoSerializationUtils;


public abstract class AbstractStageNode implements StageNode, ProtoSerializable {

  protected final int _stageId;
  protected final List<StageNode> _inputs;

  public AbstractStageNode(int stageId) {
    _stageId = stageId;
    _inputs = new ArrayList<>();
  }

  @Override
  public List<StageNode> getInputs() {
    return _inputs;
  }

  @Override
  public void addInput(StageNode stageNode) {
    _inputs.add(stageNode);
  }

  @Override
  public int getStageId() {
    return _stageId;
  }

  @Override
  public void setObjectField(Plan.ObjectField objectField) {
    ProtoSerializationUtils.fromObjectField(this, objectField);
  }

  @Override
  public Plan.ObjectField getObjectField() {
    return ProtoSerializationUtils.toObjectField(this);
  }
}
