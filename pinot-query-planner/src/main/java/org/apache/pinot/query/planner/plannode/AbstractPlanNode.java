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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.serde.ProtoProperties;
import org.apache.pinot.query.planner.serde.ProtoSerializable;
import org.apache.pinot.query.planner.serde.ProtoSerializationUtils;


public abstract class AbstractPlanNode implements PlanNode, ProtoSerializable {

  protected int _planFragmentId;
  protected final List<PlanNode> _inputs;
  protected DataSchema _dataSchema;

  public AbstractPlanNode(int planFragmentId) {
    this(planFragmentId, null);
  }

  public AbstractPlanNode(int planFragmentId, DataSchema dataSchema) {
    _planFragmentId = planFragmentId;
    _dataSchema = dataSchema;
    _inputs = new ArrayList<>();
  }

  @Override
  public int getPlanFragmentId() {
    return _planFragmentId;
  }

  @Override
  public void setPlanFragmentId(int planFragmentId) {
    _planFragmentId = planFragmentId;
  }

  @Override
  public List<PlanNode> getInputs() {
    return _inputs;
  }

  @Override
  public void addInput(PlanNode planNode) {
    _inputs.add(planNode);
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  @Override
  public void setDataSchema(DataSchema dataSchema) {
    _dataSchema = dataSchema;
  }

  @Override
  public void fromObjectField(Plan.ObjectField objectField) {
    ProtoSerializationUtils.setObjectFieldToObject(this, objectField);
  }

  @Override
  public Plan.ObjectField toObjectField() {
    return ProtoSerializationUtils.convertObjectToObjectField(this);
  }

  public static class NodeHint {
    @ProtoProperties
    public Map<String, Map<String, String>> _hintOptions;
    public NodeHint() {
    }

    public NodeHint(List<RelHint> relHints) {
      _hintOptions = new HashMap<>();
      for (RelHint relHint : relHints) {
        Map<String, String> kvOptions = new HashMap<>(relHint.kvOptions);
        _hintOptions.put(relHint.hintName, kvOptions);
      }
    }
  }
}
