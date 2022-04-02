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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelector;


public class JoinNode extends AbstractStageNode {
  private JoinRelType _joinRelType;
  private List<JoinClause> _criteria;

  public JoinNode(int stageId) {
    super(stageId);
  }

  public JoinNode(int stageId, JoinRelType joinRelType, List<JoinClause> criteria
  ) {
    super(stageId);
    _joinRelType = joinRelType;
    _criteria = criteria;
  }

  public JoinRelType getJoinRelType() {
    return _joinRelType;
  }

  public List<JoinClause> getCriteria() {
    return _criteria;
  }

  public static class JoinClause implements ProtoSerializable {
    private FieldSelectionKeySelector _leftJoinKeySelector;
    private FieldSelectionKeySelector _rightJoinKeySelector;

    public JoinClause() {
    }

    public JoinClause(FieldSelectionKeySelector leftKeySelector, FieldSelectionKeySelector rightKeySelector) {
      _leftJoinKeySelector = leftKeySelector;
      _rightJoinKeySelector = rightKeySelector;
    }

    public FieldSelectionKeySelector getLeftJoinKeySelector() {
      return _leftJoinKeySelector;
    }

    public FieldSelectionKeySelector getRightJoinKeySelector() {
      return _rightJoinKeySelector;
    }

    @Override
    public void setFields(Plan.ObjectFields objFields) {
      // Only column index based key selector is supported.
      // TODO: support generic KeySelector
      _leftJoinKeySelector = new FieldSelectionKeySelector(
          objFields.getLiteralFieldOrThrow("leftColumnIdx").getIntField());
      _rightJoinKeySelector = new FieldSelectionKeySelector(
          objFields.getLiteralFieldOrThrow("rightColumnIdx").getIntField());
    }

    @Override
    public Plan.ObjectFields getFields() {
      return Plan.ObjectFields.newBuilder()
          .putLiteralField("leftColumnIdx", SerDeUtils.intField(_leftJoinKeySelector.getColumnIndex()))
          .putLiteralField("rightColumnIdx", SerDeUtils.intField(_rightJoinKeySelector.getColumnIndex()))
          .build();
    }
  }

  @Override
  public void setFields(Plan.ObjectFields objFields) {
    _joinRelType = JoinRelType.valueOf(objFields.getLiteralFieldOrThrow("jobRelType").getStringField());
    _criteria = new ArrayList<>();
    for (Plan.ObjectFields joinClauseField : objFields.getListFieldsOrThrow("criteria").getObjectsList()) {
      JoinClause joinClause = new JoinClause();
      joinClause.setFields(joinClauseField);
      _criteria.add(joinClause);
    }
  }

  @Override
  public Plan.ObjectFields getFields() {
    Plan.ListField.Builder listBuilder = Plan.ListField.newBuilder();
    for (JoinClause joinClause : _criteria) {
      listBuilder.addObjects(joinClause.getFields());
    }
    return Plan.ObjectFields.newBuilder()
        .putLiteralField("jobRelType", SerDeUtils.stringField(_joinRelType.name()))
        .putListFields("criteria", listBuilder.build())
        .build();
  }
}
