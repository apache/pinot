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

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class JoinNode extends AbstractPlanNode {
  @ProtoProperties
  private JoinRelType _joinRelType;
  @ProtoProperties
  private JoinKeys _joinKeys;
  @ProtoProperties
  private List<RexExpression> _joinClause;
  @ProtoProperties
  private List<String> _leftColumnNames;
  @ProtoProperties
  private List<String> _rightColumnNames;

  public JoinNode(int planFragmentId) {
    super(planFragmentId);
  }

  public JoinNode(int planFragmentId, DataSchema dataSchema, DataSchema leftSchema, DataSchema rightSchema,
      JoinRelType joinRelType, JoinKeys joinKeys, List<RexExpression> joinClause) {
    super(planFragmentId, dataSchema);
    _leftColumnNames = Arrays.asList(leftSchema.getColumnNames());
    _rightColumnNames = Arrays.asList(rightSchema.getColumnNames());
    _joinRelType = joinRelType;
    _joinKeys = joinKeys;
    _joinClause = joinClause;
  }

  public JoinRelType getJoinRelType() {
    return _joinRelType;
  }

  public JoinKeys getJoinKeys() {
    return _joinKeys;
  }

  public List<RexExpression> getJoinClauses() {
    return _joinClause;
  }

  public List<String> getLeftColumnNames() {
    return _leftColumnNames;
  }

  public List<String> getRightColumnNames() {
    return _rightColumnNames;
  }

  @Override
  public String explain() {
    return "JOIN";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitJoin(this, context);
  }

  public static class JoinKeys {
    @ProtoProperties
    private KeySelector<Object[], Object[]> _leftJoinKeySelector;
    @ProtoProperties
    private KeySelector<Object[], Object[]> _rightJoinKeySelector;

    public JoinKeys() {
    }

    public JoinKeys(FieldSelectionKeySelector leftKeySelector, FieldSelectionKeySelector rightKeySelector) {
      _leftJoinKeySelector = leftKeySelector;
      _rightJoinKeySelector = rightKeySelector;
    }

    public KeySelector<Object[], Object[]> getLeftJoinKeySelector() {
      return _leftJoinKeySelector;
    }

    public KeySelector<Object[], Object[]> getRightJoinKeySelector() {
      return _rightJoinKeySelector;
    }
  }
}
