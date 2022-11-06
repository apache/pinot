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
package org.apache.pinot.query.planner.stage;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class AggregateNode extends AbstractStageNode {
  @ProtoProperties
  private List<RexExpression> _aggCalls;
  @ProtoProperties
  private List<RexExpression> _groupSet;

  public AggregateNode(int stageId) {
    super(stageId);
  }

  public AggregateNode(int stageId, DataSchema dataSchema, List<AggregateCall> aggCalls, ImmutableBitSet groupSet) {
    super(stageId, dataSchema);
    _aggCalls = aggCalls.stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
    _groupSet = new ArrayList<>(groupSet.cardinality());
    for (Integer integer : groupSet) {
      _groupSet.add(new RexExpression.InputRef(integer));
    }
  }

  public List<RexExpression> getAggCalls() {
    return _aggCalls;
  }

  public List<RexExpression> getGroupSet() {
    return _groupSet;
  }

  @Override
  public String explain() {
    return "AGGREGATE";
  }

  @Override
  public <T, C> T visit(StageNodeVisitor<T, C> visitor, C context) {
    return visitor.visitAggregate(this, context);
  }
}
