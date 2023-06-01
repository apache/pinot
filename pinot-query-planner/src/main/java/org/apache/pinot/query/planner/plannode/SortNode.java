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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class SortNode extends AbstractPlanNode {
  @ProtoProperties
  private List<RexExpression> _collationKeys;
  @ProtoProperties
  private List<Direction> _collationDirections;
  @ProtoProperties
  private List<NullDirection> _collationNullDirections;
  @ProtoProperties
  private int _fetch;
  @ProtoProperties
  private int _offset;

  public SortNode(int planFragmentId) {
    super(planFragmentId);
  }

  public SortNode(int planFragmentId, List<RelFieldCollation> fieldCollations, int fetch, int offset,
      DataSchema dataSchema) {
    super(planFragmentId, dataSchema);
    int numCollations = fieldCollations.size();
    _collationKeys = new ArrayList<>(numCollations);
    _collationDirections = new ArrayList<>(numCollations);
    _collationNullDirections = new ArrayList<>(numCollations);
    for (RelFieldCollation fieldCollation : fieldCollations) {
      _collationKeys.add(new RexExpression.InputRef(fieldCollation.getFieldIndex()));
      Direction direction = fieldCollation.getDirection();
      Preconditions.checkArgument(direction == Direction.ASCENDING || direction == Direction.DESCENDING,
          "Unsupported ORDER-BY direction: %s", direction);
      _collationDirections.add(direction);
      NullDirection nullDirection = fieldCollation.nullDirection;
      if (nullDirection == NullDirection.UNSPECIFIED) {
        nullDirection = direction == Direction.ASCENDING ? NullDirection.LAST : NullDirection.FIRST;
      }
      _collationNullDirections.add(nullDirection);
    }
    _fetch = fetch;
    _offset = offset;
  }

  public List<RexExpression> getCollationKeys() {
    return _collationKeys;
  }

  public List<Direction> getCollationDirections() {
    return _collationDirections;
  }

  public List<NullDirection> getCollationNullDirections() {
    return _collationNullDirections;
  }

  public int getFetch() {
    return _fetch;
  }

  public int getOffset() {
    return _offset;
  }

  @Override
  public String explain() {
    return String.format("SORT%s%s", (_fetch > 0) ? " LIMIT " + _fetch : "", (_offset > 0) ? " OFFSET " + _offset : "");
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitSort(this, context);
  }
}
