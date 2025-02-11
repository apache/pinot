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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;


public class SortNode extends BasePlanNode {
  private final List<RelFieldCollation> _collations;
  private final int _fetch;
  private final int _offset;

  public SortNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<RelFieldCollation> collations, int fetch, int offset) {
    super(stageId, dataSchema, nodeHint, inputs);
    _collations = collations;
    _fetch = fetch;
    _offset = offset;
  }

  public List<RelFieldCollation> getCollations() {
    return _collations;
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

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new SortNode(_stageId, _dataSchema, _nodeHint, inputs, _collations, _fetch, _offset);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SortNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SortNode sortNode = (SortNode) o;
    return _fetch == sortNode._fetch && _offset == sortNode._offset && Objects.equals(_collations,
        sortNode._collations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _collations, _fetch, _offset);
  }
}
