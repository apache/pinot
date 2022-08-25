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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class SortNode extends AbstractStageNode {
  @ProtoProperties
  private List<RexExpression> _collationKeys;
  @ProtoProperties
  private List<RelFieldCollation.Direction> _collationDirections;
  @ProtoProperties
  private int _fetch;
  @ProtoProperties
  private int _offset;

  public SortNode(int stageId) {
    super(stageId);
  }

  public SortNode(int stageId, List<RelFieldCollation> fieldCollations, int fetch, int offset, DataSchema dataSchema) {
    super(stageId, dataSchema);
    _collationDirections = new ArrayList<>(fieldCollations.size());
    _collationKeys = new ArrayList<>(fieldCollations.size());
    _fetch = fetch;
    _offset = offset;
    for (RelFieldCollation fieldCollation : fieldCollations) {
      _collationDirections.add(fieldCollation.getDirection());
      _collationKeys.add(new RexExpression.InputRef(fieldCollation.getFieldIndex()));
    }
  }

  public List<RexExpression> getCollationKeys() {
    return _collationKeys;
  }

  public List<RelFieldCollation.Direction> getCollationDirections() {
    return _collationDirections;
  }

  public int getFetch() {
    return _fetch;
  }

  public int getOffset() {
    return _offset;
  }
}
