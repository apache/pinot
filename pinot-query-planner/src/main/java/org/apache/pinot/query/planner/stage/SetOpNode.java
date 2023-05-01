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

import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.serde.ProtoProperties;


/**
 * Set operation node is used to represent UNION, INTERSECT, EXCEPT.
 */
public class SetOpNode extends AbstractStageNode {

  @ProtoProperties
  private SetOpType _setOpType;

  @ProtoProperties
  private boolean _all;

  public SetOpNode(int stageId) {
    super(stageId);
  }

  public SetOpNode(SetOpType setOpType, int stageId, DataSchema dataSchema, boolean all) {
    super(stageId, dataSchema);
    _setOpType = setOpType;
    _all = all;
  }

  public SetOpType getSetOpType() {
    return _setOpType;
  }

  public boolean isAll() {
    return _all;
  }

  @Override
  public String explain() {
    return _setOpType.toString();
  }

  @Override
  public <T, C> T visit(StageNodeVisitor<T, C> visitor, C context) {
    return visitor.visitSetOp(this, context);
  }

  public enum SetOpType {
    UNION, INTERSECT, MINUS;

    public static SetOpType fromObject(SetOp setOp) {
      if (setOp instanceof LogicalUnion) {
        return UNION;
      }
      if (setOp instanceof LogicalIntersect) {
        return INTERSECT;
      }
      if (setOp instanceof LogicalMinus) {
        return MINUS;
      }
      throw new IllegalArgumentException("Unsupported set operation: " + setOp.getClass());
    }
  }
}
