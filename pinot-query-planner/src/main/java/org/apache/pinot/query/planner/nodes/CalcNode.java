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

import org.apache.pinot.common.proto.Plan;


public class CalcNode extends AbstractStageNode {
  private String _expression;

  public CalcNode(int stageId) {
    super(stageId);
  }

  public CalcNode(int stageId, String expression) {
    super(stageId);
    _expression = expression;
  }

  public String getExpression() {
    return _expression;
  }

  @Override
  public void setFields(Plan.ObjectFields objFields) {
    _expression = objFields.getLiteralFieldOrThrow("expression").getStringField();
  }

  @Override
  public Plan.ObjectFields getFields() {
    return Plan.ObjectFields.newBuilder()
        .putLiteralField("expression", SerDeUtils.stringField(_expression))
        .build();
  }
}
