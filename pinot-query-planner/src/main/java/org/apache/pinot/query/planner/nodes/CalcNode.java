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

import org.apache.calcite.rel.logical.LogicalCalc;


public class CalcNode extends AbstractStageNode {
  private final String _expression;

  public CalcNode(LogicalCalc node, int currentStageId) {
    super(currentStageId);
    _expression = toExpression(node);
  }

  public String getExpression() {
    return _expression;
  }

  private String toExpression(LogicalCalc node) {
    // TODO: make it real.
    return node.getDigest();
  }
}
