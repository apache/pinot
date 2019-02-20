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
package org.apache.pinot.core.operator;

import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;


public class InstanceResponseOperator extends BaseOperator<InstanceResponseBlock> {
  private static final String OPERATOR_NAME = "InstanceResponseOperator";

  private final Operator _operator;

  public InstanceResponseOperator(Operator combinedOperator) {
    _operator = combinedOperator;
  }

  @Override
  protected InstanceResponseBlock getNextBlock() {
    return new InstanceResponseBlock((IntermediateResultsBlock) _operator.nextBlock());
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
