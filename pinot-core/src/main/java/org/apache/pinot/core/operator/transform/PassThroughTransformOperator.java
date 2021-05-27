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
package org.apache.pinot.core.operator.transform;

import java.util.Collection;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.PassThroughTransformBlock;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;


/**
 * Class for evaluating pass through transform expressions.
 */
public class PassThroughTransformOperator extends TransformOperator {
  private static final String OPERATOR_NAME = "PassThroughTransformOperator";
  /**
   * Constructor for the class
   *
   * @param projectionOperator Projection operator
   * @param expressions Collection of expressions to evaluate
   */
  public PassThroughTransformOperator(ProjectionOperator projectionOperator, Collection<ExpressionContext> expressions) {
    super(projectionOperator, expressions);
  }

  @Override
  protected PassThroughTransformBlock getNextBlock() {
    ProjectionBlock projectionBlock = _projectionOperator.nextBlock();
    if (projectionBlock == null) {
      return null;
    } else {
      return new PassThroughTransformBlock(projectionBlock, _transformFunctionMap);
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
