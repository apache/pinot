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
package org.apache.pinot.query.runtime.operator.operands;

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public abstract class TransformOperand {
  protected String _resultName;
  protected DataSchema.ColumnDataType _resultType;

  public static TransformOperand toTransformOperand(RexExpression rexExpression, DataSchema dataSchema) {
    if (rexExpression instanceof RexExpression.InputRef) {
      return new ReferenceOperand((RexExpression.InputRef) rexExpression, dataSchema);
    } else if (rexExpression instanceof RexExpression.FunctionCall) {
      return new FunctionOperand((RexExpression.FunctionCall) rexExpression, dataSchema);
    } else if (rexExpression instanceof RexExpression.Literal) {
      return new LiteralOperand((RexExpression.Literal) rexExpression);
    } else {
      throw new UnsupportedOperationException("Unsupported RexExpression: " + rexExpression);
    }
  }

  public String getResultName() {
    return _resultName;
  }

  public DataSchema.ColumnDataType getResultType() {
    return _resultType;
  }

  public abstract Object apply(Object[] row);
}
