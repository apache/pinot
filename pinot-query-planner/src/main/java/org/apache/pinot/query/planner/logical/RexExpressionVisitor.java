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
package org.apache.pinot.query.planner.logical;

public interface RexExpressionVisitor<R, A> {
  R visit(RexExpression.FunctionCall call, A arg);

  R visit(RexExpression.InputRef inputRef, A arg);

  R visit(RexExpression.Literal literal, A arg);

  abstract class Walker<A> implements RexExpressionVisitor<Void, A> {
    @Override
    public Void visit(RexExpression.FunctionCall call, A arg) {
      for (RexExpression operand : call.getFunctionOperands()) {
        operand.accept(this, arg);
      }
      return null;
    }

    @Override
    public Void visit(RexExpression.InputRef inputRef, A arg) {
      return null;
    }

    @Override
    public Void visit(RexExpression.Literal literal, A arg) {
      return null;
    }
  }
}
