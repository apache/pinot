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
package org.apache.pinot.core.query.request.context;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.ArrayJoinType;
import org.apache.pinot.common.request.context.ExpressionContext;


public class ArrayJoinContext {
  private final ArrayJoinType _type;
  private final List<Operand> _operands;

  public ArrayJoinContext(ArrayJoinType type, List<Operand> operands) {
    _type = type;
    _operands = operands;
  }

  public ArrayJoinType getType() {
    return _type;
  }

  public List<Operand> getOperands() {
    return _operands;
  }

  public static class Operand {
    private final ExpressionContext _expression;
    private final @Nullable String _alias;

    public Operand(ExpressionContext expression, @Nullable String alias) {
      _expression = expression;
      _alias = alias;
    }

    public ExpressionContext getExpression() {
      return _expression;
    }

    @Nullable
    public String getAlias() {
      return _alias;
    }
  }
}
