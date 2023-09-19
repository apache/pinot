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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;


/**
 * Base implementation of {@link AggregationFunction} with single input expression.
 */
public abstract class BaseSingleInputAggregationFunction<I, F extends Comparable> implements AggregationFunction<I, F> {
  protected final ExpressionContext _expression;

  /**
   * Constructor for the class.
   *
   * @param expression Expression to aggregate on.
   */
  public BaseSingleInputAggregationFunction(ExpressionContext expression) {
    _expression = expression;
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expression + ")";
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return Collections.singletonList(_expression);
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(getType().getName()).append('(');
    int numArguments = getInputExpressions().size();
    if (numArguments > 0) {
      stringBuilder.append(getInputExpressions().get(0).toString());
      for (int i = 1; i < numArguments; i++) {
        stringBuilder.append(", ").append(getInputExpressions().get(i).toString());
      }
    }
    return stringBuilder.append(')').toString();
  }

  protected static ExpressionContext verifySingleArgument(List<ExpressionContext> arguments, String functionName) {
    Preconditions.checkArgument(arguments.size() == 1, "%s expects 1 argument, got: %s", functionName,
        arguments.size());
    return arguments.get(0);
  }
}
