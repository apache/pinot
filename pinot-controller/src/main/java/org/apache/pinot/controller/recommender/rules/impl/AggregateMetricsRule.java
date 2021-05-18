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

package org.apache.pinot.controller.recommender.rules.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.HYBRID;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.REALTIME;


/**
 * This rule checks the provided queries and suggests the value for 'AggregateMetrics' flag in table config.
 * It looks at selection columns and if all of them are SUM function, the flag should be true, otherwise it's false.
 * It also checks if all column names appearing in sum function are in fact metric columns.
 */
public class AggregateMetricsRule extends AbstractRule {

  public AggregateMetricsRule(InputManager input, ConfigManager output) {
    super(input, output);
  }

  @Override
  public void run()
      throws InvalidInputException {
    String tableType = _input.getTableType();
    if ((tableType.equalsIgnoreCase(REALTIME) || tableType.equalsIgnoreCase(HYBRID))) {
      _output.setAggregateMetrics(shouldAggregate(_input));
    }
  }

  private boolean shouldAggregate(InputManager inputManager) {
    Set<String> metricNames = new HashSet<>(inputManager.getSchema().getMetricNames());
    for (String query : inputManager.getParsedQueries()) {
      for (ExpressionContext selectExpr : inputManager.getQueryContext(query).getSelectExpressions()) {
        FunctionContext funcCtx = selectExpr.getFunction();
        if (selectExpr.getType() != ExpressionContext.Type.FUNCTION
            || !funcCtx.getFunctionName().equalsIgnoreCase("SUM")
            || hasNonMetricArguments(funcCtx.getArguments(), metricNames)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean hasNonMetricArguments(List<ExpressionContext> arguments, Set<String> metricNames) {
    for (ExpressionContext arg : arguments) {
      if (arg.getType() == ExpressionContext.Type.IDENTIFIER) {
        if (!metricNames.contains(arg.getIdentifier())) {
          return true;
        }
      } else if (arg.getType() == ExpressionContext.Type.FUNCTION) {
        if (hasNonMetricArguments(arg.getFunction().getArguments(), metricNames)) {
          return true;
        }
      }
    }
    return false;
  }
}
