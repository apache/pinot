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

import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.core.query.request.context.ExpressionContext;

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.HYBRID;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.REALTIME;


/**
 * This rule checks the provided queries and suggest the value for 'AggregateMetrics' flag in table config.
 * It looks at selection columns and if all of them are SUM function, the flag should be true, otherwise it's false.
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
    for (String query : inputManager.getParsedQueries()) {
      for (ExpressionContext selectExpr : inputManager.getQueryContext(query).getSelectExpressions()) {
        if (selectExpr.getType() != ExpressionContext.Type.FUNCTION
            || !selectExpr.getFunction().getFunctionName().equalsIgnoreCase("SUM")) {
          return false;
        }
      }
    }
    return true;
  }
}
