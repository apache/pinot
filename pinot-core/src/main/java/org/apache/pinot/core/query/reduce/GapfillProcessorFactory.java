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
package org.apache.pinot.core.query.reduce;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GapfillUtils;


/**
 * Factory class to construct the right ScalableGapfillProcessor based on the query context.
 */
@SuppressWarnings("rawtypes")
public final class GapfillProcessorFactory {
  private GapfillProcessorFactory() {
  }

  /**
   * Constructs the right result reducer based on the given query context.
   */
  public static BaseGapfillProcessor getGapfillProcessor(
      QueryContext queryContext, GapfillUtils.GapfillType gapfillType) {
    if (gapfillType == GapfillUtils.GapfillType.AGGREGATE_GAP_FILL_AGGREGATE) {
      if (queryContext.getSelectExpressions().size() == 2
          && queryContext.getSelectExpressions().get(0).getType() == ExpressionContext.Type.IDENTIFIER
          && queryContext.getSelectExpressions().get(1).getType() == ExpressionContext.Type.FUNCTION
          && queryContext.getSelectExpressions().get(1).getFunction().getFunctionName().equals("count")) {
        return new CountGapfillProcessor(queryContext, gapfillType);
      }

      int sumOrAvg = 0;
      int groupBy = 0;
      for (ExpressionContext expressionContext : queryContext.getSelectExpressions()) {
        if (expressionContext.getType() == ExpressionContext.Type.FUNCTION
            && (expressionContext.getFunction().getFunctionName().equals("sum")
            || expressionContext.getFunction().getFunctionName().equals("avg"))) {
          sumOrAvg++;
        } else if (expressionContext.getType() == ExpressionContext.Type.IDENTIFIER) {
          groupBy++;
        }
      }
      if (groupBy == 1 && sumOrAvg + groupBy == queryContext.getSelectExpressions().size()) {
        return new SumAvgGapfillProcessor(queryContext, gapfillType);
      }
    }

    return new GapfillProcessor(queryContext, gapfillType);
  }
}
