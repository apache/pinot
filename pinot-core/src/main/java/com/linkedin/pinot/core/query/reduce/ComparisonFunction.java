/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.reduce;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;


//This class will be inherited by different classes that compare (e.g., for equality) the input value by the base value
public abstract class ComparisonFunction {
  private final String _functionExpression;

  protected ComparisonFunction(AggregationInfo aggregationInfo) {
    _functionExpression =
        AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfo).getAggregationColumnName();
  }

  public abstract boolean isComparisonValid(String aggResult);

  public String getFunctionExpression() {
    return _functionExpression;
  }
}
