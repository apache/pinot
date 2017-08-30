/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.math.BigDecimal;


public class BetweenComparison extends ComparisonFunction {
  private BigDecimal _rightValue;
  private BigDecimal _leftValue;

  public BetweenComparison(String leftValue, String rightValue, AggregationInfo aggregationInfo) {
    this._leftValue = new BigDecimal(leftValue);
    this._rightValue = new BigDecimal(rightValue);

    if (!aggregationInfo.getAggregationParams().get("column").equals("*")) {
      this._functionExpression =
          aggregationInfo.getAggregationType() + "_" + aggregationInfo.getAggregationParams().get("column");
    } else {
      this._functionExpression = aggregationInfo.getAggregationType() + "_star";
    }
  }

  @Override
  public boolean isComparisonValid(String aggResult) {
    BigDecimal middleValue = new BigDecimal(aggResult);
    if ((middleValue.compareTo(_leftValue) >= 0) && (middleValue.compareTo(_rightValue) <= 0)) {
      return true;
    } else {
      return false;
    }
  }
}
