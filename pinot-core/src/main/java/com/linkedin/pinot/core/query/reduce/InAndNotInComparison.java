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


public class InAndNotInComparison extends ComparisonFunction {
  private BigDecimal[] _values;
  private boolean _isItNotIn;

  public InAndNotInComparison(String values, boolean isItNotIn, AggregationInfo aggregationInfo) {

    String[] splitedValues = values.split("\\t\\t");
    int size = splitedValues.length;
    this._values = new BigDecimal[size];
    for (int i = 0; i < size; i++) {
      this._values[i] = new BigDecimal(splitedValues[i]);
    }
    this._isItNotIn = isItNotIn;
    if (!aggregationInfo.getAggregationParams().get("column").equals("*")) {
      this._functionExpression =
          aggregationInfo.getAggregationType() + "_" + aggregationInfo.getAggregationParams().get("column");
    } else {
      this._functionExpression = aggregationInfo.getAggregationType() + "_star";
    }
  }

  @Override
  public boolean isComparisonValid(String aggResult) {
    BigDecimal baseValue = new BigDecimal(aggResult);
    int size = _values.length;

    int i;
    for (i = 0; i < size; i++) {
      if (baseValue.compareTo(_values[i]) == 0) {
        break;
      }
    }

    if (!_isItNotIn) {
      if (i < size) {
        return true;
      } else {
        return false;
      }
    } else {
      if (i < size) {
        return false;
      } else {
        return true;
      }
    }
  }
}
