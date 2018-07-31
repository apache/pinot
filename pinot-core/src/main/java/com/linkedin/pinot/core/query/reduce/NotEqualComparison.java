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
import org.slf4j.LoggerFactory;


public class NotEqualComparison extends ComparisonFunction {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(NotEqualComparison.class);
  private double _rightValue;

  public NotEqualComparison(String rightValue, AggregationInfo aggregationInfo) {
    super(aggregationInfo);
    try {
      this._rightValue = Double.parseDouble(rightValue);
    } catch (Exception e) {
      LOGGER.info("Exception in applying HAVING clause NOT EQUAL predicate", e);
    }
  }

  @Override
  public boolean isComparisonValid(String aggResult) {
    try {
      double leftValue = Double.parseDouble(aggResult);
      if (leftValue != _rightValue) {
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      LOGGER.info("Exception in applying HAVING clause NOT EQUAL predicate", e);
      return false;
    }
  }

  public double getRightValue() {
    return _rightValue;
  }
}
