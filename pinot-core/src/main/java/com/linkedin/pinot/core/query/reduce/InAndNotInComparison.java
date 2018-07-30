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


public class InAndNotInComparison extends ComparisonFunction {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(InAndNotInComparison.class);
  private double[] _values;
  private boolean _isItNotIn;

  public InAndNotInComparison(String values, boolean isItNotIn, AggregationInfo aggregationInfo) {
    super(aggregationInfo);
    String[] splitedValues = values.split("\\t\\t");
    int size = splitedValues.length;
    this._values = new double[size];
    for (int i = 0; i < size; i++) {
      try {
        this._values[i] = Double.parseDouble(splitedValues[i]);
      } catch (Exception e) {
        LOGGER.info("Exception in creating HAVING clause IN/NOT-IN predicate", e);
      }
    }
  }

  @Override
  public boolean isComparisonValid(String aggResult) {
    try {
      double baseValue = Double.parseDouble(aggResult);
      int size = _values.length;
      int i;
      for (i = 0; i < size; i++) {
        if (baseValue == _values[i]) {
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
    } catch (Exception e) {
      LOGGER.info("Exception in applying HAVING clause IN/NOT-IN predicate", e);
      return false;
    }
  }

  public double[] getValues() {
    return _values;
  }
}
