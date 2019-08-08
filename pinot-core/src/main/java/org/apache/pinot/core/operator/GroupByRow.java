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
package org.apache.pinot.core.operator;

import com.google.common.base.Joiner;
import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;


public class GroupByRow {
  private String _stringKey;
  private String[] _arrayKey;
  private Object[] _aggregationResults;

  public GroupByRow(String stringKey, Object[] aggregationResults) {
    _stringKey = stringKey;
    _arrayKey = stringKey.split("\t");
    _aggregationResults = aggregationResults;
  }

  public GroupByRow(String[] arrayKey, Object[] aggregationResults) {
    _stringKey = Joiner.on("\t").join(arrayKey);
    _arrayKey = arrayKey;
    _aggregationResults = aggregationResults;
  }

  public String[] getArrayKey() {
    return _arrayKey;
  }

  public Object[] getAggregationResults() {
    return _aggregationResults;
  }

  public String getStringKey() {
    return _stringKey;
  }

  public void merge(GroupByRow rowToMerge, AggregationFunction[] aggregationFunctions, int numAggregationFunctions) {
    Object[] resultToMerge = rowToMerge.getAggregationResults();
    for (int i = 0; i < numAggregationFunctions; i++) {
      _aggregationResults[i] = aggregationFunctions[i].merge(_aggregationResults[i], resultToMerge[i]);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    GroupByRow that = (GroupByRow) o;

    return EqualityUtils.isEqual(_stringKey, that._stringKey) && EqualityUtils.isEqual(_aggregationResults,
        that._aggregationResults);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_stringKey);
    result = EqualityUtils.hashCodeOf(result, _aggregationResults);
    return result;
  }
}
