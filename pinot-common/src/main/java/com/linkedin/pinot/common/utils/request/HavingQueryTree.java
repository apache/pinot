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

package com.linkedin.pinot.common.utils.request;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.FilterOperator;
import java.util.List;


public class HavingQueryTree extends QueryTree {
  final List<HavingQueryTree> _children;
  private final AggregationInfo _aggregationInfo;

  public HavingQueryTree(AggregationInfo aggregationInfo, List<String> value, FilterOperator operator,
      List<HavingQueryTree> children) {
    this._value = value;
    this._aggregationInfo = aggregationInfo;
    this._operator = operator;
    this._children = children;
  }

  public List<HavingQueryTree> getChildren() {
    return _children;
  }

  public AggregationInfo getAggregationInfo() {
    return _aggregationInfo;
  }

  @Override
  public String toString() {
    StringBuffer stringBuffer = new StringBuffer();
    recursiveToStringIntoBuffer(0, stringBuffer);
    return stringBuffer.toString();
  }

  @Override
  protected void recursiveToStringIntoBuffer(int indent, StringBuffer stringBuffer) {
    for (int i = 0; i < indent; i++) {
      stringBuffer.append(' ');
    }
    if (_operator == FilterOperator.OR || _operator == FilterOperator.AND) {
      stringBuffer.append(_operator);
    } else {
      stringBuffer.append(_aggregationInfo.getAggregationType())
          .append("(")
          .append(_aggregationInfo.getAggregationParams().toString())
          .append(")")
          .append(" ")
          .append(_operator)
          .append(" ")
          .append(_value);
    }
    if (_children != null) {
      for (QueryTree child : _children) {
        stringBuffer.append('\n');
        child.recursiveToStringIntoBuffer(indent + 1, stringBuffer);
      }
    }
  }
}
