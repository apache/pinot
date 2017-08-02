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

import com.linkedin.pinot.common.request.FilterOperator;
import java.util.List;


public class FilterQueryTree {
  private final String column;
  private final List<String> value;
  private final FilterOperator operator;
  private final List<FilterQueryTree> children;

  public FilterQueryTree(String column, List<String> value, FilterOperator operator, List<FilterQueryTree> children) {
    this.column = column;
    this.value = value;
    this.operator = operator;
    this.children = children;
  }

  public String getColumn() {
    return column;
  }

  public List<String> getValue() {
    return value;
  }

  public FilterOperator getOperator() {
    return operator;
  }

  public List<FilterQueryTree> getChildren() {
    return children;
  }

  public String toString() {
    StringBuffer stringBuffer = new StringBuffer();
    recursiveToStringIntoBuffer(0, stringBuffer);
    return stringBuffer.toString();
  }

  private void recursiveToStringIntoBuffer(int indent, StringBuffer stringBuffer) {
    for (int i = 0; i < indent; i++) {
      stringBuffer.append(' ');
    }
    if (operator == FilterOperator.OR || operator == FilterOperator.AND) {
      stringBuffer.append(operator);
    } else {
      stringBuffer.append(column).append(" ").append(operator).append(" ").append(value);
    }
    if (children != null) {
      for (FilterQueryTree child : children) {
        stringBuffer.append('\n');
        child.recursiveToStringIntoBuffer(indent + 1, stringBuffer);
      }
    }
  }
}
