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
package org.apache.pinot.common.utils.request;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.StringUtil;


public class FilterQueryTree {
  private final String column;
  private final TransformExpressionTree _expression;
  private final List<String> value;
  private final FilterOperator operator;
  private final List<FilterQueryTree> children;

  public FilterQueryTree(String column, List<String> value, FilterOperator operator, List<FilterQueryTree> children) {
    this.column = column;
    this.value = value;
    this.operator = operator;
    this.children = children;
    if (column != null) {
      _expression = TransformExpressionTree.compileToExpressionTree(column);
    } else {
      _expression = null;
    }
  }

  public String getColumn() {
    return column;
  }

  public TransformExpressionTree getExpression() {
    return _expression;
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
      List<String> sortedValues = new ArrayList<>(value);

      // Old style double-tab separated list
      if (sortedValues.size() == 1) {
        String firstItem = sortedValues.get(0);
        List<String> firstItemValues = Lists.newArrayList(firstItem.split("\t\t"));
        Collections.sort(firstItemValues);
        sortedValues.set(0, StringUtil.join("\t\t", firstItemValues.toArray(new String[firstItemValues.size()])));
      }

      Collections.sort(sortedValues);

      stringBuffer.append(column).append(" ").append(operator).append(" ").append(sortedValues);
    }

    if (children != null) {
      for (FilterQueryTree child : children) {
        stringBuffer.append('\n');
        child.recursiveToStringIntoBuffer(indent + 1, stringBuffer);
      }
    }
  }
}
