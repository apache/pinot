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

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.request.transform.TransformExpressionTree;


public class RequestInfo {
  // Pre-computed segment independent information
  private final Set<String> _allColumns;
  private final Set<String> _physicalColumns;
  private final FilterQueryTree _filterQueryTree;
  private final Set<String> _filterColumns;
  private final Set<TransformExpressionTree> _aggregationExpressions;
  private final Set<String> _aggregationColumns;
  private final Set<TransformExpressionTree> _groupByExpressions;
  private final Set<String> _groupByColumns;
  private final Set<String> _selectionColumns;

  public RequestInfo(Set<String> allColumns, FilterQueryTree filterQueryTree, Set<String> filterColumns,
      Set<TransformExpressionTree> aggregationExpressions, Set<String> aggregationColumns,
      Set<TransformExpressionTree> groupByExpressions, Set<String> groupByColumns, Set<String> selectionColumns) {
    _allColumns = allColumns;
    _physicalColumns = new HashSet<>(_allColumns);
    // filters out virtual columns in the query.
    _physicalColumns.removeIf(column -> column.startsWith("$"));
    _filterQueryTree = filterQueryTree;
    _filterColumns = filterColumns;
    _aggregationExpressions = aggregationExpressions;
    _aggregationColumns = aggregationColumns;
    _groupByExpressions = groupByExpressions;
    _groupByColumns = groupByColumns;
    _selectionColumns = selectionColumns;
  }

  public Set<String> getAllColumns() {
    return _allColumns;
  }

  public Set<String> getPhysicalColumns() {
    return _physicalColumns;
  }

  public FilterQueryTree getFilterQueryTree() {
    return _filterQueryTree;
  }

  public Set<String> getFilterColumns() {
    return _filterColumns;
  }

  public Set<TransformExpressionTree> getAggregationExpressions() {
    return _aggregationExpressions;
  }

  public Set<String> getAggregationColumns() {
    return _aggregationColumns;
  }

  public Set<TransformExpressionTree> getGroupByExpressions() {
    return _groupByExpressions;
  }

  public Set<String> getGroupByColumns() {
    return _groupByColumns;
  }

  public Set<String> getSelectionColumns() {
    return _selectionColumns;
  }
}
