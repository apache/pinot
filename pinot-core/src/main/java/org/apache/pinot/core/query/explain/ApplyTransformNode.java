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

package org.apache.pinot.core.query.explain;

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * ApplyTransformNode for the output of EXPLAIN PLAN queries
 */
public class ApplyTransformNode implements ExplainPlanTreeNode {

  private String _name = "APPLY_TRANSFORM";
  private ExplainPlanTreeNode[] _childNodes; // can be scans or project
  private Set<String> _transformFunctions;

  public ApplyTransformNode(QueryContext queryContext, Set<String> transforms, TableConfig tableConfig) {
    assert (transforms != null && !transforms.isEmpty());
    _transformFunctions = transforms;
    _childNodes = new ProjectNode[1];
    _childNodes[0] = new ProjectNode(queryContext, tableConfig);
  }

  public ApplyTransformNode(QueryContext queryContext, Set<String> transforms, FilterContext filter,
      TableConfig tableConfig) {
    assert (transforms != null && !transforms.isEmpty());
    _transformFunctions = transforms;
    Set<String> colsInFilter = new HashSet<>();
    filter.getColumns(colsInFilter);
    _childNodes = new ScanNode[colsInFilter.size()];
    int i = 0;
    for (String col : colsInFilter) {
      // full scans for all columns in the filter
      _childNodes[i] = new ScanNode(queryContext, col, tableConfig.getTableName(), "FULL_SCAN");
      i++;
    }
  }

  @Override
  public ExplainPlanTreeNode[] getChildNodes() {
    return _childNodes;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_name).append("(transformFuncs:");
    int count = 0;
    for (String func : _transformFunctions) {
      if (count == _transformFunctions.size() - 1) {
        stringBuilder.append(func);
      } else {
        stringBuilder.append(func).append(", ");
      }
      count++;
    }
    return stringBuilder.append(')').toString();
  }
}
