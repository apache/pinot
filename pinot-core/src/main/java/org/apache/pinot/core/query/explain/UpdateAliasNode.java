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

import java.util.List;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * UpdateAliasNode for the output of EXPLAIN PLAN queries
 */
public class UpdateAliasNode implements ExplainPlanTreeNode {

  private static final String _NAME = "UPDATE_ALIAS";
  private List<String> _originalCols;
  private List<String> _aliasCols;
  private ExplainPlanTreeNode[] _childNodes = new ExplainPlanTreeNode[1];

  public UpdateAliasNode(QueryContext queryContext, List<String> originalCols, List<String> aliasCols,
      TableConfig tableConfig) {
    assert (queryContext.getAliasList() != null && !queryContext.getAliasList().isEmpty() && !originalCols.isEmpty()
        && !aliasCols.isEmpty());
    _originalCols = originalCols;
    _aliasCols = aliasCols;
    _childNodes[0] = new BrokerReduceNode(queryContext, tableConfig);
  }

  @Override
  public ExplainPlanTreeNode[] getChildNodes() {
    return _childNodes;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_NAME).append('(');
    stringBuilder.append(_originalCols.get(0)).append("->").append(_aliasCols.get(0));
    for (int i = 1; i < _originalCols.size(); i++) {
      String original = _originalCols.get(i);
      String alias = _aliasCols.get(i);
      stringBuilder.append(", ").append(original).append("->").append(alias);
    }
    return stringBuilder.append(')').toString();
  }
}
