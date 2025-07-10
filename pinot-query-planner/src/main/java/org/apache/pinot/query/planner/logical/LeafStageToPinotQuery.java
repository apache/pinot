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
package org.apache.pinot.query.planner.logical;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.parser.CalciteRexExpressionParser;


/**
 * Utility to convert a leaf stage to a {@link PinotQuery}.
 */
public class LeafStageToPinotQuery {
  private LeafStageToPinotQuery() {
  }

  /**
   * Converts a leaf stage root to a {@link PinotQuery}. This method only handles Project, Filter and TableScan nodes.
   * Other node types are ignored since they don't impact routing.
   *
   * @param tableName the name of the table. Needs to be provided separately since it needs TableCache.
   * @param leafStageRoot the root of the leaf stage
   * @param skipFilter whether to skip the filter in the query
   * @return a {@link PinotQuery} representing the leaf stage
   */
  public static PinotQuery createPinotQueryForRouting(String tableName, RelNode leafStageRoot, boolean skipFilter) {
    List<RelNode> bottomToTopNodes = new ArrayList<>();
    accumulateBottomToTop(leafStageRoot, bottomToTopNodes);
    Preconditions.checkState(!bottomToTopNodes.isEmpty() && bottomToTopNodes.get(0) instanceof TableScan,
        "Could not find table scan");
    TableScan tableScan = (TableScan) bottomToTopNodes.get(0);
    PinotQuery pinotQuery = initializePinotQueryForTableScan(tableName, tableScan);
    for (RelNode parentNode : bottomToTopNodes) {
      if (parentNode instanceof Filter) {
        if (!skipFilter) {
          handleFilter((Filter) parentNode, pinotQuery);
        }
      } else if (parentNode instanceof Project) {
        handleProject((Project) parentNode, pinotQuery);
      }
    }
    return pinotQuery;
  }

  private static void accumulateBottomToTop(RelNode root, List<RelNode> parentNodes) {
    Preconditions.checkState(root.getInputs().size() <= 1,
        "Leaf stage nodes should have at most one input, found: %s", root.getInputs().size());
    for (RelNode input : root.getInputs()) {
      accumulateBottomToTop(input, parentNodes);
    }
    parentNodes.add(root);
  }

  private static PinotQuery initializePinotQueryForTableScan(String tableName, TableScan tableScan) {
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setDataSource(new DataSource());
    pinotQuery.getDataSource().setTableName(tableName);
    pinotQuery.setSelectList(tableScan.getRowType().getFieldNames().stream().map(
        RequestUtils::getIdentifierExpression).collect(Collectors.toList()));
    return pinotQuery;
  }

  private static void handleProject(Project project, PinotQuery pinotQuery) {
    if (project != null) {
      List<RexExpression> rexExpressions = RexExpressionUtils.fromRexNodes(project.getProjects());
      List<Expression> selectList = CalciteRexExpressionParser.convertRexNodes(rexExpressions,
          pinotQuery.getSelectList());
      pinotQuery.setSelectList(selectList);
    }
  }

  private static void handleFilter(Filter filter, PinotQuery pinotQuery) {
    if (filter != null) {
      RexExpression rexExpression = RexExpressionUtils.fromRexNode(filter.getCondition());
      pinotQuery.setFilterExpression(CalciteRexExpressionParser.toExpression(rexExpression,
          pinotQuery.getSelectList()));
    }
  }
}
