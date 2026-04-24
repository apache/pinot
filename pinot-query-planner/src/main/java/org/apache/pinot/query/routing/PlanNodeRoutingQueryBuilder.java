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
package org.apache.pinot.query.routing;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.parser.CalciteRexExpressionParser;
import org.apache.pinot.query.planner.logical.LeafStageToPinotQuery;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;


/**
 * Converts an MSE leaf stage {@link PlanNode} tree to a {@link PinotQuery} for routing purposes. Used by the broker
 * pruning path in {@link WorkerManager} to build a filter-bearing routing query that enables segment pruning at the
 * broker.
 */
public class PlanNodeRoutingQueryBuilder {
  private PlanNodeRoutingQueryBuilder() {
  }

  /**
   * Converts a PlanNode leaf stage root to a {@link PinotQuery} for routing purposes. Only handles Project, Filter
   * and TableScan nodes — other node types in the chain are silently skipped. Callers should expect this method
   * to throw on malformed trees (e.g., missing TableScanNode, multi-input nodes) and handle failures gracefully.
   *
   * @param tableName the table name (with or without type suffix). Passed explicitly because PlanNode trees
   *                  don't carry the resolved table name.
   * @param leafStageRoot the root of the leaf stage
   * @param skipFilter whether to skip the filter in the query
   * @return a {@link PinotQuery} representing the leaf stage
   */
  public static PinotQuery createPinotQueryForRouting(String tableName, PlanNode leafStageRoot, boolean skipFilter) {
    List<PlanNode> bottomToTopNodes = new ArrayList<>();
    accumulateBottomToTop(leafStageRoot, bottomToTopNodes);
    Preconditions.checkState(!bottomToTopNodes.isEmpty() && bottomToTopNodes.get(0) instanceof TableScanNode,
        "Could not find table scan");
    TableScanNode tableScan = (TableScanNode) bottomToTopNodes.get(0);
    PinotQuery pinotQuery = initializePinotQueryForTableScan(tableName, tableScan);
    for (PlanNode parentNode : bottomToTopNodes) {
      if (parentNode instanceof FilterNode) {
        if (!skipFilter) {
          handleFilter((FilterNode) parentNode, pinotQuery);
        }
      } else if (parentNode instanceof ProjectNode) {
        handleProject((ProjectNode) parentNode, pinotQuery);
      }
    }
    return pinotQuery;
  }

  private static void accumulateBottomToTop(PlanNode root, List<PlanNode> parentNodes) {
    Preconditions.checkState(root.getInputs().size() <= 1,
        "Leaf stage nodes should have at most one input, found: %s", root.getInputs().size());
    for (PlanNode input : root.getInputs()) {
      accumulateBottomToTop(input, parentNodes);
    }
    parentNodes.add(root);
  }

  private static PinotQuery initializePinotQueryForTableScan(String tableName, TableScanNode tableScan) {
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setDataSource(new DataSource());
    pinotQuery.getDataSource().setTableName(tableName);
    pinotQuery.setSelectList(tableScan.getColumns().stream().map(
        RequestUtils::getIdentifierExpression).collect(Collectors.toList()));
    return pinotQuery;
  }

  private static void handleProject(ProjectNode project, PinotQuery pinotQuery) {
    List<Expression> selectList = CalciteRexExpressionParser.convertRexNodes(project.getProjects(),
        pinotQuery.getSelectList());
    pinotQuery.setSelectList(selectList);
  }

  private static void handleFilter(FilterNode filter, PinotQuery pinotQuery) {
    Expression filterExpression = CalciteRexExpressionParser.toExpression(filter.getCondition(),
        pinotQuery.getSelectList());
    pinotQuery.setFilterExpression(LeafStageToPinotQuery.ensureFilterIsFunctionExpression(filterExpression));
  }
}
