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


/// Converts an MSE leaf stage [PlanNode] tree to a [PinotQuery] for routing purposes. Used by the broker
/// pruning path in [WorkerManager] to build a filter-bearing routing query that enables segment pruning at the
/// broker.
///
/// This is the [PlanNode] (logical-planner) counterpart of [LeafStageToPinotQuery], which performs the
/// same leaf-stage → routing-query fold over Calcite [org.apache.calcite.rel.RelNode]s for the physical
/// optimizer. The two traversals are intentionally kept as separate builders because they walk different node
/// hierarchies; the shared filter-normalization logic (`ensureFilterIsFunctionExpression` /
/// `addFilterExpression`) lives in [LeafStageToPinotQuery]. Keep the leaf-boundary and filter-combining
/// behavior of the two in sync.
public class PlanNodeRoutingQueryBuilder {
  private PlanNodeRoutingQueryBuilder() {
  }

  /// Converts a PlanNode leaf stage root to a [PinotQuery] for routing purposes. Folds Filter and Project nodes
  /// bottom-up starting from the TableScan, stopping at the first node of any other type (a leaf boundary such as an
  /// un-split aggregate): nodes above the boundary operate on a different row space and must not contribute to the
  /// routing filter. Callers should expect this method to throw on malformed trees (e.g., missing TableScanNode,
  /// multi-input nodes) and handle failures gracefully.
  ///
  /// @param tableName the table name (with or without type suffix). Passed explicitly because PlanNode trees
  ///                  don't carry the resolved table name.
  /// @param leafStageRoot the root of the leaf stage
  /// @param skipFilter whether to skip the filter in the query
  /// @return a [PinotQuery] representing the leaf stage
  public static PinotQuery createPinotQueryForRouting(String tableName, PlanNode leafStageRoot, boolean skipFilter) {
    List<PlanNode> bottomToTopNodes = new ArrayList<>();
    accumulateBottomToTop(leafStageRoot, bottomToTopNodes);
    Preconditions.checkState(!bottomToTopNodes.isEmpty() && bottomToTopNodes.get(0) instanceof TableScanNode,
        "Could not find table scan");
    TableScanNode tableScan = (TableScanNode) bottomToTopNodes.get(0);
    PinotQuery pinotQuery = initializePinotQueryForTableScan(tableName, tableScan);
    for (int i = 1; i < bottomToTopNodes.size(); i++) {
      PlanNode parentNode = bottomToTopNodes.get(i);
      if (parentNode instanceof FilterNode) {
        if (!skipFilter) {
          handleFilter((FilterNode) parentNode, pinotQuery);
        }
      } else if (parentNode instanceof ProjectNode) {
        handleProject((ProjectNode) parentNode, pinotQuery);
      } else {
        // Leaf boundary: the first node that is neither Filter nor Project (e.g. an un-split DIRECT aggregate)
        // changes the row space -- InputRefs in nodes above it index that node's output, not the scan/project
        // columns -- so folding anything above it (e.g. a HAVING filter) would mis-resolve columns and corrupt the
        // routing filter. Everything below this boundary is a genuine row-level condition; ignore everything above.
        break;
      }
    }
    return pinotQuery;
  }

  /// Whether [#createPinotQueryForRouting] can fold the given leaf stage tree, i.e. whether it holds no multi-input
  /// node. Lets a caller skip the attempt rather than pay a thrown-and-caught exception per query, which a colocated
  /// semi-join's probe leaf -- the one holding the join -- would otherwise do on every query. It lives here so that it
  /// walks the tree the same way [#accumulateBottomToTop] does; the two disagreeing would either bring the exception
  /// back or, worse, silently refuse shapes that fold perfectly well.
  ///
  /// A `true` result is not a promise that the fold succeeds: the tree may still be missing a table scan, which only
  /// the fold itself detects.
  static boolean canBuildRoutingQuery(PlanNode leafStageRoot) {
    List<PlanNode> inputs = leafStageRoot.getInputs();
    if (inputs.size() > 1) {
      return false;
    }
    for (PlanNode input : inputs) {
      if (!canBuildRoutingQuery(input)) {
        return false;
      }
    }
    return true;
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
    LeafStageToPinotQuery.addFilterExpression(pinotQuery,
        LeafStageToPinotQuery.ensureFilterIsFunctionExpression(filterExpression));
  }
}
