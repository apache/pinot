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
package org.apache.pinot.core.plan;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.pql.parsers.pql2.ast.IdentifierAstNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>TransformPlanNode</code> class provides the execution plan for transforms on a single segment.
 */
public class TransformPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformPlanNode.class);

  private final String _segmentName;
  private final ProjectionPlanNode _projectionPlanNode;
  private final Set<String> _projectionColumns = new HashSet<>();
  private final Set<TransformExpressionTree> _expressions = new HashSet<>();
  private int _maxDocPerNextCall = DocIdSetPlanNode.MAX_DOC_PER_CALL;

  public TransformPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    _segmentName = indexSegment.getSegmentName();
    extractColumnsAndTransforms(brokerRequest, indexSegment);
    _projectionPlanNode = new ProjectionPlanNode(indexSegment, _projectionColumns,
        new DocIdSetPlanNode(indexSegment, brokerRequest, _maxDocPerNextCall));
  }

  /**
   * Helper method to extract projection columns and transform expressions from the given broker request.
   */
  private void extractColumnsAndTransforms(BrokerRequest brokerRequest, IndexSegment indexSegment) {
    Set<String> columns = new HashSet<>();
    if (brokerRequest.isSetAggregationsInfo()) {
      // Extract aggregation expressions
      for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
        if (!aggregationInfo.getAggregationType().equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
          columns.add(AggregationFunctionUtils.getColumn(aggregationInfo));
        }
      }
      // Extract group-by expressions
      if (brokerRequest.isSetGroupBy()) {
        columns.addAll(brokerRequest.getGroupBy().getExpressions());
      }
    } else {
      Selection selection = brokerRequest.getSelections();

      // Extract selection expressions
      List<String> selectionColumns = selection.getSelectionColumns();
      if (selectionColumns.size() == 1 && selectionColumns.get(0).equals("*")) {
        for (String column : indexSegment.getPhysicalColumnNames()) {
          _projectionColumns.add(column);
          _expressions.add(new TransformExpressionTree(new IdentifierAstNode(column)));
        }
      } else {
        columns.addAll(selectionColumns);
      }

      // Extract order-by expressions and update maxDocPerNextCall
      if (selection.getSize() > 0) {
        List<SelectionSort> sortSequence = selection.getSelectionSortSequence();
        if (sortSequence == null) {
          // For selection only queries, select minimum number of documents
          _maxDocPerNextCall = Math.min(selection.getSize(), _maxDocPerNextCall);
        } else {
          for (SelectionSort selectionSort : sortSequence) {
            String orderByColumn = selectionSort.getColumn();
            if (!_projectionColumns.contains(orderByColumn)) {
              columns.add(orderByColumn);
            }
          }
        }
      } else {
        // For LIMIT 0 queries, fetch at least 1 document per DocIdSetPlanNode's requirement
        // TODO: Skip the filtering phase and document fetching for LIMIT 0 case
        _maxDocPerNextCall = 1;
      }
    }
    for (String column : columns) {
      TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree(column);
      expression.getColumns(_projectionColumns);
      _expressions.add(expression);
    }
  }

  @Override
  public TransformOperator run() {
    return new TransformOperator(_projectionPlanNode.run(), _expressions);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Segment Level Inner-Segment Plan Node:");
    LOGGER.debug(prefix + "Operator: TransformOperator");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _segmentName);
    LOGGER.debug(prefix + "Argument 1: Projection -");
    _projectionPlanNode.showTree(prefix + "    ");
  }
}
