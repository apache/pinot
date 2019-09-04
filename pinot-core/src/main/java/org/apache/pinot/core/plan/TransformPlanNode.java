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

import java.util.ArrayList;
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
  private final List<TransformExpressionTree> _expressions = new ArrayList<>();
  private final List<SelectionSort> _sortSequence = new ArrayList<>();
  private int _maxDocPerNextCall = DocIdSetPlanNode.MAX_DOC_PER_CALL;

  public TransformPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    _segmentName = indexSegment.getSegmentName();
    extractColumnsAndTransforms(indexSegment, brokerRequest);
    _projectionPlanNode = new ProjectionPlanNode(indexSegment, _projectionColumns,
        new DocIdSetPlanNode(indexSegment, brokerRequest, _maxDocPerNextCall));
  }

  /**
   * Helper method to extract projection columns and transform expressions from the given broker request.
   */
  private void extractColumnsAndTransforms(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    if (brokerRequest.isSetAggregationsInfo()) {
      // Aggregation queries

      for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
        if (!aggregationInfo.getAggregationType().equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
          String expression = AggregationFunctionUtils.getColumn(aggregationInfo);
          TransformExpressionTree expressionTree = TransformExpressionTree.compileToExpressionTree(expression);
          if (!_expressions.contains(expressionTree)) {
            expressionTree.getColumns(_projectionColumns);
            _expressions.add(expressionTree);
          }
        }
      }

      // Process all group-by expressions
      if (brokerRequest.isSetGroupBy()) {
        for (String expression : brokerRequest.getGroupBy().getExpressions()) {
          TransformExpressionTree expressionTree = TransformExpressionTree.compileToExpressionTree(expression);
          if (!_expressions.contains(expressionTree)) {
            expressionTree.getColumns(_projectionColumns);
            _expressions.add(expressionTree);
          }
        }
      }
    } else {
      // Selection queries

      // Deduplicate sort expressions, and put them in front of other expressions
      Selection selection = brokerRequest.getSelections();
      List<SelectionSort> sortSequence = selection.getSelectionSortSequence();
      if (sortSequence == null || sortSequence.isEmpty()) {
        // No ordering required, select minimum number of documents
        _maxDocPerNextCall = Math.min(selection.getSize(), _maxDocPerNextCall);
      } else {
        for (SelectionSort selectionSort : sortSequence) {
          String expression = selectionSort.getColumn();
          TransformExpressionTree expressionTree = TransformExpressionTree.compileToExpressionTree(expression);
          if (!_expressions.contains(expressionTree)) {
            expressionTree.getColumns(_projectionColumns);
            _expressions.add(expressionTree);
            _sortSequence.add(selectionSort);
          }
        }
      }

      List<String> selectionColumns = selection.getSelectionColumns();
      if (selectionColumns.size() == 1 && selectionColumns.get(0).equals("*")) {
        selectionColumns = new ArrayList<>(indexSegment.getPhysicalColumnNames());
        selectionColumns.sort(null);
      }
      for (String expression : selectionColumns) {
        TransformExpressionTree expressionTree = TransformExpressionTree.compileToExpressionTree(expression);
        if (!_expressions.contains(expressionTree)) {
          expressionTree.getColumns(_projectionColumns);
          _expressions.add(expressionTree);
        }
      }
    }
  }

  public List<SelectionSort> getSortSequence() {
    return _sortSequence;
  }

  @Override
  public TransformOperator run() {
    return new TransformOperator(_projectionPlanNode.run(), new ArrayList<>(_expressions));
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
