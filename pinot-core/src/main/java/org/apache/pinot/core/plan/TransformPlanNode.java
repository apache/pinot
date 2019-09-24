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
    if (brokerRequest.isSetAggregationsInfo()) {
      for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
        if (!aggregationInfo.getAggregationType().equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
          String expression = AggregationFunctionUtils.getColumn(aggregationInfo);
          TransformExpressionTree transformExpressionTree = TransformExpressionTree.compileToExpressionTree(expression);
          transformExpressionTree.getColumns(_projectionColumns);
          _expressions.add(transformExpressionTree);
        }
      }

      // Process all group-by expressions
      if (brokerRequest.isSetGroupBy()) {
        for (String expression : brokerRequest.getGroupBy().getExpressions()) {
          TransformExpressionTree transformExpressionTree = TransformExpressionTree.compileToExpressionTree(expression);
          transformExpressionTree.getColumns(_projectionColumns);
          _expressions.add(transformExpressionTree);
        }
      }
    } else {
      Selection selection = brokerRequest.getSelections();
      List<String> columns = selection.getSelectionColumns();
      if (columns.size() == 1 && columns.get(0).equals("*")) {
        columns = new ArrayList<>(indexSegment.getPhysicalColumnNames());
      }
      List<SelectionSort> sortSequence = selection.getSelectionSortSequence();
      if (sortSequence == null) {
        // For selection only queries, select minimum number of documents. Fetch at least 1 document per
        // DocIdSetPlanNode's requirement.
        // TODO: Skip the filtering phase and document fetching for LIMIT 0 case
        _maxDocPerNextCall = Math.max(Math.min(selection.getSize(), _maxDocPerNextCall), 1);
      } else {
        for (SelectionSort selectionSort : sortSequence) {
          columns.add(selectionSort.getColumn());
        }
      }
      for (String column : columns) {
        TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree(column);
        expression.getColumns(_projectionColumns);
        _expressions.add(expression);
      }
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
