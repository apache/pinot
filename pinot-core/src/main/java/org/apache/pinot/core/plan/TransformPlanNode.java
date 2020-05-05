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
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>TransformPlanNode</code> class provides the execution plan for transforms on a single segment.
 */
public class TransformPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformPlanNode.class);

  private final String _segmentName;
  private final ProjectionPlanNode _projectionPlanNode;
  private final Set<TransformExpressionTree> _expressions;
  private int _maxDocPerNextCall = DocIdSetPlanNode.MAX_DOC_PER_CALL;

  public TransformPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest,
      Set<TransformExpressionTree> expressionsToPlan) {
    _segmentName = indexSegment.getSegmentName();

    setMaxDocsForSelection(brokerRequest);
    Set<String> projectionColumns = new HashSet<>();
    extractProjectionColumns(expressionsToPlan, projectionColumns);

    _expressions = expressionsToPlan;
    _projectionPlanNode = new ProjectionPlanNode(indexSegment, projectionColumns,
        new DocIdSetPlanNode(indexSegment, brokerRequest, _maxDocPerNextCall));
  }

  private void extractProjectionColumns(Set<TransformExpressionTree> expressionsToPlan, Set<String> projectionColumns) {
    for (TransformExpressionTree expression : expressionsToPlan) {
      extractProjectionColumns(expression, projectionColumns);
    }
  }

  private void extractProjectionColumns(TransformExpressionTree expression, Set<String> projectionColumns) {
    TransformExpressionTree.ExpressionType expressionType = expression.getExpressionType();
    switch (expressionType) {
      case FUNCTION:
        for (TransformExpressionTree child : expression.getChildren()) {
          extractProjectionColumns(child, projectionColumns);
        }
        break;

      case IDENTIFIER:
        projectionColumns.add(expression.getValue());
        break;

      case LITERAL:
        // Do nothing.
        break;

      default:
        throw new UnsupportedOperationException("Unsupported expression type: " + expressionType);
    }
  }

  /**
   * Helper method to set the max number of docs to return for selection queries
   */
  private void setMaxDocsForSelection(BrokerRequest brokerRequest) {
    if (!brokerRequest.isSetAggregationsInfo()) {
      Selection selection = brokerRequest.getSelections();

      // Update MaxDocPerNextCall
      if (selection.getSize() > 0) {
        List<SelectionSort> sortSequence = selection.getSelectionSortSequence();
        if (sortSequence == null) {
          // For selection only queries, select minimum number of documents
          _maxDocPerNextCall = Math.min(selection.getSize(), _maxDocPerNextCall);
        }
      } else {
        // For LIMIT 0 queries, fetch at least 1 document per DocIdSetPlanNode's requirement
        // TODO: Skip the filtering phase and document fetching for LIMIT 0 case
        _maxDocPerNextCall = 1;
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
