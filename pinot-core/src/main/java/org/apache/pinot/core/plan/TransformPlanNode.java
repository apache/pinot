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

import static org.apache.pinot.core.query.selection.SelectionOperatorUtils.getSelectionColumns;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.common.function.AggregationFunctionType;
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
  private final Set<TransformExpressionTree> _expressionTrees = new LinkedHashSet<>();
  private int _maxDocPerNextCall = DocIdSetPlanNode.MAX_DOC_PER_CALL;

  /**
   * Constructor for the class
   *
   * @param indexSegment Segment to process
   * @param brokerRequest BrokerRequest to process
   */
  public TransformPlanNode(@Nonnull IndexSegment indexSegment, @Nonnull BrokerRequest brokerRequest) {
    _segmentName = indexSegment.getSegmentName();
    extractColumnsAndTransforms(brokerRequest, indexSegment);
    _projectionPlanNode =
        new ProjectionPlanNode(indexSegment, _projectionColumns, new DocIdSetPlanNode(indexSegment, brokerRequest, _maxDocPerNextCall));
  }

  /**
   * Helper method to extract projection columns and transform expressions from the given broker request.
   *
   * @param brokerRequest Broker request to process
   * @param indexSegment
   */
  private void extractColumnsAndTransforms(@Nonnull BrokerRequest brokerRequest,
      IndexSegment indexSegment) {
    if (brokerRequest.isSetAggregationsInfo()) {
      for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
        if (!aggregationInfo.getAggregationType().equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
          String expression = AggregationFunctionUtils.getColumn(aggregationInfo);
          TransformExpressionTree transformExpressionTree = TransformExpressionTree.compileToExpressionTree(expression);
          transformExpressionTree.getColumns(_projectionColumns);
          _expressionTrees.add(transformExpressionTree);
        }
      }

      // Process all group-by expressions
      if (brokerRequest.isSetGroupBy()) {
        for (String expression : brokerRequest.getGroupBy().getExpressions()) {
          TransformExpressionTree transformExpressionTree = TransformExpressionTree.compileToExpressionTree(expression);
          transformExpressionTree.getColumns(_projectionColumns);
          _expressionTrees.add(transformExpressionTree);
        }
      }
    } else {
//      throw new UnsupportedOperationException("Transforms not supported in selection queries.");
      Selection selection = brokerRequest.getSelections();
      // No ordering required, select minimum number of documents
      if (!selection.isSetSelectionSortSequence()) {
        _maxDocPerNextCall = Math.min(selection.getOffset() + selection.getSize(), _maxDocPerNextCall);
      }
      List<String> expressions = selection.getSelectionColumns();
      if (expressions.size() == 1 && expressions.get(0).equals("*")) {
        expressions = new LinkedList<>(indexSegment.getPhysicalColumnNames());
        Collections.sort(expressions);
      }
      if (selection.getSelectionSortSequence() != null) {
        for (SelectionSort selectionSort : selection.getSelectionSortSequence()) {
          String expression = selectionSort.getColumn();
          if(!expressions.contains(expression)) {
            expressions.add(expression);
          }
        }
      }
      for (String expression : expressions) {
        TransformExpressionTree transformExpressionTree = TransformExpressionTree.compileToExpressionTree(expression);
        transformExpressionTree.getColumns(_projectionColumns);
        _expressionTrees.add(transformExpressionTree);
      }
    }
  }

  @Override
  public TransformOperator run() {
    return new TransformOperator(_projectionPlanNode.run(), new ArrayList<>(_expressionTrees));
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
