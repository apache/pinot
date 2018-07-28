/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.transform.TransformOperator;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
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
  private final Set<TransformExpressionTree> _expressionTrees = new HashSet<>();

  /**
   * Constructor for the class
   *
   * @param indexSegment Segment to process
   * @param brokerRequest BrokerRequest to process
   */
  public TransformPlanNode(@Nonnull IndexSegment indexSegment, @Nonnull BrokerRequest brokerRequest) {
    _segmentName = indexSegment.getSegmentName();
    extractColumnsAndTransforms(brokerRequest);
    _projectionPlanNode =
        new ProjectionPlanNode(indexSegment, _projectionColumns, new DocIdSetPlanNode(indexSegment, brokerRequest));
  }

  /**
   * Helper method to extract projection columns and transform expressions from the given broker request.
   *
   * @param brokerRequest Broker request to process
   */
  private void extractColumnsAndTransforms(@Nonnull BrokerRequest brokerRequest) {
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
      throw new UnsupportedOperationException("Transforms not supported in selection queries.");
      // TODO: Add transform support.
      // projectionColumns.addAll(brokerRequest.getSelections().getSelectionColumns());
    }
  }

  @Override
  public TransformOperator run() {
    return new TransformOperator(_projectionPlanNode.run(), _expressionTrees);
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
