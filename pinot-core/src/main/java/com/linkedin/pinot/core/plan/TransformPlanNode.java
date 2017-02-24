/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.transform.TransformExpressionOperator;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>TransformPlanNode</code> class provides the execution plan for transforms on a single segment.
 */
public class TransformPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformPlanNode.class);

  private final ProjectionPlanNode _projectionPlanNode;
  private final List<TransformExpressionTree> _expressionTrees;
  private final String _segmentName;

  private static ThreadLocal<Pql2Compiler> _compiler = new ThreadLocal<Pql2Compiler>() {
    @Override
    protected Pql2Compiler initialValue() {
      return new Pql2Compiler();
    }
  };

  /**
   * Constructor for the class
   *
   * @param indexSegment Segment to process
   * @param brokerRequest BrokerRequest to process
   */
  public TransformPlanNode(@Nonnull IndexSegment indexSegment, @Nonnull BrokerRequest brokerRequest) {

    Set<String> projectionColumns = new HashSet<>();
    Set<String> transformExpressions = new HashSet<>();
    _segmentName = indexSegment.getSegmentName();

    extractColumnsAndTransforms(brokerRequest, projectionColumns, transformExpressions);

    if (!transformExpressions.isEmpty()) {
      _expressionTrees = buildTransformExpressionTrees(transformExpressions);
      projectionColumns.addAll(getTransformColumns(_expressionTrees));
    } else {
      _expressionTrees = null;
    }

    _projectionPlanNode =
        new ProjectionPlanNode(indexSegment, projectionColumns.toArray(new String[projectionColumns.size()]),
            new DocIdSetPlanNode(indexSegment, brokerRequest));
  }

  /**
   * Helper method to get all columns from expressions.
   * @param expressionTrees List of expression trees for which to get the columns.
   * @return List of columns from all the expression trees
   */
  private List<String> getTransformColumns(List<TransformExpressionTree> expressionTrees) {
    List<String> columns = new ArrayList<>();
    for (TransformExpressionTree expressionTree : expressionTrees) {
      expressionTree.getColumns(columns);
    }
    return columns;
  }

  /**
   * Helper method to build TransformExpressionTrees for a given set of transform expressions.
   * Ignores any trees that are just columns.
   *
   * @param transformExpressions Set of expressions for which to build trees.
   * @return List of expression trees for the given transform expressions.
   */
  public static List<TransformExpressionTree> buildTransformExpressionTrees(Set<String> transformExpressions) {
    List<TransformExpressionTree> expressionTrees = new ArrayList<>(transformExpressions.size());

    for (String transformExpression : transformExpressions) {
      TransformExpressionTree expressionTree = _compiler.get().compileToExpressionTree(transformExpression);
      if (!expressionTree.isColumn()) {
        expressionTrees.add(expressionTree);
      }
    }
    return expressionTrees;
  }

  /**
   * Helper method to extract projection columns and transform expressions from the given
   * BrokerRequest.
   *  @param brokerRequest BrokerRequest to process
   * @param projectionColumns Output projection columns from broker request
   * @param transformExpressions Output transform expression from broker request
   */
  private void extractColumnsAndTransforms(BrokerRequest brokerRequest, Set<String> projectionColumns,
      Set<String> transformExpressions) {

    if (brokerRequest.isSetAggregationsInfo()) {
      // TODO: Add transform support.
      for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
        if (!aggregationInfo.getAggregationType()
            .equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.COUNT.getName())) {
          String columns = aggregationInfo.getAggregationParams().get("column").trim();
          projectionColumns.addAll(Arrays.asList(columns.split(",")));
        }
      }

      // Collect all group by related columns.
      if (brokerRequest.isSetGroupBy()) {
        GroupBy groupBy = brokerRequest.getGroupBy();
        List<String> groupByColumns = groupBy.getColumns();

        // GroupByColumns can be null if all group-bys are transforms.
        if (groupByColumns != null) {
          projectionColumns.addAll(groupByColumns);
        }

        // Check null for backward compatibility.
        List<String> expressions = groupBy.getExpressions();
        if (expressions != null) {
          transformExpressions.addAll(expressions);
          if (groupByColumns != null && !groupByColumns.isEmpty()) {
            transformExpressions.removeAll(groupByColumns);
          }
        }
      }
    } else {
      throw new UnsupportedOperationException("Transforms not supported in selection queries.");
      // TODO: Add transform support.
      // projectionColumns.addAll(brokerRequest.getSelections().getSelectionColumns());
    }
  }

  @Override
  public Operator run() {
    MProjectionOperator projectionOperator = (MProjectionOperator) _projectionPlanNode.run();
    return new TransformExpressionOperator(projectionOperator, _expressionTrees);
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
