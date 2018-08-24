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
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.query.AggregationGroupByOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import com.linkedin.pinot.core.startree.StarTreeUtils;
import com.linkedin.pinot.core.startree.plan.StarTreeTransformPlanNode;
import com.linkedin.pinot.core.startree.v2.AggregationFunctionColumnPair;
import com.linkedin.pinot.core.startree.v2.StarTreeV2;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>AggregationGroupByPlanNode</code> class provides the execution plan for aggregation group-by query on a
 * single segment.
 */
public class AggregationGroupByPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregationGroupByPlanNode.class);

  private final IndexSegment _indexSegment;
  private final int _maxInitialResultHolderCapacity;
  private final int _numGroupsLimit;
  private final List<AggregationInfo> _aggregationInfos;
  private final AggregationFunctionContext[] _functionContexts;
  private final GroupBy _groupBy;
  private final TransformPlanNode _transformPlanNode;
  private final StarTreeTransformPlanNode _starTreeTransformPlanNode;

  public AggregationGroupByPlanNode(@Nonnull IndexSegment indexSegment, @Nonnull BrokerRequest brokerRequest,
      int maxInitialResultHolderCapacity, int numGroupsLimit) {
    _indexSegment = indexSegment;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
    _aggregationInfos = brokerRequest.getAggregationsInfo();
    _functionContexts =
        AggregationFunctionUtils.getAggregationFunctionContexts(_aggregationInfos, indexSegment.getSegmentMetadata());
    _groupBy = brokerRequest.getGroupBy();

    List<StarTreeV2> starTrees = indexSegment.getStarTrees();
    if (starTrees != null) {
      if (!StarTreeUtils.isStarTreeDisabled(brokerRequest)) {
        Set<AggregationFunctionColumnPair> aggregationFunctionColumnPairs = new HashSet<>();
        for (AggregationInfo aggregationInfo : _aggregationInfos) {
          aggregationFunctionColumnPairs.add(AggregationFunctionUtils.getFunctionColumnPair(aggregationInfo));
        }
        Set<TransformExpressionTree> groupByExpressions = new HashSet<>();
        for (String expression : _groupBy.getExpressions()) {
          groupByExpressions.add(TransformExpressionTree.compileToExpressionTree(expression));
        }
        FilterQueryTree rootFilterNode = RequestUtils.generateFilterQueryTree(brokerRequest);
        for (StarTreeV2 starTreeV2 : starTrees) {
          if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs,
              groupByExpressions, rootFilterNode)) {
            _transformPlanNode = null;
            _starTreeTransformPlanNode =
                new StarTreeTransformPlanNode(starTreeV2, aggregationFunctionColumnPairs, groupByExpressions,
                    rootFilterNode, brokerRequest.getDebugOptions());
            return;
          }
        }
      }
    }

    _transformPlanNode = new TransformPlanNode(_indexSegment, brokerRequest);
    _starTreeTransformPlanNode = null;
  }

  @Override
  public AggregationGroupByOperator run() {
    int numTotalRawDocs = _indexSegment.getSegmentMetadata().getTotalRawDocs();
    if (_transformPlanNode != null) {
      // Do not use star-tree
      return new AggregationGroupByOperator(_functionContexts, _groupBy, _maxInitialResultHolderCapacity,
          _numGroupsLimit, _transformPlanNode.run(), numTotalRawDocs, false);
    } else {
      // Use star-tree
      return new AggregationGroupByOperator(_functionContexts, _groupBy, _maxInitialResultHolderCapacity,
          _numGroupsLimit, _starTreeTransformPlanNode.run(), numTotalRawDocs, true);
    }
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Aggregation Group-by Plan Node:");
    LOGGER.debug(prefix + "Operator: AggregationGroupByOperator");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    LOGGER.debug(prefix + "Argument 1: Aggregations - " + _aggregationInfos);
    LOGGER.debug(prefix + "Argument 2: GroupBy - " + _groupBy);
    if (_transformPlanNode != null) {
      LOGGER.debug(prefix + "Argument 3: TransformPlanNode -");
      _transformPlanNode.showTree(prefix + "    ");
    } else {
      LOGGER.debug(prefix + "Argument 3: StarTreeTransformPlanNode -");
      _starTreeTransformPlanNode.showTree(prefix + "    ");
    }
  }
}
