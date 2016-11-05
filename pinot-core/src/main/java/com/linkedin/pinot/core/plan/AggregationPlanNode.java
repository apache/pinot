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
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.aggregation.AggregationOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionUtils;
import com.linkedin.pinot.core.startree.hll.HllConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AggregationPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger("QueryPlanLog");

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final List<AggregationFunctionPlanNode> _aggregationFunctionPlanNodes =
      new ArrayList<AggregationFunctionPlanNode>();
  private final ProjectionPlanNode _projectionPlanNode;

  public AggregationPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _projectionPlanNode = new ProjectionPlanNode(_indexSegment, getAggregationRelatedColumns(),
        new DocIdSetPlanNode(_indexSegment, _brokerRequest));
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      AggregationInfo aggregationInfo = _brokerRequest.getAggregationsInfo().get(i);
      AggregationFunctionUtils.ensureAggregationColumnsAreSingleValued(aggregationInfo, _indexSegment);
      boolean hasDictionary = AggregationFunctionUtils.isAggregationFunctionWithDictionary(aggregationInfo, _indexSegment);
      _aggregationFunctionPlanNodes.add(new AggregationFunctionPlanNode(aggregationInfo, _projectionPlanNode, hasDictionary));
    }
  }

  private String[] getAggregationRelatedColumns() {
    Set<String> aggregationRelatedColumns = new HashSet<String>();
    for (AggregationInfo aggregationInfo : _brokerRequest.getAggregationsInfo()) {
      if (!aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
        String columns = aggregationInfo.getAggregationParams().get("column").trim();
        aggregationRelatedColumns.addAll(Arrays.asList(columns.split(",")));
        if (aggregationInfo.getAggregationType().equalsIgnoreCase("fasthll")) {
          aggregationRelatedColumns.add(columns + HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX);
        }
      }
    }
    return aggregationRelatedColumns.toArray(new String[0]);
  }

  @Override
  public Operator run() {
    MProjectionOperator projectionOperator = (MProjectionOperator) _projectionPlanNode.run();
    return new AggregationOperator(_brokerRequest.getAggregationsInfo(), projectionOperator, _indexSegment.getSegmentMetadata().getTotalRawDocs());
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Inner-Segment Plan Node :");
    LOGGER.debug(prefix + "Operator: MAggregationOperator");
    LOGGER.debug(prefix + "Argument 0: Projection - ");
    _projectionPlanNode.showTree(prefix + "    ");
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      LOGGER.debug(prefix + "Argument " + (i + 1) + ": Aggregation  - ");
      _aggregationFunctionPlanNodes.get(i).showTree(prefix + "    ");
    }
  }
}
