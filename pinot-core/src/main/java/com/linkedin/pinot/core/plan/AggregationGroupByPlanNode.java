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
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.aggregation.groupby.AggregationGroupByOperator;
import com.linkedin.pinot.core.query.config.AggregationOperatorConfig;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>AggregationGroupByPlanNode</code> class provides the execution plan for aggregation group-by query on a
 * single segment.
 */
public class AggregationGroupByPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregationGroupByPlanNode.class);

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfos;
  private final GroupBy _groupBy;
  private final ProjectionPlanNode _projectionPlanNode;
  private final int _numAggrGroupsLimit;

  public AggregationGroupByPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest, int numAggrGroupsLimit) {
    _indexSegment = indexSegment;
    _aggregationInfos = brokerRequest.getAggregationsInfo();
    _groupBy = brokerRequest.getGroupBy();
    _numAggrGroupsLimit = numAggrGroupsLimit;
    _projectionPlanNode = new ProjectionPlanNode(_indexSegment, getAggregationGroupByRelatedColumns(),
        new DocIdSetPlanNode(_indexSegment, brokerRequest, 5000));
  }

  private String[] getAggregationGroupByRelatedColumns() {
    Set<String> aggregationGroupByRelatedColumns = new HashSet<>();
    for (AggregationInfo aggregationInfo : _aggregationInfos) {
      if (aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
        continue;
      }
      String columns = aggregationInfo.getAggregationParams().get("column").trim();
      aggregationGroupByRelatedColumns.addAll(Arrays.asList(columns.split(",")));
    }
    aggregationGroupByRelatedColumns.addAll(_groupBy.getColumns());
    return aggregationGroupByRelatedColumns.toArray(new String[aggregationGroupByRelatedColumns.size()]);
  }

  @Override
  public Operator run() {
    MProjectionOperator projectionOperator = (MProjectionOperator) _projectionPlanNode.run();
    AggregationOperatorConfig aggregationConfig = AggregationOperatorConfig.instantiateFrom(_indexSegment.getSegmentMetadata());
    return new AggregationGroupByOperator(_aggregationInfos, _groupBy, projectionOperator, _numAggrGroupsLimit,
        aggregationConfig);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Segment Level Inner-Segment Plan Node:");
    LOGGER.debug(prefix + "Operator: AggregationGroupByOperator");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    LOGGER.debug(prefix + "Argument 1: Aggregations - " + _aggregationInfos);
    LOGGER.debug(prefix + "Argument 2: GroupBy - " + _groupBy);
    LOGGER.debug(prefix + "Argument 3: Projection -");
    _projectionPlanNode.showTree(prefix + "    ");
  }
}
