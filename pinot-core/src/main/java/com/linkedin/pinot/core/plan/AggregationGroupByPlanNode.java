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
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.query.AggregationGroupByOperator;
import com.linkedin.pinot.core.operator.transform.TransformExpressionOperator;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import java.util.List;
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
  private final List<AggregationInfo> _aggregationInfos;
  private final GroupBy _groupBy;
  private final TransformPlanNode _transformPlanNode;
  private final int _maxInitialResultHolderCapacity;
  private final int _numGroupsLimit;

  public AggregationGroupByPlanNode(@Nonnull IndexSegment indexSegment, @Nonnull BrokerRequest brokerRequest,
      int maxInitialResultHolderCapacity, int numGroupsLimit) {
    _indexSegment = indexSegment;
    _aggregationInfos = brokerRequest.getAggregationsInfo();
    _groupBy = brokerRequest.getGroupBy();
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
    _transformPlanNode = new TransformPlanNode(_indexSegment, brokerRequest);
  }

  @Override
  public Operator run() {
    TransformExpressionOperator transformOperator = (TransformExpressionOperator) _transformPlanNode.run();
    SegmentMetadata segmentMetadata = _indexSegment.getSegmentMetadata();
    return new AggregationGroupByOperator(
        AggregationFunctionUtils.getAggregationFunctionContexts(_aggregationInfos, segmentMetadata), _groupBy,
        _maxInitialResultHolderCapacity, _numGroupsLimit, transformOperator, segmentMetadata.getTotalRawDocs());
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Segment Level Inner-Segment Plan Node:");
    LOGGER.debug(prefix + "Operator: AggregationGroupByOperator");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    LOGGER.debug(prefix + "Argument 1: Aggregations - " + _aggregationInfos);
    LOGGER.debug(prefix + "Argument 2: GroupBy - " + _groupBy);
    LOGGER.debug(prefix + "Argument 3: Transform -");
    _transformPlanNode.showTree(prefix + "    ");
  }
}
