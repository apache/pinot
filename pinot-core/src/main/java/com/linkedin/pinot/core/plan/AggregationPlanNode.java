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
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionUtils;
import com.linkedin.pinot.core.operator.query.AggregationOperator;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>AggregationPlanNode</code> class provides the execution plan for aggregation only query on a single
 * segment.
 */
public class AggregationPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregationPlanNode.class);

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfos;
  private final ProjectionPlanNode _projectionPlanNode;

  public AggregationPlanNode(@Nonnull IndexSegment indexSegment, @Nonnull BrokerRequest brokerRequest) {
    _indexSegment = indexSegment;
    _aggregationInfos = brokerRequest.getAggregationsInfo();
    _projectionPlanNode = new ProjectionPlanNode(_indexSegment, getAggregationRelatedColumns(),
        new DocIdSetPlanNode(_indexSegment, brokerRequest));
  }

  @Nonnull
  private String[] getAggregationRelatedColumns() {
    Set<String> aggregationRelatedColumns = new HashSet<>();
    for (AggregationInfo aggregationInfo : _aggregationInfos) {
      if (!aggregationInfo.getAggregationType()
          .equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.COUNT.getName())) {
        String columns = aggregationInfo.getAggregationParams().get("column").trim();
        aggregationRelatedColumns.addAll(Arrays.asList(columns.split(",")));
      }
    }
    return aggregationRelatedColumns.toArray(new String[aggregationRelatedColumns.size()]);
  }

  @Override
  public Operator run() {
    MProjectionOperator projectionOperator = (MProjectionOperator) _projectionPlanNode.run();
    SegmentMetadata segmentMetadata = _indexSegment.getSegmentMetadata();
    return new AggregationOperator(
        AggregationFunctionUtils.getAggregationFunctionContexts(_aggregationInfos, segmentMetadata), projectionOperator,
        segmentMetadata.getTotalRawDocs());
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Segment Level Inner-Segment Plan Node:");
    LOGGER.debug(prefix + "Operator: AggregationOperator");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    LOGGER.debug(prefix + "Argument 1: Aggregations - " + _aggregationInfos);
    LOGGER.debug(prefix + "Argument 2: Projection -");
    _projectionPlanNode.showTree(prefix + "    ");
  }
}
