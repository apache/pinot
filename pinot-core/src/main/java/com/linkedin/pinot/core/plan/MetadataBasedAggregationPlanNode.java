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
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.query.MetadataBasedAggregationOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Metadata based aggregation plan node.
 */
public class MetadataBasedAggregationPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataBasedAggregationPlanNode.class);

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfos;

  /**
   * Constructor for the class.
   *
   * @param indexSegment Segment to process
   * @param aggregationInfos List of aggregation info
   */
  public MetadataBasedAggregationPlanNode(IndexSegment indexSegment, List<AggregationInfo> aggregationInfos) {
    _indexSegment = indexSegment;
    _aggregationInfos = aggregationInfos;
  }

  @Override
  public Operator run() {
    SegmentMetadata segmentMetadata = _indexSegment.getSegmentMetadata();
    AggregationFunctionContext[] aggregationFunctionContexts =
        AggregationFunctionUtils.getAggregationFunctionContexts(_aggregationInfos, segmentMetadata);

    Map<String, DataSource> dataSourceMap = new HashMap<>();
    for (AggregationFunctionContext aggregationFunctionContext : aggregationFunctionContexts) {
      if (aggregationFunctionContext.getAggregationFunction().getType() != AggregationFunctionType.COUNT) {
        String column = aggregationFunctionContext.getColumn();
        if (!dataSourceMap.containsKey(column)) {
          dataSourceMap.put(column, _indexSegment.getDataSource(column));
        }
      }
    }

    return new MetadataBasedAggregationOperator(aggregationFunctionContexts, segmentMetadata, dataSourceMap);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Segment Level Inner-Segment Plan Node:");
    LOGGER.debug(prefix + "Operator: MetadataBasedAggregationOperator");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    LOGGER.debug(prefix + "Argument 1: Aggregations - " + _aggregationInfos);
  }
}
