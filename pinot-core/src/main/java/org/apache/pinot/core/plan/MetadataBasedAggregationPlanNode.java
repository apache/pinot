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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.query.MetadataBasedAggregationOperator;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Metadata based aggregation plan node.
 */
public class MetadataBasedAggregationPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataBasedAggregationPlanNode.class);

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfos;
  private final BrokerRequest _brokerRequest;

  /**
   * Constructor for the class.
   *
   * @param indexSegment Segment to process
   * @param brokerRequest Broker request
   */
  public MetadataBasedAggregationPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    _indexSegment = indexSegment;
    _brokerRequest = brokerRequest;
    _aggregationInfos = brokerRequest.getAggregationsInfo();
  }

  @Override
  public Operator run() {
    SegmentMetadata segmentMetadata = _indexSegment.getSegmentMetadata();
    AggregationFunctionContext[] aggregationFunctionContexts =
        AggregationFunctionUtils.getAggregationFunctionContexts(_brokerRequest, segmentMetadata);

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
