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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.query.AggregationOperator;
import com.linkedin.pinot.core.operator.transform.TransformExpressionOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory.AggregationFunctionType;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final AggregationFunctionContext[] _aggregationFunctionContexts;
  private final TransformPlanNode _transformPlanNode;
  private final Map<String, FieldSpec.DataType> _dataTypeMap;

  public AggregationPlanNode(@Nonnull IndexSegment indexSegment, @Nonnull BrokerRequest brokerRequest) {
    _indexSegment = indexSegment;
    _aggregationInfos = brokerRequest.getAggregationsInfo();
    _transformPlanNode = new TransformPlanNode(_indexSegment, brokerRequest);
    _dataTypeMap = new HashMap<>();
    _aggregationFunctionContexts =
        AggregationFunctionUtils.getAggregationFunctionContexts(_aggregationInfos, _indexSegment.getSegmentMetadata());
    for (AggregationFunctionContext aggregationFunctionContext : _aggregationFunctionContexts) {
      // For count(*), there's no column to have the datatype or dictionary for
      if (!aggregationFunctionContext.getAggregationFunction()
          .getName()
          .equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
        String column = aggregationFunctionContext.getAggregationColumns()[0];
        if (!_dataTypeMap.containsKey(column)) {
          _dataTypeMap.put(column, _indexSegment.getDataSource(column).getDataSourceMetadata().getDataType());
        }
      }
    }
  }

  @Override
  public Operator run() {
    TransformExpressionOperator transformOperator = (TransformExpressionOperator) _transformPlanNode.run();
    return new AggregationOperator(_aggregationFunctionContexts, transformOperator,
        _indexSegment.getSegmentMetadata().getTotalRawDocs(), _dataTypeMap);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Segment Level Inner-Segment Plan Node:");
    LOGGER.debug(prefix + "Operator: AggregationOperator");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    LOGGER.debug(prefix + "Argument 1: Aggregations - " + _aggregationInfos);
    LOGGER.debug(prefix + "Argument 2: Transform -");
    _transformPlanNode.showTree(prefix + "    ");
  }
}
