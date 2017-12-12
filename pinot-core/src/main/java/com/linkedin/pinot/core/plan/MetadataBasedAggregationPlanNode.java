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
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.query.MetadataBasedAggregationOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
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

  private final Map<String, ColumnarDataSourcePlanNode> _dataSourcePlanNodeMap;
  private final AggregationFunctionContext[] _aggregationFunctionContexts;
  private IndexSegment _indexSegment;

  /**
   * Constructor for the class.
   *
   * @param indexSegment Segment to process
   * @param aggregationInfos List of aggregation context info.
   */
  public MetadataBasedAggregationPlanNode(IndexSegment indexSegment, List<AggregationInfo> aggregationInfos) {
    _indexSegment = indexSegment;
    _dataSourcePlanNodeMap = new HashMap<>();

    _aggregationFunctionContexts =
        AggregationFunctionUtils.getAggregationFunctionContexts(aggregationInfos, indexSegment.getSegmentMetadata());

    for (AggregationFunctionContext aggregationFunctionContext : _aggregationFunctionContexts) {
      String column = aggregationFunctionContext.getAggregationColumns()[0];

      if (!_dataSourcePlanNodeMap.containsKey(column)) {
        // For count(*), there's no column to have the metadata for.
        if (!aggregationFunctionContext.getAggregationFunction()
            .getName()
            .equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.COUNT.getName())) {
          _dataSourcePlanNodeMap.put(column, new ColumnarDataSourcePlanNode(indexSegment, column));
        }
      }
    }
  }

  @Override
  public Operator run() {
    Map<String, BaseOperator> dataSourceMap = new HashMap<>();

    for (String column : _dataSourcePlanNodeMap.keySet()) {
      ColumnarDataSourcePlanNode columnarDataSourcePlanNode = _dataSourcePlanNodeMap.get(column);
      BaseOperator operator = columnarDataSourcePlanNode.run();
      dataSourceMap.put(column, operator);
    }

    return new MetadataBasedAggregationOperator(_aggregationFunctionContexts, _indexSegment.getSegmentMetadata(),
        dataSourceMap);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug("{} Segment Level Inner-Segment Plan Node:", prefix);
    LOGGER.debug("{} Operator: MetadataBasedAggregationOperator", prefix);
    LOGGER.debug("{} IndexSegment: {}", prefix, _indexSegment.getSegmentName());

    int i = 0;
    for (String column : _dataSourcePlanNodeMap.keySet()) {
      LOGGER.debug("{} Argument {}: DataSourceOperator", prefix, (i + 1));
      _dataSourcePlanNodeMap.get(column).showTree(prefix + "    ");
      i++;
    }
  }
}
