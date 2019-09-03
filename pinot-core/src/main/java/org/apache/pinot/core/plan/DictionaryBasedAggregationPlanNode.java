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
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dictionary based aggregation plan node.
 */
public class DictionaryBasedAggregationPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryBasedAggregationPlanNode.class);

  private final Map<String, Dictionary> _dictionaryMap;
  private final AggregationFunctionContext[] _aggregationFunctionContexts;
  private IndexSegment _indexSegment;

  /**
   * Constructor for the class.
   *
   * @param indexSegment Segment to process
   * @param aggregationInfos List of aggregation context info.
   */
  public DictionaryBasedAggregationPlanNode(IndexSegment indexSegment, List<AggregationInfo> aggregationInfos) {
    _indexSegment = indexSegment;
    _dictionaryMap = new HashMap<>();

    _aggregationFunctionContexts =
        AggregationFunctionUtils.getAggregationFunctionContexts(aggregationInfos, indexSegment.getSegmentMetadata());

    for (AggregationFunctionContext aggregationFunctionContext : _aggregationFunctionContexts) {
      String column = aggregationFunctionContext.getColumn();
      if (!_dictionaryMap.containsKey(column)) {
        _dictionaryMap.put(column, _indexSegment.getDataSource(column).getDictionary());
      }
    }
  }

  @Override
  public Operator run() {
    return new DictionaryBasedAggregationOperator(_aggregationFunctionContexts,
        _indexSegment.getSegmentMetadata().getTotalRawDocs(), _dictionaryMap);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug("{} Segment Level Inner-Segment Plan Node:", prefix);
    LOGGER.debug("{} Operator: DictionaryBasedAggregationOperator", prefix);
    LOGGER.debug("{} IndexSegment: {}", prefix, _indexSegment.getSegmentName());
  }
}
