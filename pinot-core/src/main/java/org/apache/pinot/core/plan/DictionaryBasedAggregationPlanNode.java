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
import java.util.Map;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dictionary based aggregation plan node.
 */
public class DictionaryBasedAggregationPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryBasedAggregationPlanNode.class);

  private final IndexSegment _indexSegment;
  private final AggregationFunction[] _aggregationFunctions;
  private final Map<String, Dictionary> _dictionaryMap;

  /**
   * Constructor for the class.
   *
   * @param indexSegment Segment to process
   * @param brokerRequest Broker request
   */
  public DictionaryBasedAggregationPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    _indexSegment = indexSegment;
    _aggregationFunctions = AggregationFunctionUtils.getAggregationFunctions(brokerRequest);
    _dictionaryMap = new HashMap<>();
    for (AggregationFunction aggregationFunction : _aggregationFunctions) {
      String column = ((TransformExpressionTree) aggregationFunction.getInputExpressions().get(0)).getValue();
      _dictionaryMap.computeIfAbsent(column, k -> _indexSegment.getDataSource(k).getDictionary());
    }
  }

  @Override
  public Operator run() {
    return new DictionaryBasedAggregationOperator(_aggregationFunctions, _dictionaryMap,
        _indexSegment.getSegmentMetadata().getTotalDocs());
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug("{} Segment Level Inner-Segment Plan Node:", prefix);
    LOGGER.debug("{} Operator: DictionaryBasedAggregationOperator", prefix);
    LOGGER.debug("{} IndexSegment: {}", prefix, _indexSegment.getSegmentName());
  }
}
