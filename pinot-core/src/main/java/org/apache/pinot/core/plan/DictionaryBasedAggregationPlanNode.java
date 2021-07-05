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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Dictionary based aggregation plan node.
 */
@SuppressWarnings("rawtypes")
public class DictionaryBasedAggregationPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final AggregationFunction[] _aggregationFunctions;
  private final Map<String, Dictionary> _dictionaryMap;
  private final int _limit;

  /**
   * Constructor for the class.
   *
   * @param indexSegment Segment to process
   * @param queryContext Query context
   */
  public DictionaryBasedAggregationPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _dictionaryMap = new HashMap<>();
    for (AggregationFunction aggregationFunction : _aggregationFunctions) {
      String column = ((ExpressionContext) aggregationFunction.getInputExpressions().get(0)).getIdentifier();
      _dictionaryMap.computeIfAbsent(column, k -> _indexSegment.getDataSource(k).getDictionary());
    }

    _limit = queryContext.getLimit();
  }

  @Override
  public DictionaryBasedAggregationOperator run() {
    return new DictionaryBasedAggregationOperator(_aggregationFunctions, _dictionaryMap,
        _indexSegment.getSegmentMetadata().getTotalDocs(), _limit);
  }
}
