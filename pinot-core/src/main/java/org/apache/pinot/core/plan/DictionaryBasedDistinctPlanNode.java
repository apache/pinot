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

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.query.DictionaryBasedDistinctOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Execute a DISTINCT operation using dictionary based plan
 */
public class DictionaryBasedDistinctPlanNode implements PlanNode {
  private final FieldSpec.DataType _dataType;
  private final int _numDocs;
  private final DistinctAggregationFunction _distinctAggregationFunction;
  private final Dictionary _dictionary;

  /**
   * Constructor for the class.
   *
   * @param indexSegment Segment to process
   * @param queryContext Query context
   */
  public DictionaryBasedDistinctPlanNode(IndexSegment indexSegment, QueryContext queryContext, Dictionary dictionary) {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();

    assert aggregationFunctions != null && aggregationFunctions.length == 1
        && aggregationFunctions[0] instanceof DistinctAggregationFunction;

    _distinctAggregationFunction = (DistinctAggregationFunction) aggregationFunctions[0];

    List<ExpressionContext> expressions = _distinctAggregationFunction.getInputExpressions();
    ExpressionContext expression = expressions.get(0);
    _dataType = indexSegment.getDataSource(expression.getIdentifier()).getDataSourceMetadata().getDataType();

    _dictionary = dictionary;
    _numDocs = indexSegment.getSegmentMetadata().getTotalDocs();
  }

  @Override
  public DictionaryBasedDistinctOperator run() {
    return new DictionaryBasedDistinctOperator(_dataType, _distinctAggregationFunction, _dictionary, _numDocs);
  }
}
