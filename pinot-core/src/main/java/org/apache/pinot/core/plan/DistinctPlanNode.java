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
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.DictionaryBasedDistinctOperator;
import org.apache.pinot.core.operator.query.DistinctOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Execution plan for distinct queries on a single segment.
 */
@SuppressWarnings("rawtypes")
public class DistinctPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public DistinctPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public Operator<IntermediateResultsBlock> run() {
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null && aggregationFunctions.length == 1
        && aggregationFunctions[0] instanceof DistinctAggregationFunction;
    DistinctAggregationFunction distinctAggregationFunction = (DistinctAggregationFunction) aggregationFunctions[0];
    List<ExpressionContext> expressions = distinctAggregationFunction.getInputExpressions();

    // Use dictionary to solve the query if possible
    if (_queryContext.getFilter() == null && expressions.size() == 1) {
      ExpressionContext expression = expressions.get(0);
      if (expression.getType() == ExpressionContext.Type.IDENTIFIER) {
        DataSource dataSource = _indexSegment.getDataSource(expression.getIdentifier());
        Dictionary dictionary = dataSource.getDictionary();
        if (dictionary != null) {
          DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
          return new DictionaryBasedDistinctOperator(dataSourceMetadata.getDataType(), distinctAggregationFunction,
              dictionary, dataSourceMetadata.getNumDocs());
        }
      }
    }

    TransformOperator transformOperator =
        new TransformPlanNode(_indexSegment, _queryContext, expressions, DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
    return new DistinctOperator(_indexSegment, distinctAggregationFunction, transformOperator);
  }
}
