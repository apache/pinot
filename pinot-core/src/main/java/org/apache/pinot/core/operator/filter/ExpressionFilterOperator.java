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
package org.apache.pinot.core.operator.filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.ExpressionFilterDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.request.context.ExpressionContext;
import org.apache.pinot.spi.request.context.predicate.Predicate;


public class ExpressionFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "ExpressionFilterOperator";

  private final int _numDocs;
  private final Map<String, DataSource> _dataSourceMap;
  private final TransformFunction _transformFunction;
  private final PredicateEvaluator _predicateEvaluator;

  public ExpressionFilterOperator(IndexSegment segment, Predicate predicate, int numDocs) {
    _numDocs = numDocs;

    _dataSourceMap = new HashMap<>();
    Set<String> columns = new HashSet<>();
    ExpressionContext lhs = predicate.getLhs();
    lhs.getColumns(columns);
    for (String column : columns) {
      _dataSourceMap.put(column, segment.getDataSource(column));
    }

    _transformFunction = TransformFunctionFactory.get(lhs, _dataSourceMap);
    _predicateEvaluator = PredicateEvaluatorProvider
        .getPredicateEvaluator(predicate, _transformFunction.getDictionary(),
            _transformFunction.getResultMetadata().getDataType());
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(
        new ExpressionFilterDocIdSet(_transformFunction, _predicateEvaluator, _dataSourceMap, _numDocs));
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
