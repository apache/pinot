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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.dociditerators.ExpressionScanDocIdIterator;
import org.apache.pinot.core.operator.docidsets.ExpressionDocIdSet;
import org.apache.pinot.core.operator.docidsets.NotDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class ExpressionFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_EXPRESSION";

  private final QueryContext _queryContext;
  private final Map<String, DataSource> _dataSourceMap;
  private final TransformFunction _transformFunction;
  private final Predicate.Type _predicateType;
  private final PredicateEvaluator _predicateEvaluator;

  public ExpressionFilterOperator(IndexSegment segment, QueryContext queryContext, Predicate predicate, int numDocs) {
    super(numDocs, queryContext.isNullHandlingEnabled());
    _queryContext = queryContext;

    Set<String> columns = new HashSet<>();
    ExpressionContext lhs = predicate.getLhs();
    lhs.getColumns(columns);
    int mapCapacity = HashUtil.getHashMapCapacity(columns.size());
    _dataSourceMap = new HashMap<>(mapCapacity);
    Map<String, ColumnContext> columnContextMap = new HashMap<>(mapCapacity);
    columns.forEach(column -> {
      DataSource dataSource = segment.getDataSource(column);
      _dataSourceMap.put(column, dataSource);
      columnContextMap.put(column, ColumnContext.fromDataSource(dataSource));
    });
    _transformFunction = TransformFunctionFactory.get(lhs, columnContextMap, _queryContext);
    _predicateType = predicate.getType();
    if (_predicateType == Predicate.Type.IS_NULL || _predicateType == Predicate.Type.IS_NOT_NULL) {
      _predicateEvaluator = null;
    } else {
      _predicateEvaluator =
          PredicateEvaluatorProvider.getPredicateEvaluator(predicate, _transformFunction.getDictionary(),
              _transformFunction.getResultMetadata().getDataType());
    }
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_predicateType == Predicate.Type.IS_NULL) {
      return getNulls();
    } else if (_predicateType == Predicate.Type.IS_NOT_NULL) {
      return new NotDocIdSet(getNulls(), _numDocs);
    } else {
      return new ExpressionDocIdSet(_transformFunction, _predicateEvaluator, _dataSourceMap, _numDocs,
          _queryContext.isNullHandlingEnabled(), ExpressionScanDocIdIterator.PredicateEvaluationResult.TRUE);
    }
  }

  @Override
  protected BlockDocIdSet getNulls() {
    return new ExpressionDocIdSet(_transformFunction, null, _dataSourceMap, _numDocs,
        _queryContext.isNullHandlingEnabled(), ExpressionScanDocIdIterator.PredicateEvaluationResult.NULL);
  }

  @Override
  protected BlockDocIdSet getFalses() {
    if (_predicateType == Predicate.Type.IS_NULL) {
      return new NotDocIdSet(getNulls(), _numDocs);
    } else if (_predicateType == Predicate.Type.IS_NOT_NULL) {
      return getNulls();
    } else {
      return new ExpressionDocIdSet(_transformFunction, _predicateEvaluator, _dataSourceMap, _numDocs,
          _queryContext.isNullHandlingEnabled(), ExpressionScanDocIdIterator.PredicateEvaluationResult.FALSE);
    }
  }

  @Override
  public List<Operator<?>> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder =
        new StringBuilder(EXPLAIN_NAME).append("(operator:").append(_predicateEvaluator.getPredicateType());
    stringBuilder.append(",predicate:").append(_predicateEvaluator.getPredicate().toString());
    return stringBuilder.append(')').toString();
  }
}
