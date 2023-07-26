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
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.query.DictionaryBasedDistinctOperator;
import org.apache.pinot.core.operator.query.DistinctOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;


/**
 * Execution plan for distinct queries on a single segment.
 */
public class DistinctPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public DistinctPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public Operator<DistinctResultsBlock> run() {
    List<ExpressionContext> expressions = _queryContext.getSelectExpressions();

    // Use dictionary to solve the query if possible
    if (_queryContext.getFilter() == null && expressions.size() == 1) {
      String column = expressions.get(0).getIdentifier();
      if (column != null) {
        DataSource dataSource = _indexSegment.getDataSource(column);
        if (dataSource.getDictionary() != null) {
          if (!_queryContext.isNullHandlingEnabled()) {
            return new DictionaryBasedDistinctOperator(dataSource, _queryContext);
          }
          // If nullHandlingEnabled is set to true, and the column contains null values, call DistinctOperator instead
          // of DictionaryBasedDistinctOperator since nullValueVectorReader is a form of a filter.
          // TODO: reserve special value in dictionary (e.g. -1) for null in the future so
          //  DictionaryBasedDistinctOperator can be reused since it is more efficient than DistinctOperator for
          //  dictionary-encoded columns.
          NullValueVectorReader nullValueReader = dataSource.getNullValueVector();
          if (nullValueReader == null || nullValueReader.getNullBitmap().isEmpty()) {
            return new DictionaryBasedDistinctOperator(dataSource, _queryContext);
          }
        }
      }
    }

    BaseProjectOperator<?> projectOperator =
        new ProjectPlanNode(_indexSegment, _queryContext, expressions, DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
    return new DistinctOperator(_indexSegment, projectOperator, _queryContext);
  }
}
