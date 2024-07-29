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
package org.apache.pinot.core.operator.query;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * The <code>EmptySelectionOperator</code> class provides the operator for selection query with LIMIT less or equal to 0
 * on a single segment.
 * <p>NOTE: this operator short circuit underlying operators and directly returns the data schema without any rows.
 */
public class EmptySelectionOperator extends BaseOperator<SelectionResultsBlock> {
  private static final String EXPLAIN_NAME = "SELECT_EMPTY";

  private final BaseProjectOperator<?> _projectOperator;
  private final QueryContext _queryContext;
  private final DataSchema _dataSchema;
  private final ExecutionStatistics _executionStatistics;

  public EmptySelectionOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, BaseProjectOperator<?> projectOperator) {
    _projectOperator = projectOperator;
    _queryContext = queryContext;

    int numExpressions = expressions.size();
    String[] columnNames = new String[numExpressions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      ExpressionContext expression = expressions.get(i);
      columnNames[i] = expression.toString();
      ColumnContext columnContext = projectOperator.getResultColumnContext(expression);
      columnDataTypes[i] =
          DataSchema.ColumnDataType.fromDataType(columnContext.getDataType(), columnContext.isSingleValue());
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);

    _executionStatistics = new ExecutionStatistics(0L, 0L, 0L, indexSegment.getSegmentMetadata().getTotalDocs());
  }

  @Override
  protected SelectionResultsBlock getNextBlock() {
    return new SelectionResultsBlock(_dataSchema, Collections.emptyList(), _queryContext);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<BaseProjectOperator<?>> getChildOperators() {
    return Collections.singletonList(_projectOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
