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
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * The <code>EmptySelectionOperator</code> class provides the operator for selection query with LIMIT less or equal to 0
 * on a single segment.
 * <p>NOTE: this operator short circuit underlying operators and directly returns the data schema without any rows.
 */
public class EmptySelectionOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "EmptySelectionOperator";

  private final DataSchema _dataSchema;
  private final ExecutionStatistics _executionStatistics;

  public EmptySelectionOperator(IndexSegment indexSegment, List<ExpressionContext> expressions,
      TransformOperator transformOperator) {
    int numExpressions = expressions.size();
    String[] columnNames = new String[numExpressions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      ExpressionContext expression = expressions.get(i);
      TransformResultMetadata expressionMetadata = transformOperator.getResultMetadata(expression);
      columnNames[i] = expression.toString();
      columnDataTypes[i] =
          DataSchema.ColumnDataType.fromDataType(expressionMetadata.getDataType(), expressionMetadata.isSingleValue());
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);

    _executionStatistics = new ExecutionStatistics(0L, 0L, 0L, indexSegment.getSegmentMetadata().getTotalDocs());
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    return new IntermediateResultsBlock(_dataSchema, Collections.emptyList());
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
