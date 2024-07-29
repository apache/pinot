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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.utils.ParentAggregationFunctionResultObject;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Base class for parent aggregation functions. A parent aggregation function is an aggregation function
 * whose result is a nested data block containing multiple columns, each of which corresponds to a child
 * aggregation function's result.
 */
public abstract class ParentAggregationFunction<I, F extends ParentAggregationFunctionResultObject>
    implements AggregationFunction<I, F> {

  protected static final int PARENT_AGGREGATION_FUNCTION_ID_OFFSET = 0;
  protected List<ExpressionContext> _arguments;

  ParentAggregationFunction(List<ExpressionContext> arguments) {
    _arguments = arguments;
  }

  @Override
  public final DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  // The name of the column is the prefix of the parent aggregation function + the name of the
  // aggregation function + the id of the parent aggregation function
  // e.g. if the parent aggregation function is "exprmax(0,3,a,b,c,x,y,z)", the name of the column is
  // "pinotparentaggregationexprmax0"
  @Override
  public final String getResultColumnName() {
    return CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX
        + getType().getName().toLowerCase()
        + _arguments.get(PARENT_AGGREGATION_FUNCTION_ID_OFFSET).getLiteral().getIntValue();
  }

  public final String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX)
        .append("_").append(getType().getName()).append('(');
    int numArguments = _arguments.size();
    if (numArguments > 0) {
      stringBuilder.append(_arguments.get(0).toString());
      for (int i = 1; i < numArguments; i++) {
        stringBuilder.append(", ").append(_arguments.get(i).toString());
      }
    }
    return stringBuilder.append(')').toString();
  }
}
