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
package org.apache.pinot.core.operator.transform;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Class for evaluating transform expressions.
 */
public class TransformOperator extends BaseOperator<TransformBlock> {
  private static final String OPERATOR_NAME = "TransformOperator";
  private static final String EXPLAIN_NAME = null;

  protected final ProjectionOperator _projectionOperator;
  protected final Map<String, DataSource> _dataSourceMap;
  protected final Map<ExpressionContext, TransformFunction> _transformFunctionMap = new HashMap<>();

  /**
   * Constructor for the class
   *
   * @param projectionOperator Projection operator
   * @param expressions Collection of expressions to evaluate
   */
  public TransformOperator(ProjectionOperator projectionOperator, Collection<ExpressionContext> expressions) {
    _projectionOperator = projectionOperator;
    _dataSourceMap = projectionOperator.getDataSourceMap();
    for (ExpressionContext expression : expressions) {
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
      _transformFunctionMap.put(expression, transformFunction);
    }
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(getExplainPlanName()).append("(transformFuncs:");
    ExpressionContext[] functions = (ExpressionContext[]) _transformFunctionMap.keySet().toArray();

    if (functions != null && functions.length > 0) {
      stringBuilder.append(functions[0].toString());
      for (int i = 1; i < functions.length; i++) {
        stringBuilder.append(", ").append(functions[i].toString());
      }
    }

    return stringBuilder.append(')').toString();
  }

  /**
   * Returns the number of columns projected.
   *
   * @return Number of columns projected
   */
  public int getNumColumnsProjected() {
    return _dataSourceMap.size();
  }

  /**
   * Returns the transform result metadata associated with the given expression.
   *
   * @param expression Expression
   * @return Transform result metadata
   */
  public TransformResultMetadata getResultMetadata(ExpressionContext expression) {
    return _transformFunctionMap.get(expression).getResultMetadata();
  }

  /**
   * Returns the dictionary associated with the given expression if the transform result is dictionary-encoded, or
   * {@code null} if not.
   *
   * @return Dictionary
   */
  public Dictionary getDictionary(ExpressionContext expression) {
    return _transformFunctionMap.get(expression).getDictionary();
  }

  @Override
  protected TransformBlock getNextBlock() {
    ProjectionBlock projectionBlock = _projectionOperator.nextBlock();
    if (projectionBlock == null) {
      return null;
    } else {
      return new TransformBlock(projectionBlock, _transformFunctionMap);
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public String getExplainPlanName() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_projectionOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _projectionOperator.getExecutionStatistics();
  }
}
