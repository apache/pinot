/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.transform;

import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.ProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.operator.transform.function.TransformFunction;
import com.linkedin.pinot.core.operator.transform.function.TransformFunctionFactory;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Class for evaluating transform expressions.
 */
public class TransformOperator extends BaseOperator<TransformBlock> {
  private static final String OPERATOR_NAME = "TransformOperator";

  private final ProjectionOperator _projectionOperator;
  private final Map<String, DataSource> _dataSourceMap;
  private final Map<TransformExpressionTree, TransformFunction> _transformFunctionMap = new HashMap<>();

  /**
   * Constructor for the class
   *
   * @param projectionOperator Projection operator
   * @param expressionTrees Set of expression trees to evaluate
   */
  public TransformOperator(@Nonnull ProjectionOperator projectionOperator,
      @Nonnull Set<TransformExpressionTree> expressionTrees) {
    _projectionOperator = projectionOperator;
    _dataSourceMap = projectionOperator.getDataSourceMap();
    for (TransformExpressionTree expressionTree : expressionTrees) {
      TransformFunction transformFunction = TransformFunctionFactory.get(expressionTree, _dataSourceMap);
      _transformFunctionMap.put(expressionTree, transformFunction);
    }
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
   * Returns the data source metadata associated with the given expression tree.
   *
   * @param expressionTree Expression tree
   * @return Data source metadata
   */
  public DataSourceMetadata getDataSourceMetadata(@Nonnull TransformExpressionTree expressionTree) {
    return _transformFunctionMap.get(expressionTree).getResultMetadata();
  }

  /**
   * Returns the dictionary associated with the given expression tree.
   * <p>Should be called only if {@link #getDataSourceMetadata(TransformExpressionTree)} indicates that the data source
   * has dictionary.
   *
   * @return Dictionary
   */
  public Dictionary getDictionary(@Nonnull TransformExpressionTree expressionTree) {
    return _transformFunctionMap.get(expressionTree).getDictionary();
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
  public ExecutionStatistics getExecutionStatistics() {
    return _projectionOperator.getExecutionStatistics();
  }
}
