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
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.trace.Tracing;


/**
 * Class for evaluating transform expressions.
 */
public class TransformOperator extends BaseProjectOperator<TransformBlock> {
  private static final String EXPLAIN_NAME = "TRANSFORM";

  private final BaseProjectOperator<?> _projectOperator;
  private final Map<ExpressionContext, TransformFunction> _transformFunctionMap;

  public TransformOperator(QueryContext queryContext, BaseProjectOperator<?> projectOperator,
      Collection<ExpressionContext> expressions) {
    _projectOperator = projectOperator;
    _transformFunctionMap = new HashMap<>(HashUtil.getHashMapCapacity(expressions.size()));
    for (ExpressionContext expression : expressions) {
      TransformFunction transformFunction =
          TransformFunctionFactory.get(expression, projectOperator.getSourceColumnContextMap(), queryContext);
      _transformFunctionMap.put(expression, transformFunction);
    }
  }

  @Override
  public Map<String, ColumnContext> getSourceColumnContextMap() {
    return _projectOperator.getSourceColumnContextMap();
  }

  @Override
  public ColumnContext getResultColumnContext(ExpressionContext expression) {
    return ColumnContext.fromTransformFunction(_transformFunctionMap.get(expression));
  }

  @Override
  protected TransformBlock getNextBlock() {
    ValueBlock sourceBlock = _projectOperator.nextBlock();
    if (sourceBlock == null) {
      return null;
    }
    Tracing.activeRecording().setNumChildren(_transformFunctionMap.size());
    return new TransformBlock(sourceBlock, _transformFunctionMap);
  }

  @Override
  public String toExplainString() {
    List<String> expressions =
        _transformFunctionMap.keySet().stream().map(ExpressionContext::toString).sorted().collect(Collectors.toList());
    return getExplainName() + "(" + StringUtils.join(expressions, ", ") + ")";
  }

  protected String getExplainName() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<BaseProjectOperator<?>> getChildOperators() {
    return Collections.singletonList(_projectOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _projectOperator.getExecutionStatistics();
  }
}
