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

import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.transform.TransformExpressionTree;


/**
 * Base implementation of {@link AggregationFunction} with single input expression.
 */
public abstract class BaseSingleInputAggregationFunction<I, F extends Comparable> implements AggregationFunction<I, F> {
  protected final String _column;
  protected final TransformExpressionTree _expression;

  /**
   * Constructor for the class.
   *
   * @param column Column to aggregate on (could be column name or transform function).
   */
  public BaseSingleInputAggregationFunction(String column) {
    _column = column;
    _expression = TransformExpressionTree.compileToExpressionTree(column);
  }

  @Override
  public String getColumnName() {
    return getType().getName() + "_" + _column;
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _column + ")";
  }

  @Override
  public List<TransformExpressionTree> getInputExpressions() {
    return Collections.singletonList(_expression);
  }
}
