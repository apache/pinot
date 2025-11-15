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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.DocIdOrderedOperator.DocIdOrder;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.request.context.ArrayJoinContext;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Placeholder operator for ARRAY JOIN queries. At this stage the operator simply delegates to the child projection
 * operator so that future work can plug in the row-multiplication logic without touching the plan builder.
 */
public class ArrayJoinOperator extends BaseProjectOperator<ValueBlock> {
  private static final String EXPLAIN_NAME = "ARRAY_JOIN";

  private final QueryContext _queryContext;
  private final List<ArrayJoinContext> _arrayJoinContexts;
  private final BaseProjectOperator<? extends ValueBlock> _inputOperator;

  public ArrayJoinOperator(QueryContext queryContext, BaseProjectOperator<? extends ValueBlock> inputOperator) {
    _queryContext = queryContext;
    _arrayJoinContexts = queryContext.getArrayJoinContexts();
    _inputOperator = inputOperator;
  }

  @Override
  protected ValueBlock getNextBlock() {
    // TODO: Implement the actual ARRAY JOIN row expansion logic.
    return _inputOperator.nextBlock();
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_inputOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _inputOperator.getExecutionStatistics();
  }

  @Override
  public boolean isCompatibleWith(DocIdOrder order) {
    return _inputOperator.isCompatibleWith(order);
  }

  @Override
  public Map<String, ColumnContext> getSourceColumnContextMap() {
    return _inputOperator.getSourceColumnContextMap();
  }

  @Override
  public ColumnContext getResultColumnContext(ExpressionContext expression) {
    return _inputOperator.getResultColumnContext(expression);
  }

  @Override
  public BaseProjectOperator<ValueBlock> withOrder(DocIdOrder newOrder) {
    BaseProjectOperator<? extends ValueBlock> reordered = _inputOperator.withOrder(newOrder);
    if (reordered == _inputOperator) {
      return this;
    }
    return new ArrayJoinOperator(_queryContext, reordered);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  public List<ArrayJoinContext> getArrayJoinContexts() {
    return _arrayJoinContexts;
  }
}
