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
package org.apache.pinot.query.runtime.operator;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// An operator that emits an error block with a specific error code and message.
///
/// This operator is not produced by any planner rule but instead when we find an error and it is too late to throw an
/// exception. This for example happens when servers receive a query in
/// [org.apache.pinot.query.planner.plannode.PlanNode] and for whatever reason (ie uses a function that is unknown for
/// the server) it cannot be transformed into an actual operator. At that time it is too late to throw an exception
/// because the server dispatcher is async and the only way to communicate the error is through the mailbox mechanism.
/// Therefore we create an `ErrorOperator` that emits an error block with the error code and message, aborting the query
/// execution in a controlled way.
public class ErrorOperator extends MultiStageOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ErrorOperator.class);
  private static final String EXPLAIN_NAME = "ERROR";
  private final QueryErrorCode _errorCode;
  private final String _errorMessage;
  private final StatMap<LiteralValueOperator.StatKey> _statMap = new StatMap<>(LiteralValueOperator.StatKey.class);
  private final List<MultiStageOperator> _childOperators;


  public ErrorOperator(OpChainExecutionContext context, QueryErrorCode errorCode,
      String errorMessage) {
    this(context, errorCode, errorMessage, List.of());
  }

  public ErrorOperator(OpChainExecutionContext context, QueryErrorCode errorCode,
      String errorMessage, @Nullable MultiStageOperator childOperator) {
    this(context, errorCode, errorMessage, childOperator == null ? List.of() : List.of(childOperator));
  }

  public ErrorOperator(OpChainExecutionContext context, QueryErrorCode errorCode,
      String errorMessage, List<MultiStageOperator> childOperators) {
    super(context);
    _errorCode = errorCode;
    _errorMessage = errorMessage;
    _childOperators = childOperators;
  }

  @Override
  protected MseBlock getNextBlock() {
    return ErrorMseBlock.fromError(_errorCode, _errorMessage);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(LiteralValueOperator.StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(LiteralValueOperator.StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  public Type getOperatorType() {
    return Type.LITERAL;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return _childOperators;
  }

  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }
}
