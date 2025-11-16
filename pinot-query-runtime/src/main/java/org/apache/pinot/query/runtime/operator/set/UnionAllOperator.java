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
package org.apache.pinot.query.runtime.operator.set;

import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Union operator for UNION ALL queries. Each child operator is fully drained sequentially and all rows are returned.
 */
public class UnionAllOperator extends SetOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnionAllOperator.class);
  private static final String EXPLAIN_NAME = "UNION_ALL";

  private MseBlock _eosBlock = null;
  private int _currentOperatorIndex = 0;

  public UnionAllOperator(OpChainExecutionContext opChainExecutionContext, List<MultiStageOperator> inputOperators,
      DataSchema dataSchema) {
    super(opChainExecutionContext, inputOperators, dataSchema);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public Type getOperatorType() {
    return Type.UNION;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock getNextBlock()
      throws Exception {
    if (_eosBlock != null) {
      return _eosBlock;
    }

    while (_currentOperatorIndex < _inputOperators.size()) {
      MultiStageOperator currentOperator = _inputOperators.get(_currentOperatorIndex);
      MseBlock block = currentOperator.nextBlock();
      if (block.isError()) {
        _eosBlock = block;
        return block;
      } else if (block.isSuccess()) {
        _currentOperatorIndex++;
        if (_currentOperatorIndex == _inputOperators.size()) {
          _eosBlock = block;
          return block;
        }
      } else if (block.isData()) {
        return block;
      }
    }

    // All input operators are exhausted, return EoS block.
    assert _eosBlock != null;
    return _eosBlock;
  }
}
