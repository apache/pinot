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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Union operator for UNION ALL queries.
 */
public class UnionOperator extends SetOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnionOperator.class);
  private static final String EXPLAIN_NAME = "UNION";
  @Nullable
  private MultiStageQueryStats _queryStats = null;
  private int _finishedChildren = 0;

  public UnionOperator(OpChainExecutionContext opChainExecutionContext, List<MultiStageOperator> inputOperators,
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
  protected MseBlock getNextBlock() {
    if (_eos != null) {
      return _eos;
    }
    List<MultiStageOperator> childOperators = getChildOperators();
    for (int i = _finishedChildren; i < childOperators.size(); i++) {
      MultiStageOperator upstreamOperator = childOperators.get(i);
      MseBlock block = upstreamOperator.nextBlock();
      if (block.isData()) {
        return block;
      }
      MseBlock.Eos eosBlock = (MseBlock.Eos) block;
      if (eosBlock.isSuccess()) {
        _finishedChildren++;
      } else {
        _eos = eosBlock;
        return block;
      }
    }
    return SuccessMseBlock.INSTANCE;
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  @Override
  protected boolean handleRowMatched(Object[] row) {
    throw new UnsupportedOperationException("Union operator does not support row matching");
  }
}
