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

import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
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
  protected TransferableBlock getNextBlock() {
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    }
    List<MultiStageOperator> childOperators = getChildOperators();
    for (int i = _finishedChildren; i < childOperators.size(); i++) {
      MultiStageOperator upstreamOperator = childOperators.get(i);
      TransferableBlock block = upstreamOperator.nextBlock();
      if (block.isDataBlock()) {
        return block;
      } else if (block.isSuccessfulEndOfStreamBlock()) {
        _finishedChildren++;
        consumeEos(block);
      } else {
        assert block.isErrorBlock();
        _upstreamErrorBlock = block;
        return block;
      }
    }
    assert _queryStats != null : "Should have at least one EOS block from the upstream operators";
    addStats(_queryStats, _statMap);
    return TransferableBlockUtils.getEndOfStreamTransferableBlock(_queryStats);
  }

  private void consumeEos(TransferableBlock block) {
    MultiStageQueryStats queryStats = block.getQueryStats();
    assert queryStats != null;
    if (_queryStats == null) {
      Preconditions.checkArgument(queryStats.getCurrentStageId() == _context.getStageId(),
          "The current stage id of the stats holder: %s does not match the current stage id: %s",
          queryStats.getCurrentStageId(), _context.getStageId());
      _queryStats = queryStats;
    } else {
      _queryStats.mergeUpstream(queryStats);
      _queryStats.getCurrentStats().concat(queryStats.getCurrentStats());
    }
  }

  @Override
  protected boolean handleRowMatched(Object[] row) {
    throw new UnsupportedOperationException("Union operator does not support row matching");
  }
}
