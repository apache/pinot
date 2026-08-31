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

import com.google.common.base.Joiner;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A [SortOperator] that only applies `offset` and `fetch`, because no ordering work is needed: either the input is
/// already ordered on the collation, or the [org.apache.pinot.query.planner.plannode.SortNode] carries no collation at
/// all (a plain `LIMIT`).
///
/// Nothing is sorted and nothing is buffered. Input blocks are forwarded as they arrive, with the first `offset` rows
/// skipped and at most `fetch` rows emitted in total, after which the input is early-terminated. This is the only
/// [SortOperator] that does not break the pipeline: a consumer sees the first rows before the input has finished.
public class LimitSortOperator extends SortOperator {
  private static final String EXPLAIN_NAME = "SORT_LIMIT";
  private static final Logger LOGGER = LoggerFactory.getLogger(LimitSortOperator.class);

  /// Rows still to be skipped to honor `offset`.
  private int _rowsToSkip;
  /// Rows still to be emitted to honor `fetch`. Once zero, remaining input blocks are discarded while waiting for the
  /// terminal block.
  private int _rowsToEmit;

  LimitSortOperator(OpChainExecutionContext context, MultiStageOperator input, DataSchema dataSchema, int offset,
      int numRowsToKeep, int maxRowsPerBlock) {
    super(context, input, dataSchema, offset, numRowsToKeep, maxRowsPerBlock, false);
    _rowsToSkip = offset;
    _rowsToEmit = Math.max(numRowsToKeep - offset, 0);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock produceNextBlock() {
    while (true) {
      MseBlock block = _input.nextBlock();
      if (!block.isData()) {
        _eosBlock = (MseBlock.Eos) block;
        return _eosBlock;
      }
      if (_rowsToEmit == 0) {
        // Already satisfied the fetch. The input has been early-terminated, so just discard whatever is still in
        // flight until it hands back the terminal block.
        continue;
      }
      List<Object[]> rows = ((MseBlock.Data) block).asRowHeap().getRows();
      if (_rowsToSkip > 0) {
        int skipped = Math.min(_rowsToSkip, rows.size());
        _rowsToSkip -= skipped;
        if (skipped == rows.size()) {
          continue;
        }
        rows = rows.subList(skipped, rows.size());
      }
      if (rows.size() >= _rowsToEmit) {
        rows = rows.subList(0, _rowsToEmit);
        _rowsToEmit = 0;
        if (LOGGER.isDebugEnabled()) {
          // This operatorId is an old name. It is being kept to avoid breaking changes on the log message.
          String operatorId =
              Joiner.on("_").join(SortOperator.class.getSimpleName(), _context.getStageId(), _context.getServer());
          LOGGER.debug("Early terminate at SortOperator - operatorId={}, opChainId={}", operatorId, _context.getId());
        }
        // Set the input to be early terminated and await its EOS block.
        earlyTerminate();
      } else {
        _rowsToEmit -= rows.size();
      }
      if (rows.isEmpty()) {
        continue;
      }
      return emit(rows);
    }
  }
}
