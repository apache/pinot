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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A [SortOperator] that buffers every input row and sorts once.
///
/// This is the fallback, chosen only when the input is not already ordered *and* nothing bounds the result - no
/// `fetch`, and a broker response limit of [Integer#MAX_VALUE]. It is the only [SortOperator] whose memory is
/// proportional to the input.
///
/// A single sort of a list beats feeding an unbounded [java.util.PriorityQueue], which is what this operator
/// replaced: the heap pays `O(n log n)` sifting on the way in and again on the way out, and cannot exploit
/// pre-sorted input at all. It is faster on every shape measured, and it allocates more, because the merge buffer
/// costs what the heap does not. `BenchmarkMseSortImplementations` in `pinot-perf` measures both.
public class FullSortOperator extends SortOperator {
  private static final String EXPLAIN_NAME = "SORT_FULL";
  private static final Logger LOGGER = LoggerFactory.getLogger(FullSortOperator.class);

  private final Comparator<Object[]> _comparator;
  private final int _maxInitialCapacity;
  /// The buffered input. Handed downstream as a sublist view of itself, so releasing has to drop the reference
  /// rather than empty the list.
  @Nullable
  private ArrayList<Object[]> _rows;
  /// Retains the first non-empty block until the next block determines whether to size for one block or the full
  /// holder capacity.
  @Nullable
  private List<Object[]> _firstInputRows;

  FullSortOperator(OpChainExecutionContext context, MultiStageOperator input, DataSchema dataSchema, int offset,
      int numRowsToKeep, int maxRowsPerBlock, List<RelFieldCollation> collations, int defaultHolderCapacity) {
    super(context, input, dataSchema, offset, numRowsToKeep, maxRowsPerBlock, true);
    _comparator = new SortUtils.SortComparator(collations, false);
    _maxInitialCapacity = Math.min(defaultHolderCapacity, numRowsToKeep);
    _rows = new ArrayList<>();
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
  protected void releaseBuffers() {
    super.releaseBuffers();
    _rows = null;
    _firstInputRows = null;
  }

  @Override
  protected boolean hasBufferedState() {
    return super.hasBufferedState() || _rows != null || _firstInputRows != null;
  }

  @Override
  protected MseBlock produceNextBlock() {
    assert _rows != null : "Rows must not be released while the operator is still producing";
    MseBlock block = _input.nextBlock();
    while (block.isData()) {
      List<Object[]> inputRows = ((MseBlock.Data) block).asRowHeap().getRows();
      if (!inputRows.isEmpty()) {
        if (_rows.isEmpty() && _firstInputRows == null) {
          _firstInputRows = inputRows;
        } else {
          if (_firstInputRows != null) {
            _rows.ensureCapacity(_maxInitialCapacity);
            _rows.addAll(_firstInputRows);
            _firstInputRows = null;
          }
          _rows.addAll(inputRows);
        }
      }
      checkTerminationAndSampleUsage();
      block = _input.nextBlock();
    }
    _eosBlock = (MseBlock.Eos) block;
    if (_eosBlock.isError()) {
      return _eosBlock;
    }
    if (_firstInputRows != null) {
      _rows = new ArrayList<>(_firstInputRows);
      _firstInputRows = null;
    }
    if (_rows.size() <= _offset) {
      return _eosBlock;
    }
    _rows.sort(_comparator);
    // _numRowsToKeep is Integer.MAX_VALUE whenever this operator is chosen, so the bound below never truncates today.
    // It is applied anyway so that the contract of the three implementations stays identical.
    return emit(_rows.subList(_offset, Math.min(_rows.size(), _numRowsToKeep)));
  }
}
