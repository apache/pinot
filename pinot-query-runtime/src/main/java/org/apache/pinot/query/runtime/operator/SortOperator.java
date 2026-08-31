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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants;


/// Base class for the operators that implement a [SortNode].
///
/// A `SortNode` carries a collation, a `fetch` and an `offset`, but the cheapest way to honor them depends on what the
/// input can promise and on whether the fetch bounds the result. Rather than branching inside one operator, the
/// [#create] factory picks one of three implementations, each of which reports itself in the explain plan:
///
///   - [LimitSortOperator] (`SORT_LIMIT`) - there is no collation, so no ordering is required at all. Nothing is
///     sorted and nothing is accumulated: blocks stream through with `offset` rows skipped and at most `fetch` rows
///     emitted.
///   - [TopNSortOperator] (`SORT_TOP_N`) - the result is bounded by `fetch` (or by the broker response limit), so a
///     bounded max-heap of `fetch + offset` entries is enough. Peak memory is the bound, not the input size.
///   - [FullSortOperator] (`SORT_FULL`) - no bound is available, so every row is buffered and sorted once. This is the
///     only implementation whose memory is proportional to the input, and it is chosen only when nothing smaller is
///     correct.
///
/// All three emit their result as a stream of blocks of at most [#DEFAULT_MAX_ROWS_PER_BLOCK] rows. A single block
/// holding the whole result is expensive to serialize and forces the consumer to materialize everything at once, so
/// even the two buffering implementations hand their result back in slices.
///
/// Like every multi-stage operator this class is driven by a single thread and is not thread-safe.
public abstract class SortOperator extends MultiStageOperator {
  /// Upper bound on the number of rows in each emitted block.
  ///
  /// This bounds serialization cost and consumer-side materialization; it does not bound this operator's own memory,
  /// which is decided by the implementation the factory picks.
  protected static final int DEFAULT_MAX_ROWS_PER_BLOCK = 10_000;

  protected final MultiStageOperator _input;
  protected final DataSchema _dataSchema;
  protected final int _offset;
  /// Maximum number of rows to retain before `offset` is applied, i.e. `fetch + offset`, or the broker response limit
  /// when there is no fetch. [Integer#MAX_VALUE] means the result is unbounded.
  protected final int _numRowsToKeep;
  protected final int _maxRowsPerBlock;
  protected final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  /// Set once a terminal block has been read from the input. Returned from [#getNextBlock()] after any pending rows
  /// have been drained.
  @Nullable
  protected MseBlock.Eos _eosBlock;

  /// Result rows that did not fit in the block already returned, together with the index of the first row not yet
  /// emitted. Only used by the implementations that produce their whole result at once.
  @Nullable
  private List<Object[]> _pendingRows;
  private int _pendingIndex;

  protected SortOperator(OpChainExecutionContext context, MultiStageOperator input, DataSchema dataSchema, int offset,
      int numRowsToKeep, int maxRowsPerBlock, boolean requiresSort) {
    super(context);
    _input = input;
    _dataSchema = dataSchema;
    _offset = offset;
    _numRowsToKeep = numRowsToKeep;
    _maxRowsPerBlock = maxRowsPerBlock;
    _statMap.merge(StatKey.REQUIRE_SORT, requiresSort);
  }

  /// Creates the cheapest operator that correctly implements `node` over `input`.
  public static SortOperator create(OpChainExecutionContext context, MultiStageOperator input, SortNode node) {
    return create(context, input, node, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY,
        CommonConstants.Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT, DEFAULT_MAX_ROWS_PER_BLOCK);
  }

  @VisibleForTesting
  static SortOperator create(OpChainExecutionContext context, MultiStageOperator input, SortNode node,
      int defaultHolderCapacity, int defaultResponseLimit) {
    return create(context, input, node, defaultHolderCapacity, defaultResponseLimit, DEFAULT_MAX_ROWS_PER_BLOCK);
  }

  @VisibleForTesting
  static SortOperator create(OpChainExecutionContext context, MultiStageOperator input, SortNode node,
      int defaultHolderCapacity, int defaultResponseLimit, int maxRowsPerBlock) {
    int offset = Math.max(node.getOffset(), 0);
    // Setting numRowsToKeep as default maximum on Broker if limit not set.
    // TODO: make this default behavior configurable.
    int fetch = node.getFetch();
    int numRowsToKeep = fetch > 0 ? fetch + offset : defaultResponseLimit;
    List<RelFieldCollation> collations = node.getCollations();
    DataSchema dataSchema = node.getDataSchema();
    if (collations.isEmpty()) {
      return new LimitSortOperator(context, input, dataSchema, offset, numRowsToKeep, maxRowsPerBlock);
    }
    if (numRowsToKeep == Integer.MAX_VALUE) {
      // Nothing bounds the result, so a bounded heap cannot be used and every row has to be buffered and sorted.
      return new FullSortOperator(context, input, dataSchema, offset, numRowsToKeep, maxRowsPerBlock, collations);
    }
    return new TopNSortOperator(context, input, dataSchema, offset, numRowsToKeep, maxRowsPerBlock, collations,
        defaultHolderCapacity);
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  public Type getOperatorType() {
    return Type.SORT_OR_LIMIT;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_input);
  }

  @Override
  public void cancel(Throwable e) {
  }

  @Override
  public StatMap<StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  @Override
  protected MseBlock getNextBlock() {
    MseBlock pending = nextPendingBlock();
    if (pending != null) {
      return pending;
    }
    if (_eosBlock != null) {
      return _eosBlock;
    }
    return produceNextBlock();
  }

  /// Produces the next block once every previously produced row has been emitted and no terminal block has been
  /// reached yet. Implementations must assign [#_eosBlock] when they read a terminal block from the input, and must
  /// hand result rows back through [#emit] so that block sizes stay bounded.
  protected abstract MseBlock produceNextBlock();

  /// Returns `rows` as a block, retaining anything beyond [#_maxRowsPerBlock] for the following calls.
  ///
  /// `rows` must not be modified afterwards: the returned blocks are views over it.
  protected MseBlock emit(List<Object[]> rows) {
    assert !rows.isEmpty() : "emit() must not be called with an empty row list";
    if (rows.size() <= _maxRowsPerBlock) {
      return new RowHeapDataBlock(rows, _dataSchema);
    }
    _pendingRows = rows;
    _pendingIndex = _maxRowsPerBlock;
    return new RowHeapDataBlock(rows.subList(0, _maxRowsPerBlock), _dataSchema);
  }

  @Nullable
  private MseBlock nextPendingBlock() {
    if (_pendingRows == null) {
      return null;
    }
    int end = Math.min(_pendingIndex + _maxRowsPerBlock, _pendingRows.size());
    List<Object[]> slice = _pendingRows.subList(_pendingIndex, end);
    if (end == _pendingRows.size()) {
      _pendingRows = null;
      _pendingIndex = 0;
    } else {
      _pendingIndex = end;
    }
    return new RowHeapDataBlock(slice, _dataSchema);
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG), REQUIRE_SORT(StatMap.Type.BOOLEAN) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    /// Allocated memory in bytes for this operator or its children in the same stage.
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    /// Time spent on GC while this operator or its children in the same stage were running.
    GC_TIME_MS(StatMap.Type.LONG);

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
