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
package org.apache.pinot.core.operator.streaming;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock.EarlyTerminationReason;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.combine.merger.DistinctResultsBlockMerger;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Streaming combine operator for distinct queries. Instead of accumulating every distinct value into a single
/// [org.apache.pinot.core.query.distinct.table.DistinctTable] before returning (like
/// [org.apache.pinot.core.operator.combine.DistinctCombineOperator]), this operator flushes the accumulated values
/// once the table reaches a configurable threshold.
///
/// This bounds server memory usage for high-cardinality distinct queries on MSE leaf stages, while still performing
/// partial de-duplication to reduce data volume compared to skipping leaf-stage de-duplication entirely (the
/// `is_skip_leaf_stage_group_by` hint).
///
/// The downstream FINAL stage de-duplicates partial results from multiple flushes correctly because:
///
/// - Hash exchange routes the same distinct key to the same FINAL worker (the exchange is keyed on the group keys,
///   which for a zero-aggregate-call aggregate are exactly the distinct columns)
/// - Set union is associative, commutative and idempotent, so a value emitted in more than one flush window is a
///   no-op downstream
///
/// That downstream stage is a precondition, not an implementation detail: a flushed block carries only part of the
/// distinct set, and one value can span several flush windows. Leaves that must return final results are rejected in
/// the constructor as a backstop -- the real gate is in [org.apache.pinot.core.plan.CombinePlanNode], but this
/// operator is public and nothing else would catch a direct construction, which would silently emit duplicate rows.
///
/// Unlike [StreamingGroupByCombineOperator], no `detachFromWorkerThreadState` hook is needed: a per-segment
/// `DistinctTable` is allocated per [org.apache.pinot.core.operator.query.DistinctOperator] invocation and the
/// dictionary-based executors materialize actual values in `getResult()`, so a block handed to the consumer thread
/// is self-contained and references no reused worker-thread-local state.
///
/// Two limits to be aware of when enabling this:
///
/// - It bounds only the server-level accumulated table. The per-segment `DistinctTable`s that feed it are still
///   built in full (unlike group-by, whose per-segment maps are capped by `numGroupsLimit`), so peak leaf heap is
///   roughly `flushThreshold` plus one table per in-flight worker thread. In particular, when a single segment
///   already holds more than `flushThreshold` distinct values, its block is adopted and flushed with nothing merged
///   into it, so the leaf performs no cross-segment de-duplication at all and only amplifies rows. Deliberately not
///   "fixed" by requiring a merge before each flush: that would let the accumulator grow to two segments' worth and
///   raise the very ceiling this operator exists to lower. Bounding the per-segment tables is the real fix.
/// - The distinct guardrails weaken. All three (`maxRowsInDistinct`, `maxRowsWithoutChangeInDistinct`,
///   `maxExecutionTimeMsInDistinct`) are evaluated inside [DistinctResultsBlockMerger#mergeResultsBlocks], which
///   only runs for blocks merged into an existing accumulator -- never for the first block of a flush window, which
///   is adopted. So in the no-merge regime above, none of them can fire at all. When merging does happen,
///   `maxRowsInDistinct` and `maxExecutionTimeMsInDistinct` stay per-query bounds because [#mergeBlock] deliberately
///   carries the scanned-doc count across windows, but `maxRowsWithoutChangeInDistinct` still degrades: its counter
///   only advances when a merge leaves the table's size unchanged, and every window restarts from an empty table.
@SuppressWarnings("rawtypes")
public class StreamingDistinctCombineOperator extends BaseStreamingCombineOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "STREAMING_COMBINE_DISTINCT";

  private final int _flushThreshold;

  // Main-thread-only state for accumulating distinct results
  private DistinctResultsBlock _mergedBlock;
  private long _numDocsScanned;
  private EarlyTerminationReason _earlyTerminationReason = EarlyTerminationReason.NONE;

  public StreamingDistinctCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, int flushThreshold) {
    super(new DistinctResultsBlockMerger(queryContext), operators, queryContext, executorService);
    Preconditions.checkState(
        !queryContext.isServerReturnFinalResult() && !queryContext.isServerReturnFinalResultKeyUnpartitioned(),
        "Streaming distinct combine requires a leaf whose results are de-duplicated by a later stage");
    _flushThreshold = flushThreshold;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /// Disables the worker-side early-termination check, matching [StreamingGroupByCombineOperator].
  ///
  /// [BaseStreamingCombineOperator#processSegments] calls this on the PRODUCING thread with a block it has already
  /// published to the consumer. This operator adopts that same block as its accumulator and mutates it (merging
  /// other tables into its `DistinctTable`, and writing `numDocsScanned` / the early-termination reason), none of
  /// which is synchronized -- so letting the worker read `isSatisfied()` or the termination reason off it afterwards
  /// is a data race on a plain `HashSet` mid-`add`/`rehash`. The sibling streaming operators are each safe for one
  /// of two reasons: the group-by one never lets a worker read a published block, and the selection-only one never
  /// mutates one. This operator would otherwise be the only one doing both.
  ///
  /// Nothing is lost: the consumer still evaluates satisfaction itself in [#mergeBlock] via the results-block
  /// merger, and the cross-segment early exit is already given up in this mode by construction (flushing empties the
  /// accumulator long before it can reach LIMIT -- see the gate in
  /// [org.apache.pinot.core.plan.CombinePlanNode]). Returning `false` also removes the hazard of a worker returning
  /// early without emitting its `LAST_RESULTS_BLOCK`.
  @Override
  protected boolean isQuerySatisfied(DistinctResultsBlock resultsBlock, Object tracker) {
    return false;
  }

  /// Polls per-segment result blocks from worker threads, merges them into the accumulated distinct table, and
  /// flushes when the table reaches the flush threshold. Returns one block per call:
  /// - A DistinctResultsBlock when flushing accumulated values
  /// - A MetadataResultsBlock when all operators are done and remaining data has been flushed
  /// - An ExceptionResultsBlock on error or timeout
  @Override
  protected BaseResultsBlock getNextBlock() {
    long endTimeMs = _queryContext.getEndTimeMs();
    try {
      while (!_querySatisfied && _numOperatorsFinished < _numOperators) {
        QueryThreadContext.checkTermination(this::getExplainName);
        BaseResultsBlock resultsBlock =
            _blockingQueue.poll(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        if (resultsBlock == null) {
          throw QueryErrorCode.EXECUTION_TIMEOUT.asException("Timed out while streaming distinct results");
        }
        if (resultsBlock instanceof ExceptionResultsBlock) {
          return checkTerminateExceptionAndAttachExecutionStats(resultsBlock);
        }
        if (resultsBlock == LAST_RESULTS_BLOCK) {
          _numOperatorsFinished++;
          continue;
        }
        mergeBlock((DistinctResultsBlock) resultsBlock);
        if (_mergedBlock.getDistinctTable().size() >= _flushThreshold) {
          return flush();
        }
      }
    } catch (Exception e) {
      return createExceptionResultsBlockAndAttachExecutionStats(e, "streaming distinct results");
    }
    // All operators done (or the query is satisfied) — flush any remaining accumulated data
    if (_mergedBlock != null && _mergedBlock.getDistinctTable().size() > 0) {
      return flush();
    }
    // Return final metadata block. The early-termination reason is carried over from the accumulated block: the
    // blocking path reports it on the single results block, but here that block has already been streamed out, so
    // without this the reason (and hence BrokerResponse.isPartialResult) would be silently dropped.
    MetadataResultsBlock metadataBlock = new MetadataResultsBlock();
    metadataBlock.setEarlyTerminationReason(_earlyTerminationReason);
    return attachExecutionStats(metadataBlock);
  }

  /// Merges a per-segment block into the accumulated result. The first block of each flush window is adopted as the
  /// accumulator (mirroring [org.apache.pinot.core.operator.combine.BaseSingleBlockCombineOperator#mergeResults]),
  /// which avoids having to construct a `DistinctTable` of the right subtype from a `DataSchema`.
  private void mergeBlock(DistinctResultsBlock blockToMerge) {
    QueryThreadContext.checkTerminationAndSampleUsage(EXPLAIN_NAME);
    if (_mergedBlock == null) {
      _mergedBlock = blockToMerge;
    } else {
      _resultsBlockMerger.mergeResultsBlocks(_mergedBlock, blockToMerge);
    }
    // Carry the running doc count across flush windows so that maxRowsInDistinct stays a per-query bound instead of
    // silently becoming a per-flush-window one. NOTE: This count is not serialized with a data block (the block's
    // DataTable carries only rows), so overwriting it here cannot double count in the response metadata.
    _numDocsScanned += blockToMerge.getNumDocsScanned();
    _mergedBlock.setNumDocsScanned(_numDocsScanned);
    if (_mergedBlock.getEarlyTerminationReason() != EarlyTerminationReason.NONE) {
      _earlyTerminationReason = _mergedBlock.getEarlyTerminationReason();
    }
    _querySatisfied = _resultsBlockMerger.isQuerySatisfied(_mergedBlock);
  }

  private DistinctResultsBlock flush() {
    DistinctResultsBlock block = _mergedBlock;
    _mergedBlock = null;
    return block;
  }
}
