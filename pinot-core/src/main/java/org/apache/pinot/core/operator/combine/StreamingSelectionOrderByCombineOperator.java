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
package org.apache.pinot.core.operator.combine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.StreamingSelectionOrderByOperator;
import org.apache.pinot.core.operator.streaming.BaseStreamingCombineOperator;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming, lazy combine operator for selection ORDER BY queries whose first order-by expression is an identifier.
 *
 * <p>It performs an incremental k-way heap merge across the per-segment operators in {@code _operators}, returning
 * globally sorted rows in bounded blocks. Each segment is exposed through a {@link SegmentCursor} that yields that
 * segment's locally-sorted rows in order:
 * <ul>
 *   <li>Segments physically sorted on the first order-by column are backed by
 *   {@link StreamingSelectionOrderByOperator}, which is pulled lazily one run/block at a time.</li>
 *   <li>Other (e.g. consuming/unsorted) segments are backed by a single materialized top-K block (any
 *   {@link SelectionResultsBlock}-producing operator such as {@code SelectionOrderByOperator}); the cursor reads that
 *   one block and iterates its rows.</li>
 * </ul>
 *
 * <p>A {@link PriorityQueue} of {@link SegmentCursor} ordered by the {@link OrderByComparatorFactory} comparator on
 * each
 * cursor's current head row drives the merge with an at-most-one-head-per-active-segment invariant (the heap holds the
 * cursors themselves, never all rows, which would degenerate into a full heap-sort that materializes everything). Each
 * cycle pops the global-min cursor, appends its head to the current output block, advances that one cursor by a single
 * row, and re-offers it if it still has a head.
 *
 * <p><b>Min/max lazy segment activation (pruning).</b> Cursors are sorted by the first order-by column's min value
 * (ASC) / max value (DESC) reusing the {@code MinMaxValueContext} idea from
 * {@link MinMaxValueBasedSelectionOrderByCombineOperator}. A cursor is only activated (its segment acquired and first
 * block read) when the merge frontier reaches its min/max, so once {@code limit + offset} rows are emitted the
 * remaining segments are never acquired or read. See {@link #activateEligibleCursors()} for the correctness argument.
 * Pruning is disabled when null handling is enabled (an unsorted segment's first order-by column may then contain nulls
 * whose ordering position the raw min/max cannot capture), in which case every segment is activated.
 *
 * <p><b>Segment acquire/release lifecycle.</b> A cursor acquires its
 * {@link AcquireReleaseColumnsSegmentOperator} on activation and releases it only when its child operator is fully
 * drained (acquire-on-activate / release-on-exhaust), rather than per run. This is intentional: the backing
 * {@link StreamingSelectionOrderByOperator} retains a buffer-backed {@code ValueBlock} across {@code nextBlock()} calls
 * in its tail-to-sort mode, so releasing between interleaved runs could read segment buffers after a release under
 * prefetch. Holding the acquire for the cursor's lifetime guarantees no release happens between a cursor's own reads;
 * min/max pruning bounds the number of simultaneously-active (acquired) segments to the merge frontier. The rows handed
 * out by the child operators are already deep-copied to heap {@code Object[]} (via {@code RowBasedBlockValueFetcher}),
 * so they remain valid after the segment is released. Any cursors still acquired when the merge ends early (LIMIT
 * reached) or errors out are released via {@link #releaseAllCursors()}.
 *
 * <p><b>Streaming vs single-block.</b> When {@code _streaming} is {@code true} (MSE leaf path, driven by
 * {@link org.apache.pinot.core.operator.streaming.StreamingInstanceResponseOperator}) the merge emits many bounded
 * {@link SelectionResultsBlock}s from successive {@link #getNextBlock()} calls followed by a final
 * {@link MetadataResultsBlock}. When {@code false} (classic single-stage path) the merge runs to completion and the
 * first {@link #getNextBlock()} call returns a single block with execution stats attached.
 *
 * <p><b>Threading.</b> This operator overrides {@link #start()}/{@link #stop()} to no-ops (other than releasing
 * segments) and runs the merge single-threaded and lazily in {@link #getNextBlock()} on the consumer thread; it does
 * not use the base worker-queue model, and {@link #processSegments()} is overridden to fail loud. The base
 * {@code Phaser} (which exists only to fence worker threads against segment release) is intentionally bypassed because
 * all child/segment access is synchronous on the single consumer thread that holds the segment references; no async
 * work may be introduced here without restoring that fence. The instance is single-use (driven once to completion) and
 * is not thread-safe.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamingSelectionOrderByCombineOperator extends BaseStreamingCombineOperator<SelectionResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingSelectionOrderByCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_SELECT_ORDERBY_STREAMING";

  private final boolean _streaming;
  private final boolean _asc;
  private final boolean _pruningEnabled;
  private final int _numRowsToKeep;
  private final int _blockSize;
  private final Comparator<Object[]> _comparator;
  private final SegmentCursor[] _sortedCursors;
  private final PriorityQueue<SegmentCursor> _priorityQueue;

  // Merge progress (single-threaded; mutated only by the consumer thread driving getNextBlock())
  private int _nextToActivate;
  private int _numRowsEmitted;
  private List<Object[]> _outputRows;
  private boolean _done;
  // Captured from the first child block seen; all child blocks share the same schema
  private DataSchema _dataSchema;
  // Deduplicated MERGE_RESPONSE errors for segment blocks dropped on schema mismatch; null until the first mismatch
  @Nullable
  private Set<String> _dataSchemaMismatchErrors;
  // Subset of the above not yet attached to an emitted block; drained on each attach
  @Nullable
  private List<String> _unreportedDataSchemaMismatchErrors;

  public StreamingSelectionOrderByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, boolean streaming) {
    // Pass a null merger: we override the consumption path entirely and never touch the base merger / worker queue.
    super(null, operators, queryContext, executorService);
    _streaming = streaming;
    _numRowsToKeep = queryContext.getLimit() + queryContext.getOffset();
    // Streaming mode flushes bounded blocks; single-stage mode flushes once at the end as a single block.
    _blockSize = streaming ? queryContext.getSortedSelectionMergeBlockSize() : Integer.MAX_VALUE;
    _pruningEnabled = !queryContext.isNullHandlingEnabled();

    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    assert orderByExpressions != null && !orderByExpressions.isEmpty();
    OrderByExpressionContext firstOrderByExpression = orderByExpressions.get(0);
    assert firstOrderByExpression.getExpression().getType() == ExpressionContext.Type.IDENTIFIER;
    _asc = firstOrderByExpression.isAsc();
    String firstOrderByColumn = firstOrderByExpression.getExpression().getIdentifier();
    _comparator = OrderByComparatorFactory.getComparator(orderByExpressions, queryContext.isNullHandlingEnabled());

    // Build one cursor per segment operator and read its first order-by column min/max for lazy activation ordering.
    // Reading DataSourceMetadata does not touch column buffers, so no segment acquire is needed here (mirrors
    // MinMaxValueBasedSelectionOrderByCombineOperator).
    _sortedCursors = new SegmentCursor[_numOperators];
    for (int i = 0; i < _numOperators; i++) {
      Operator<BaseResultsBlock> operator = _operators.get(i);
      DataSourceMetadata metadata =
          operator.getIndexSegment().getDataSource(firstOrderByColumn, queryContext.getSchema())
              .getDataSourceMetadata();
      _sortedCursors[i] = new SegmentCursor(operator, metadata.getMinValue(), metadata.getMaxValue());
    }
    sortCursorsByMinMax();

    _priorityQueue = new PriorityQueue<>(Math.max(1, _numOperators),
        (o1, o2) -> _comparator.compare(o1.currentHead(), o2.currentHead()));
    _outputRows = newOutputList();
  }

  /**
   * Sorts the cursors so the merge can activate them lazily in frontier order: ascending by the column min value for
   * ASC, descending by the column max value for DESC. Cursors without a min/max are placed first because they must
   * always be processed (mirrors {@link MinMaxValueBasedSelectionOrderByCombineOperator}).
   */
  private void sortCursorsByMinMax() {
    if (_asc) {
      Arrays.sort(_sortedCursors, (o1, o2) -> {
        if (o1._minValue == null) {
          return o2._minValue == null ? 0 : -1;
        }
        if (o2._minValue == null) {
          return 1;
        }
        return o1._minValue.compareTo(o2._minValue);
      });
    } else {
      Arrays.sort(_sortedCursors, (o1, o2) -> {
        if (o1._maxValue == null) {
          return o2._maxValue == null ? 0 : -1;
        }
        if (o2._maxValue == null) {
          return 1;
        }
        return o2._maxValue.compareTo(o1._maxValue);
      });
    }
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /// Override to a no-op: the merge is single-threaded and lazy in {@link #getNextBlock()}, so we do not spin up the
  /// base worker threads / blocking-queue model.
  @Override
  public void start() {
  }

  /// Override the base worker-queue stop: no worker threads / phaser tasks were started. Release any segments still
  /// acquired (idempotent) so an early stop by the driver cannot leak acquires.
  @Override
  public void stop() {
    _done = true;
    releaseAllCursors();
  }

  /// The base worker-thread entry point must never run here ({@link #start()} is a no-op). Fail loud if it ever does.
  @Override
  protected void processSegments() {
    throw new IllegalStateException(
        "StreamingSelectionOrderByCombineOperator runs single-threaded; processSegments() must not be called");
  }

  @Override
  protected BaseResultsBlock getNextBlock() {
    if (_done) {
      // Streaming mode: terminal metadata block after the last data block. Idempotent if called again.
      return attachExecutionStats(new MetadataResultsBlock());
    }
    try {
      long endTimeMs = _queryContext.getEndTimeMs();
      while (_numRowsEmitted < _numRowsToKeep) {
        // The merge drains the heap on this thread and, for single-block cursors, may never re-enter a child operator.
        // Without this check nothing would observe cancellation, pause, or the query deadline for up to
        // (limit + offset) iterations -- a regression against MinMaxValueBasedSelectionOrderByCombineOperator, which
        // this operator replaces on the same query shape.
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numRowsEmitted, EXPLAIN_NAME, endTimeMs);
        activateEligibleCursors();
        SegmentCursor cursor = _priorityQueue.poll();
        if (cursor == null) {
          // All active cursors exhausted (and pruning guarantees the rest cannot contribute).
          break;
        }
        _outputRows.add(cursor.currentHead());
        _numRowsEmitted++;
        cursor.advance();
        if (cursor.currentHead() != null) {
          _priorityQueue.offer(cursor);
        }
        if (_streaming && _outputRows.size() >= _blockSize) {
          return flushDataBlock();
        }
      }
      // Merge complete.
      finish();
      if (!_outputRows.isEmpty()) {
        // Streaming: the final partial data block (next call returns the terminal metadata block).
        // Single-stage: the single complete block.
        return flushDataBlock();
      }
      if (_streaming) {
        return attachDataSchemaMismatchErrors(attachExecutionStats(new MetadataResultsBlock()));
      }
      // Single-stage with no rows: still return a (single) block carrying the schema and execution stats.
      return attachDataSchemaMismatchErrors(attachExecutionStats(
          new SelectionResultsBlock(resolveDataSchema(), List.of(), _comparator, _queryContext)));
    } catch (Exception e) {
      _done = true;
      releaseAllCursors();
      return createExceptionResultsBlockAndAttachExecutionStats(e, "merging sorted selection results");
    } catch (Throwable t) {
      // An Error (e.g. OutOfMemoryError while accumulating output rows) must not leave segments acquired for the
      // lifetime of the server. In single-stage mode there is no start()/stop() backstop around this operator.
      _done = true;
      releaseAllCursors();
      throw t;
    }
  }

  /**
   * Activates not-yet-active cursors whose min/max value can still contribute before the current merge frontier.
   *
   * <p>Cursors are visited in min/max-sorted order and {@code _nextToActivate} is advanced only when a cursor is
   * actually activated; a {@code break} merely defers the current cursor, which is re-evaluated against the (rising)
   * frontier on every subsequent call. Correctness: a not-yet-activated cursor whose first order-by value range starts
   * strictly beyond the current heap head cannot contain a row that sorts before that head (the first order-by column
   * is the primary sort key), so the head is the true global minimum and is safe to emit; the deferred cursor is
   * activated later, exactly when the frontier reaches its min/max. When the heap is empty the frontier is unknown, so
   * activation is forced (never pruned), which also drains any segments that sort entirely after the ones seen so far.
   */
  private void activateEligibleCursors() {
    while (_nextToActivate < _sortedCursors.length) {
      SegmentCursor cursor = _sortedCursors[_nextToActivate];
      if (_pruningEnabled) {
        Comparable bound = _asc ? cursor._minValue : cursor._maxValue;
        // A null bound means the segment must always be processed. Otherwise, only prune against a non-null frontier;
        // if the head's first order-by value is null we cannot compare, so fall through and activate.
        if (bound != null && !_priorityQueue.isEmpty()) {
          Object headValue = _priorityQueue.peek().currentHead()[0];
          if (headValue != null) {
            // Both come from the same first order-by column: the metadata min/max and the materialized row[0] share
            // the column's stored type, so this comparison is type-safe (same assumption as MinMaxValueBased...).
            int cmp = bound.compareTo(headValue);
            if (_asc ? cmp > 0 : cmp < 0) {
              break;
            }
          }
        }
      }
      cursor.activate();
      _nextToActivate++;
      if (cursor.currentHead() != null) {
        _priorityQueue.offer(cursor);
      }
    }
  }

  /** Marks the merge done and releases any segments still held by un-drained cursors (e.g. when LIMIT is reached). */
  private void finish() {
    _done = true;
    releaseAllCursors();
  }

  private void releaseAllCursors() {
    for (SegmentCursor cursor : _sortedCursors) {
      cursor.release();
    }
  }

  /**
   * Returns the accumulated output rows as a sorted {@link SelectionResultsBlock} and resets the output buffer. The
   * block carries the comparator so the broker-side n-way reduce stays correct. In single-stage mode it is the only
   * block, so execution stats are attached; in streaming mode stats are attached to the terminal metadata block.
   */
  private BaseResultsBlock flushDataBlock() {
    List<Object[]> rows = _outputRows;
    _outputRows = newOutputList();
    SelectionResultsBlock block = new SelectionResultsBlock(resolveDataSchema(), rows, _comparator, _queryContext);
    attachDataSchemaMismatchErrors(block);
    return _streaming ? block : attachExecutionStats(block);
  }

  /**
   * Records that a segment's block was dropped because its schema disagreed with the merge schema. Deduplicated by
   * message so a mid-reload table with many divergent segments cannot flood the response.
   */
  private void recordDataSchemaMismatch(@Nullable DataSchema mismatched) {
    String errorMessage =
        String.format("Data schema mismatch between merged block: %s and block to merge: %s, drop block to merge",
            _dataSchema, mismatched);
    // NOTE: This is segment level log, so log at debug level to prevent flooding the log.
    LOGGER.debug(errorMessage);
    if (_dataSchemaMismatchErrors == null) {
      _dataSchemaMismatchErrors = new HashSet<>();
      _unreportedDataSchemaMismatchErrors = new ArrayList<>();
    }
    if (_dataSchemaMismatchErrors.add(errorMessage)) {
      _unreportedDataSchemaMismatchErrors.add(errorMessage);
    }
  }

  /**
   * Attaches mismatch errors recorded since the last emitted block. In streaming mode blocks are emitted many times,
   * so errors are drained rather than re-attached, and the terminal block picks up any recorded after the last flush.
   */
  private <T extends BaseResultsBlock> T attachDataSchemaMismatchErrors(T block) {
    if (_unreportedDataSchemaMismatchErrors != null && !_unreportedDataSchemaMismatchErrors.isEmpty()) {
      for (String errorMessage : _unreportedDataSchemaMismatchErrors) {
        block.addErrorMessage(QueryErrorMessage.safeMsg(QueryErrorCode.MERGE_RESPONSE, errorMessage));
      }
      _unreportedDataSchemaMismatchErrors.clear();
    }
    return block;
  }

  private List<Object[]> newOutputList() {
    int capacity =
        Math.min(_blockSize, Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY));
    return new ArrayList<>(Math.max(1, capacity));
  }

  /**
   * Returns the data schema captured from the first child block. If no segment produced a block (every segment is a
   * streaming operator that matched zero rows), reconstructs the schema from the first segment so that an empty result
   * still carries a valid, correctly-ordered schema (order-by expressions first, matching the child blocks' layout).
   */
  private DataSchema resolveDataSchema() {
    if (_dataSchema != null) {
      return _dataSchema;
    }
    IndexSegment indexSegment = _operators.get(0).getIndexSegment();
    List<ExpressionContext> expressions = SelectionOperatorUtils.extractExpressions(_queryContext, indexSegment);
    Set<String> columns = new HashSet<>();
    for (ExpressionContext expression : expressions) {
      expression.getColumns(columns);
    }
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    for (String column : columns) {
      dataSourceMap.put(column, indexSegment.getDataSource(column, _queryContext.getSchema()));
    }
    int numExpressions = expressions.size();
    String[] columnNames = new String[numExpressions];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      ExpressionContext expression = expressions.get(i);
      columnNames[i] = expression.toString();
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, dataSourceMap);
      columnDataTypes[i] = ColumnDataType.fromDataType(transformFunction.getResultMetadata().getDataType(),
          transformFunction.getResultMetadata().isSingleValue());
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);
    return _dataSchema;
  }

  /**
   * Iterates a single segment's locally-sorted rows, pulling blocks lazily from its operator. Streaming-backed cursors
   * loop until the operator returns {@code null}; single-block-backed cursors read exactly one block. The segment is
   * acquired on activation and released once exhausted or when the combine finishes (see class Javadoc).
   */
  private class SegmentCursor {
    private final Operator<BaseResultsBlock> _operator;
    @Nullable
    private final Comparable _minValue;
    @Nullable
    private final Comparable _maxValue;

    private boolean _streamingChild;
    private List<Object[]> _rows;
    private int _pos;
    private boolean _acquired;
    // Cached current head (rows.get(pos)) so the hot heap comparator does not re-index per comparison
    @Nullable
    private Object[] _head;

    SegmentCursor(Operator<BaseResultsBlock> operator, @Nullable Comparable minValue, @Nullable Comparable maxValue) {
      _operator = operator;
      _minValue = minValue;
      _maxValue = maxValue;
    }

    /** Returns the current head row to be merged next, or {@code null} if not activated or exhausted. */
    @Nullable
    Object[] currentHead() {
      return _head;
    }

    /**
     * Acquires the segment, resolves whether the child is the lazy streaming operator, and reads the first block. After
     * this call {@link #currentHead()} returns the first row, or {@code null} if the segment contributes nothing.
     */
    void activate() {
      acquireSegment();
      _streamingChild = isStreamingChild();
      if (!pullBlock()) {
        exhaust();
      }
    }

    /**
     * Advances past the current head, pulling the next block lazily for streaming cursors. Releases the segment when
     * the
     * cursor is exhausted; afterwards {@link #currentHead()} returns {@code null}.
     */
    void advance() {
      _pos++;
      if (_pos < _rows.size()) {
        _head = _rows.get(_pos);
        return;
      }
      // Current block drained: streaming cursors pull the next run/block; single-block cursors are done.
      if (_streamingChild && pullBlock()) {
        return;
      }
      exhaust();
    }

    /**
     * Loads the next non-empty block of rows from the operator, capturing the combine-level data schema on first sight.
     * Returns {@code false} when the operator is exhausted (no more rows). Streaming-backed operators emit one
     * run/block per call and {@code null} when done; single-block operators emit a single block and must not be called
     * again afterwards, so an empty/null block from a single-block child is treated as exhausted.
     *
     * <p>A block whose schema differs from the one already captured is dropped and reported, mirroring
     * {@link org.apache.pinot.core.operator.combine.merger.SelectionOrderByResultsBlockMerger}. Segments on a server
     * can disagree on schema mid-reload (a newly added column exists only in reloaded segments), and merging rows of
     * differing width under one schema would corrupt the result rather than fail.
     */
    private boolean pullBlock() {
      while (true) {
        SelectionResultsBlock block = nextBlock();
        if (block == null) {
          return false;
        }
        if (_dataSchema == null) {
          _dataSchema = block.getDataSchema();
        } else if (!_dataSchema.equals(block.getDataSchema())) {
          recordDataSchemaMismatch(block.getDataSchema());
          return false;
        }
        List<Object[]> rows = block.getRows();
        if (rows != null && !rows.isEmpty()) {
          _rows = rows;
          _pos = 0;
          _head = rows.get(0);
          return true;
        }
        // Defensive: an unexpected empty (non-null) block. Keep pulling only for streaming children; a single-block
        // child yields exactly one block, so treat it as exhausted.
        if (!_streamingChild) {
          return false;
        }
      }
    }

    private SelectionResultsBlock nextBlock() {
      try {
        return (SelectionResultsBlock) _operator.nextBlock();
      } catch (RuntimeException e) {
        throw wrapOperatorException(_operator, e);
      }
    }

    /**
     * Returns whether the underlying child operator is the lazy {@link StreamingSelectionOrderByOperator}. Must be
     * called after {@link #acquireSegment()} because materializing the wrapped child runs the plan node, which accesses
     * segment buffers.
     */
    private boolean isStreamingChild() {
      Operator underlying = _operator;
      if (_operator instanceof AcquireReleaseColumnsSegmentOperator) {
        AcquireReleaseColumnsSegmentOperator wrapper = (AcquireReleaseColumnsSegmentOperator) _operator;
        wrapper.materializeChildOperator();
        underlying = wrapper.getChildOperators().get(0);
      }
      return underlying instanceof StreamingSelectionOrderByOperator;
    }

    private void acquireSegment() {
      if (_operator instanceof AcquireReleaseColumnsSegmentOperator) {
        ((AcquireReleaseColumnsSegmentOperator) _operator).acquire();
      }
      _acquired = true;
    }

    /** Releases the segment if still held. Idempotent: safe to call from {@link #exhaust()} and combine cleanup. */
    private void release() {
      if (_acquired) {
        if (_operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) _operator).release();
        }
        _acquired = false;
      }
    }

    private void exhaust() {
      release();
      _rows = null;
      _head = null;
    }
  }
}
