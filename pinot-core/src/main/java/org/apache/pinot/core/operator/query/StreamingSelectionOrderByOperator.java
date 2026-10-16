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
package org.apache.pinot.core.operator.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.BitmapDocIdSetOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.ProjectionOperatorUtils;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.roaringbitmap.RoaringBitmap;


/// Lazy, incremental selection ORDER BY operator for segments that are physically sorted on the first order-by column.
///
/// Unlike [SelectionOrderByOperator] (which materializes the segment's whole top-K in a single block) this
/// operator emits one globally-sorted [SelectionResultsBlock] per [#getNextBlock()] call and returns
/// `null` when the segment is exhausted, so that a downstream k-way-merge combine operator can pull from many
/// segments lazily and stop early. It relies on the underlying project operator iterating the first order-by column in
/// the query order (the caller must guarantee `projectOperator.isCompatibleWith(DocIdOrder.fromAsc(asc))`).
///
/// Every instance emits at least one block before `null`: a segment matching no rows emits a single empty block
/// carrying the [DataSchema], so consumers never have to reconstruct a schema the segment already knows.
///
/// It runs in one of two emission modes:
///
/// - **No tail to sort** (`numSortedExpressions == numOrderByExpressions`, e.g. `ORDER BY sorted`):
///   rows already arrive from the project operator in final order, so each call emits the next project block
///   (trimmed to the remaining `limit + offset` budget).
/// - **Tail to sort** (`numSortedExpressions < numOrderByExpressions`, e.g.
///   `ORDER BY sorted, other`):
///   each call reads forward until a value of the sorted prefix changes (a "run": rows that tie on every sorted
///   order-by column), retains the run's top rows by the unsorted tail, as many as the remaining `limit + offset`
///   budget allows, and emits them sorted. This bounds the in-memory run buffer to `limit + offset` rows even when
///   the sorted prefix is near-constant (very low cardinality).
///
/// Like [SelectionOrderByOperator] it preserves the two-phase projection optimization: when there are output
/// expressions that are not order-by expressions, the forward scan only fetches the order-by expressions plus the
/// document id, and the non-order-by expressions are fetched in a second pass over the retained document ids of each
/// emitted block.
///
/// This operator is stateful across [#getNextBlock()] calls and is **not** thread-safe; a single consumer
/// must drive it.
public class StreamingSelectionOrderByOperator extends BaseOperator<SelectionResultsBlock> {
  private static final String EXPLAIN_NAME = "SELECT_ORDERBY_STREAMING";

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final boolean _nullHandlingEnabled;
  /// Deduped order-by expressions followed by output expressions from SelectionOperatorUtils.extractExpressions()
  private final List<ExpressionContext> _expressions;
  private final BaseProjectOperator<?> _projectOperator;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final ColumnContext[] _orderByColumnContexts;
  private final int _numExpressions;
  private final int _numOrderByExpressions;
  private final int _numRowsToKeep;
  /// Whether there are output expressions that are not order-by expressions (requires the two-phase fetch)
  private final boolean _twoPhase;
  /// Whether the order-by has an unsorted tail that must be sorted in memory per run
  private final boolean _tailToSort;
  /// Expressions fetched during the forward scan: order-by expressions only when two-phase, otherwise all expressions
  private final List<ExpressionContext> _phase1Expressions;
  private final int _numPhase1Columns;
  private final Comparator<Object[]> _comparator;
  /// Compares the sorted prefix of the order-by; used to detect run boundaries. Null without a tail to sort.
  @Nullable
  private final Comparator<Object[]> _sortedPrefixComparator;
  /// Orders the run heap by the unsorted suffix, reversed; null without a tail to sort
  @Nullable
  private final Comparator<Object[]> _reversedComparator;

  // Pre-computed invariants for the two-phase fetch (null when single-phase)
  private final List<ExpressionContext> _nonOrderByExpressions;
  private final Map<String, DataSource> _phase2DataSourceMap;
  private final int _phase2NumColumns;

  /// Lazily built and cached; for two-phase it requires the transform operator's result column contexts
  private DataSchema _dataSchema;
  /// Run heap, allocated on the first [#nextRun()] and then cleared and reused for every run. Not allocated in the
  /// constructor: it starts with `min(limit + offset, 10,000)` slots, and without prefetch every segment's leaf is
  /// built before the combine decides which of them to pull.
  private PriorityQueue<Object[]> _runHeap;
  /// Source column contexts for the phase-2 expressions; built only on the zero-match schema path (null otherwise)
  private Map<String, ColumnContext> _phase2ColumnContextMap;

  // Forward-scan cursor state (used by the tail-to-sort mode)
  private ValueBlock _currentBlock;
  private RowBasedBlockValueFetcher _currentFetcher;
  /// Copy of the current block's doc ids, owned by this operator. The block's own array is a scratch buffer that the
  /// doc-id-set operators share across every segment on the thread, and the streaming combine pulls other segments on
  /// this thread between two [#nextRun()] calls, so it may be overwritten before the rest of the block is read.
  private int[] _currentDocIds;
  private RoaringBitmap[] _currentNullBitmaps;
  private int _currentNumDocs;
  private int _currentPos;
  /// One-row lookahead: the first row of the next run, stashed when a run boundary is crossed
  private Object[] _pendingRow;
  private boolean _projectExhausted;

  private boolean _exhausted;
  /// Whether any block has been emitted yet; drives the zero-match segment's one schema-carrying empty block
  private boolean _emittedAnyBlock;
  private int _numRowsEmitted;
  private int _numDocsScanned = 0;
  private long _numEntriesScannedPostFilter = 0;

  public StreamingSelectionOrderByOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, BaseProjectOperator<?> projectOperator, int numSortedExpressions) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _nullHandlingEnabled = queryContext.isNullHandlingEnabled();
    _expressions = expressions;
    _projectOperator = projectOperator;

    _orderByExpressions = queryContext.getOrderByExpressions();
    assert _orderByExpressions != null;
    _numExpressions = expressions.size();
    _numOrderByExpressions = _orderByExpressions.size();
    _orderByColumnContexts = new ColumnContext[_numOrderByExpressions];
    for (int i = 0; i < _numOrderByExpressions; i++) {
      ExpressionContext expression = _orderByExpressions.get(i).getExpression();
      _orderByColumnContexts[i] = _projectOperator.getResultColumnContext(expression);
    }

    _numRowsToKeep = queryContext.getOffset() + queryContext.getLimit();
    _twoPhase = _numExpressions > _numOrderByExpressions;
    _tailToSort = numSortedExpressions < _numOrderByExpressions;
    _comparator =
        OrderByComparatorFactory.getComparator(_orderByExpressions, _orderByColumnContexts, _nullHandlingEnabled);
    if (_tailToSort) {
      // Every column of the sorted prefix is physically sorted in the scan order (and, with null handling, has no
      // nulls), so a run ends as soon as any of them changes. Rows within a run tie on the whole prefix, so the run
      // heap only needs to compare the unsorted suffix.
      assert numSortedExpressions > 0 : "The streaming operator requires a sorted first order-by column";
      _sortedPrefixComparator = OrderByComparatorFactory.getComparator(_orderByExpressions, _orderByColumnContexts,
          _nullHandlingEnabled, 0, numSortedExpressions);
      _reversedComparator = OrderByComparatorFactory.getComparator(_orderByExpressions, _orderByColumnContexts,
          _nullHandlingEnabled, numSortedExpressions, _numOrderByExpressions).reversed();
    } else {
      _sortedPrefixComparator = null;
      _reversedComparator = null;
    }

    if (_twoPhase) {
      _phase1Expressions = new ArrayList<>(_numOrderByExpressions);
      for (OrderByExpressionContext orderByExpression : _orderByExpressions) {
        _phase1Expressions.add(orderByExpression.getExpression());
      }
      _nonOrderByExpressions = _expressions.subList(_numOrderByExpressions, _numExpressions);
      Set<String> columns = new HashSet<>();
      for (ExpressionContext expressionContext : _nonOrderByExpressions) {
        expressionContext.getColumns(columns);
      }
      _phase2NumColumns = columns.size();
      _phase2DataSourceMap = new HashMap<>();
      for (String column : columns) {
        _phase2DataSourceMap.put(column, _indexSegment.getDataSource(column, _queryContext.getSchema()));
      }
    } else {
      _phase1Expressions = _expressions;
      _nonOrderByExpressions = null;
      _phase2NumColumns = 0;
      _phase2DataSourceMap = null;
      // Single-phase: all output expressions are order-by expressions, so their types are known up front.
      _dataSchema = buildSinglePhaseDataSchema();
    }
    _numPhase1Columns = _phase1Expressions.size();
  }

  @Override
  protected SelectionResultsBlock getNextBlock() {
    if (_exhausted) {
      return null;
    }
    List<Object[]> rows = _tailToSort ? nextRun() : nextSortedRows();
    if (rows == null || rows.isEmpty()) {
      _exhausted = true;
      releaseScanState();
      if (_emittedAnyBlock) {
        return null;
      }
      // Zero-match segment: emit one empty block carrying the schema before signalling end-of-stream, so consumers
      // never have to reconstruct a schema the segment already knows. Two-phase leaves _dataSchema null until the
      // first fetch, which never happens here, so build it from column metadata alone.
      if (_twoPhase && _dataSchema == null) {
        _dataSchema = buildTwoPhaseDataSchema(this::resolveResultColumnContext);
      }
      assert _dataSchema != null;
      return new SelectionResultsBlock(_dataSchema, List.of(), _comparator, _queryContext);
    }
    _emittedAnyBlock = true;
    if (_twoPhase) {
      fetchNonOrderByColumns(rows);
    }
    // Single-phase builds the schema in the constructor; two-phase builds it during fetchNonOrderByColumns above.
    assert _dataSchema != null;
    return new SelectionResultsBlock(_dataSchema, rows, _comparator, _queryContext);
  }

  /// Drops the forward-scan state once the leaf is exhausted. The streaming combine keeps a drained cursor's operator
  /// until the query ends, so without this an exhausted leaf would keep its run heap and doc-id copy reachable for the
  /// rest of the query. The current block's values live in the projection operator's cache, which this leaf still
  /// references, so they are not freed here.
  private void releaseScanState() {
    _currentBlock = null;
    _currentFetcher = null;
    _currentDocIds = null;
    _currentNullBitmaps = null;
    _pendingRow = null;
    _runHeap = null;
  }

  /// No-tail-to-sort mode: the project operator already returns rows in final order, so emit the next project block,
  /// trimmed to the remaining `limit + offset` budget. Returns `null` when exhausted, and otherwise a
  /// non-empty list -- empty project blocks are skipped and the row budget is checked up front, so "no rows" always
  /// means "no more rows".
  @Nullable
  private List<Object[]> nextSortedRows() {
    int remaining = _numRowsToKeep - _numRowsEmitted;
    if (remaining <= 0) {
      return null;
    }
    ValueBlock valueBlock = nextNonEmptyBlock();
    if (valueBlock == null) {
      return null;
    }
    int numDocsFetched = valueBlock.getNumDocs();
    BlockValSet[] blockValSets = new BlockValSet[_numPhase1Columns];
    for (int i = 0; i < _numPhase1Columns; i++) {
      blockValSets[i] = valueBlock.getBlockValueSet(_phase1Expressions.get(i));
    }
    RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);
    int[] docIds = _twoPhase ? valueBlock.getDocIds() : null;
    RoaringBitmap[] nullBitmaps = null;
    if (_nullHandlingEnabled) {
      nullBitmaps = new RoaringBitmap[_numPhase1Columns];
      for (int i = 0; i < _numPhase1Columns; i++) {
        nullBitmaps[i] = blockValSets[i].getNullBitmap();
      }
    }
    _numDocsScanned += numDocsFetched;
    _numEntriesScannedPostFilter += (long) numDocsFetched * _projectOperator.getNumColumnsProjected();
    reportScanCost(numDocsFetched, (long) numDocsFetched * _projectOperator.getNumColumnsProjected());

    // Rows arrive sorted; we only need the first 'remaining' of them globally.
    int numRows = Math.min(numDocsFetched, remaining);
    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      rows.add(materializeRow(blockValueFetcher, docIds, nullBitmaps, i));
    }
    _numRowsEmitted += rows.size();
    return rows;
  }

  /// Tail-to-sort mode: read forward until a value of the sorted prefix changes, retain the run's top `remaining` rows
  /// by the unsorted tail, and return them sorted. Returns `null` when exhausted.
  ///
  /// A segment never contributes more than `limit + offset` rows to the global result, and they are a prefix of its
  /// local sorted order, so a run may only add the smallest `remaining` of its rows. Bounding the heap by `remaining`
  /// keeps exactly those, and no more than will be emitted is sorted or kept reachable. The whole run is still scanned:
  /// the tail is not sorted within a run, so any unread row could still displace one in the heap.
  @Nullable
  private List<Object[]> nextRun() {
    int remaining = _numRowsToKeep - _numRowsEmitted;
    if (remaining <= 0) {
      return null;
    }
    if (_pendingRow == null) {
      _pendingRow = nextRow();
      if (_pendingRow == null) {
        return null;
      }
    }
    PriorityQueue<Object[]> runHeap = _runHeap;
    if (runHeap == null) {
      runHeap = new PriorityQueue<>(
          Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY), _reversedComparator);
      _runHeap = runHeap;
    } else {
      runHeap.clear();
    }
    Object[] runFirstRow = _pendingRow;
    SelectionOperatorUtils.addToPriorityQueue(_pendingRow, runHeap, remaining);
    _pendingRow = null;
    Object[] row;
    while ((row = nextRow()) != null) {
      if (_sortedPrefixComparator.compare(row, runFirstRow) == 0) {
        SelectionOperatorUtils.addToPriorityQueue(row, runHeap, remaining);
      } else {
        // Run boundary: this row starts the next run, keep it for the next call.
        _pendingRow = row;
        break;
      }
    }
    List<Object[]> rows = drainAscending(runHeap);
    assert rows.size() <= remaining;
    _numRowsEmitted += rows.size();
    return rows;
  }

  /// Pulls the next row of the forward scan (across project blocks), materialized as an
  /// `Object[_numExpressions]`. For two-phase the document id is stashed at index
  /// `_numOrderByExpressions` (overwritten in the second pass). Returns `null` when the project operator
  /// is exhausted.
  @Nullable
  private Object[] nextRow() {
    if (_currentBlock == null || _currentPos >= _currentNumDocs) {
      if (_projectExhausted) {
        return null;
      }
      _currentBlock = nextNonEmptyBlock();
      if (_currentBlock == null) {
        _projectExhausted = true;
        return null;
      }
      BlockValSet[] blockValSets = new BlockValSet[_numPhase1Columns];
      for (int i = 0; i < _numPhase1Columns; i++) {
        blockValSets[i] = _currentBlock.getBlockValueSet(_phase1Expressions.get(i));
      }
      _currentFetcher = new RowBasedBlockValueFetcher(blockValSets);
      _currentNumDocs = _currentBlock.getNumDocs();
      if (_twoPhase) {
        if (_currentDocIds == null || _currentDocIds.length < _currentNumDocs) {
          _currentDocIds = new int[_currentNumDocs];
        }
        System.arraycopy(_currentBlock.getDocIds(), 0, _currentDocIds, 0, _currentNumDocs);
      }
      if (_nullHandlingEnabled) {
        _currentNullBitmaps = new RoaringBitmap[_numPhase1Columns];
        for (int i = 0; i < _numPhase1Columns; i++) {
          _currentNullBitmaps[i] = blockValSets[i].getNullBitmap();
        }
      }
      _currentPos = 0;
      _numDocsScanned += _currentNumDocs;
      _numEntriesScannedPostFilter += (long) _currentNumDocs * _projectOperator.getNumColumnsProjected();
      reportScanCost(_currentNumDocs, (long) _currentNumDocs * _projectOperator.getNumColumnsProjected());
    }
    int rowId = _currentPos++;
    return materializeRow(_currentFetcher, _currentDocIds, _currentNullBitmaps, rowId);
  }

  /// Pulls the next project block carrying documents, skipping any that carry none. Returns `null` only when
  /// the project operator is exhausted; an empty block means "nothing in this batch", not "end of segment", and
  /// treating one as exhaustion would truncate the scan.
  ///
  /// No reachable doc-id-set operator emits an empty block today, so this normally returns on its first pull. The
  /// skip stays because that is a convention of those implementations, not something
  /// [org.apache.pinot.core.operator.BaseProjectOperator] enforces.
  @Nullable
  private ValueBlock nextNonEmptyBlock() {
    while (true) {
      ValueBlock valueBlock = _projectOperator.nextBlock();
      if (valueBlock == null) {
        return null;
      }
      if (valueBlock.getNumDocs() > 0) {
        return valueBlock;
      }
    }
  }

  /// Materializes a single phase-1 row (deep-copied out of the value block buffers) from the given fetcher.
  private Object[] materializeRow(RowBasedBlockValueFetcher fetcher, @Nullable int[] docIds,
      @Nullable RoaringBitmap[] nullBitmaps, int rowId) {
    Object[] row = new Object[_numExpressions];
    fetcher.getRow(rowId, row, 0);
    if (_twoPhase) {
      row[_numOrderByExpressions] = docIds[rowId];
    }
    if (_nullHandlingEnabled) {
      for (int colId = 0; colId < _numPhase1Columns; colId++) {
        if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId)) {
          row[colId] = null;
        }
      }
    }
    return row;
  }

  /// Drains a max-heap (created with the reversed comparator) into an ascending list. The second pass fills
  /// non-order-by values in place, so the list must at least be settable.
  ///
  /// It is a growable `ArrayList` rather than a fixed-size view for defensive reasons: `SelectionResultsBlock` is a
  /// shared type, and `SelectionOperatorUtils.mergeWithoutOrdering()` structurally adds to the row list of the block
  /// it merges into. No current consumer of this operator takes that path - the streaming combine only reads the
  /// rows - so this is hardening against a future one, not a fix for a reachable failure.
  private List<Object[]> drainAscending(PriorityQueue<Object[]> heap) {
    int numRows = heap.size();
    // One exactly-sized backing array and no intermediate copy: drain in the heap's own (descending) order, then
    // flip in place.
    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      rows.add(heap.poll());
    }
    Collections.reverse(rows);
    return rows;
  }

  /// Second pass of the two-phase fetch: fills the non-order-by expression values for the rows of a single emitted
  /// block.
  /// The rows keep their final (comparator) order; the fill iterates a document-id-sorted view that shares the same row
  /// instances, mirroring [SelectionOrderByOperator#computePartiallyOrdered()].
  private void fetchNonOrderByColumns(List<Object[]> rows) {
    int numRows = rows.size();
    RoaringBitmap docIds = new RoaringBitmap();
    for (Object[] row : rows) {
      docIds.add((int) row[_numOrderByExpressions]);
    }
    // Document-id-sorted view sharing the same row instances (the bitmap returns docIds in ascending order).
    List<Object[]> rowsByDocId = new ArrayList<>(rows);
    rowsByDocId.sort(Comparator.comparingInt(o -> (int) o[_numOrderByExpressions]));

    BitmapDocIdSetOperator docIdOperator = BitmapDocIdSetOperator.ascending(docIds, numRows);
    try (ProjectionOperator projectionOperator =
        ProjectionOperatorUtils.getProjectionOperator(_phase2DataSourceMap, docIdOperator, _queryContext)) {
      TransformOperator transformOperator =
          new TransformOperator(_queryContext, projectionOperator, _nonOrderByExpressions);

      int numNonOrderByExpressions = _nonOrderByExpressions.size();
      BlockValSet[] blockValSets = new BlockValSet[numNonOrderByExpressions];
      int rowBaseId = 0;
      ValueBlock valueBlock;
      while ((valueBlock = transformOperator.nextBlock()) != null) {
        for (int i = 0; i < numNonOrderByExpressions; i++) {
          blockValSets[i] = valueBlock.getBlockValueSet(_nonOrderByExpressions.get(i));
        }
        RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);
        int numDocsFetched = valueBlock.getNumDocs();
        for (int i = 0; i < numDocsFetched; i++) {
          blockValueFetcher.getRow(i, rowsByDocId.get(rowBaseId + i), _numOrderByExpressions);
        }
        if (_nullHandlingEnabled) {
          RoaringBitmap[] nullBitmaps = new RoaringBitmap[numNonOrderByExpressions];
          for (int i = 0; i < numNonOrderByExpressions; i++) {
            nullBitmaps[i] = blockValSets[i].getNullBitmap();
          }
          for (int i = 0; i < numDocsFetched; i++) {
            Object[] values = rowsByDocId.get(rowBaseId + i);
            for (int colId = 0; colId < numNonOrderByExpressions; colId++) {
              if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(i)) {
                values[_numOrderByExpressions + colId] = null;
              }
            }
          }
        }
        _numEntriesScannedPostFilter += (long) numDocsFetched * _phase2NumColumns;
        // Phase 2 re-reads docs already counted in phase 1, so only the extra entries are reported.
        reportScanCost(0, (long) numDocsFetched * _phase2NumColumns);
        rowBaseId += numDocsFetched;
      }

      if (_dataSchema == null) {
        _dataSchema = buildTwoPhaseDataSchema(transformOperator::getResultColumnContext);
      }
    }
  }

  private DataSchema buildSinglePhaseDataSchema() {
    String[] columnNames = new String[_numExpressions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[_numExpressions];
    for (int i = 0; i < _numExpressions; i++) {
      columnNames[i] = _expressions.get(i).toString();
      columnDataTypes[i] = DataSchema.ColumnDataType.fromDataType(_orderByColumnContexts[i].getDataType(),
          _orderByColumnContexts[i].isSingleValue());
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  /// Resolves a non-order-by expression's result type without building the phase-2 pipeline, for the zero-match case
  /// where there is nothing to fetch. This reproduces [TransformOperator#getResultColumnContext] exactly -- that
  /// method resolves against its project operator's source column contexts, which for phase 2 are
  /// [#_phase2DataSourceMap] -- so the schema is identical to the one the fetch path builds, at metadata cost
  /// only. Worth the indirection because a selective filter can leave many segments of a table matching nothing, and
  /// each would otherwise stand up a full projection and transform operator to read types it already knows.
  private ColumnContext resolveResultColumnContext(ExpressionContext expression) {
    if (_phase2ColumnContextMap == null) {
      _phase2ColumnContextMap = new HashMap<>(HashUtil.getHashMapCapacity(_phase2DataSourceMap.size()));
      for (Map.Entry<String, DataSource> entry : _phase2DataSourceMap.entrySet()) {
        _phase2ColumnContextMap.put(entry.getKey(), ColumnContext.fromDataSource(entry.getValue()));
      }
    }
    return ColumnContext.fromTransformFunction(
        TransformFunctionFactory.get(expression, _phase2ColumnContextMap, _queryContext));
  }

  private DataSchema buildTwoPhaseDataSchema(Function<ExpressionContext, ColumnContext> resultColumnContexts) {
    int numNonOrderByExpressions = _nonOrderByExpressions.size();
    String[] columnNames = new String[_numExpressions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[_numExpressions];
    for (int i = 0; i < _numExpressions; i++) {
      columnNames[i] = _expressions.get(i).toString();
    }
    for (int i = 0; i < _numOrderByExpressions; i++) {
      columnDataTypes[i] = DataSchema.ColumnDataType.fromDataType(_orderByColumnContexts[i].getDataType(),
          _orderByColumnContexts[i].isSingleValue());
    }
    for (int i = 0; i < numNonOrderByExpressions; i++) {
      ColumnContext columnContext = resultColumnContexts.apply(_nonOrderByExpressions.get(i));
      columnDataTypes[_numOrderByExpressions + i] =
          DataSchema.ColumnDataType.fromDataType(columnContext.getDataType(), columnContext.isSingleValue());
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  @VisibleForTesting
  boolean isRunHeapAllocated() {
    return _runHeap != null;
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(selectList:");
    if (!_expressions.isEmpty()) {
      stringBuilder.append(_expressions.get(0));
      for (int i = 1; i < _expressions.size(); i++) {
        stringBuilder.append(", ").append(_expressions.get(i));
      }
    }
    return stringBuilder.append(')').toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    if (_expressions.isEmpty()) {
      return;
    }
    attributeBuilder.putStringList("selectList",
        _expressions.stream().map(ExpressionContext::toString).collect(Collectors.toList()));
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_projectOperator);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, _numEntriesScannedPostFilter,
        numTotalDocs);
  }

  /// Reports scan cost to the shared [QueryScanCostContext] so scan-based query killing
  /// ([org.apache.pinot.core.common.Operator#nextBlock()] -> `checkScanBasedKilling`) can see this
  /// operator's work. Without this the killer is blind on the streaming path, unlike every other selection operator.
  private void reportScanCost(int numDocsScanned, long numEntriesScannedPostFilter) {
    QueryScanCostContext scanCost = getScanCostContext();
    if (scanCost != null) {
      if (numDocsScanned > 0) {
        scanCost.addDocsScanned(numDocsScanned);
      }
      if (numEntriesScannedPostFilter > 0) {
        scanCost.addEntriesScannedPostFilter(numEntriesScannedPostFilter);
      }
    }
  }
}
