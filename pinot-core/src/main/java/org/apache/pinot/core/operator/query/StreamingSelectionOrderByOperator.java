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

import com.google.common.base.CaseFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
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
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.RoaringBitmap;


/**
 * Lazy, incremental selection ORDER BY operator for segments that are physically sorted on the first order-by column.
 *
 * <p>Unlike {@link SelectionOrderByOperator} (which materializes the segment's whole top-K in a single block) this
 * operator emits one globally-sorted {@link SelectionResultsBlock} per {@link #getNextBlock()} call and returns
 * {@code null} when the segment is exhausted, so that a downstream k-way-merge combine operator can pull from many
 * segments lazily and stop early. It relies on the underlying project operator iterating the first order-by column in
 * the query order (the caller must guarantee {@code projectOperator.isCompatibleWith(DocIdOrder.fromAsc(asc))}).
 *
 * <p>It runs in one of two emission modes:
 * <ul>
 *   <li><b>No tail to sort</b> ({@code numSortedExpressions == numOrderByExpressions}, e.g. {@code ORDER BY sorted}):
 *   rows already arrive from the project operator in final order, so each call emits the next project block (trimmed to
 *   the remaining {@code limit + offset} budget).</li>
 *   <li><b>Tail to sort</b> ({@code numSortedExpressions < numOrderByExpressions}, e.g.
 *   {@code ORDER BY sorted, other}):
 *   each call reads forward until the first order-by value changes (a primary-value "run"), retains the run's top
 *   {@code limit + offset} rows by the full comparator, and emits them sorted. This bounds the in-memory run buffer to
 *   {@code limit + offset} rows even when the first order-by column is near-constant (very low cardinality).</li>
 * </ul>
 *
 * <p>Like {@link SelectionOrderByOperator} it preserves the two-phase projection optimization: when there are output
 * expressions that are not order-by expressions, the forward scan only fetches the order-by expressions plus the
 * document id, and the non-order-by expressions are fetched in a second pass over the retained document ids of each
 * emitted block.
 *
 * <p>This operator is stateful across {@link #getNextBlock()} calls and is <b>not</b> thread-safe; a single consumer
 * must drive it.
 */
public class StreamingSelectionOrderByOperator extends BaseOperator<SelectionResultsBlock> {
  private static final String EXPLAIN_NAME = "SELECT_ORDERBY_STREAMING";

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final boolean _nullHandlingEnabled;
  // Deduped order-by expressions followed by output expressions from SelectionOperatorUtils.extractExpressions()
  private final List<ExpressionContext> _expressions;
  private final BaseProjectOperator<?> _projectOperator;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final ColumnContext[] _orderByColumnContexts;
  private final int _numExpressions;
  private final int _numOrderByExpressions;
  private final int _numRowsToKeep;
  // Whether there are output expressions that are not order-by expressions (requires the two-phase fetch)
  private final boolean _twoPhase;
  // Whether the order-by has an unsorted tail that must be sorted in memory per run
  private final boolean _tailToSort;
  // Expressions fetched during the forward scan: order-by expressions only when two-phase, otherwise all expressions
  private final List<ExpressionContext> _phase1Expressions;
  private final int _numPhase1Columns;
  private final Comparator<Object[]> _comparator;
  // Compares only the first order-by column; used to detect primary-value run boundaries
  private final Comparator<Object[]> _primaryComparator;
  // Pre-allocated run heap (cleared and reused each nextRun() call to avoid per-run allocation)
  private final Comparator<Object[]> _reversedComparator;
  private final PriorityQueue<Object[]> _runHeap;

  // Pre-computed invariants for the two-phase fetch (null when single-phase)
  private final List<ExpressionContext> _nonOrderByExpressions;
  private final Map<String, DataSource> _phase2DataSourceMap;
  private final int _phase2NumColumns;

  // Lazily built and cached; for two-phase it requires the transform operator's result column contexts
  private DataSchema _dataSchema;

  // Forward-scan cursor state (used by the tail-to-sort mode)
  private ValueBlock _currentBlock;
  private RowBasedBlockValueFetcher _currentFetcher;
  private int[] _currentDocIds;
  private RoaringBitmap[] _currentNullBitmaps;
  private int _currentNumDocs;
  private int _currentPos;
  // One-row lookahead: the first row of the next run, stashed when a run boundary is crossed
  private Object[] _pendingRow;
  private boolean _projectExhausted;

  private boolean _exhausted;
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
    // The first order-by column is the physically sorted column, so it never contains nulls on this path; comparing
    // only index 0 is enough to detect when one primary-value run ends and the next begins.
    _primaryComparator =
        OrderByComparatorFactory.getComparator(_orderByExpressions, _orderByColumnContexts, _nullHandlingEnabled, 0, 1);
    _reversedComparator = _comparator.reversed();
    _runHeap = new PriorityQueue<>(
        Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY), _reversedComparator);

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
      return null;
    }
    if (_twoPhase) {
      fetchNonOrderByColumns(rows);
    }
    // Single-phase builds the schema in the constructor; two-phase builds it during fetchNonOrderByColumns above.
    assert _dataSchema != null;
    return new SelectionResultsBlock(_dataSchema, rows, _comparator, _queryContext);
  }

  /**
   * No-tail-to-sort mode: the project operator already returns rows in final order, so emit the next project block,
   * trimmed to the remaining {@code limit + offset} budget. Returns {@code null} when exhausted.
   */
  @Nullable
  private List<Object[]> nextSortedRows() {
    int remaining = _numRowsToKeep - _numRowsEmitted;
    if (remaining <= 0) {
      return null;
    }
    ValueBlock valueBlock = _projectOperator.nextBlock();
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

    // Rows arrive sorted; we only need the first 'remaining' of them globally.
    int numRows = Math.min(numDocsFetched, remaining);
    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      rows.add(materializeRow(blockValueFetcher, docIds, nullBitmaps, i));
    }
    _numRowsEmitted += rows.size();
    return rows;
  }

  /**
   * Tail-to-sort mode: read forward until the first order-by value changes, retain the run's top {@code limit + offset}
   * rows by the full comparator, and return them sorted. Returns {@code null} when exhausted.
   */
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
    runHeap.clear();
    Object[] runFirstRow = _pendingRow;
    SelectionOperatorUtils.addToPriorityQueue(_pendingRow, runHeap, _numRowsToKeep);
    _pendingRow = null;
    Object[] row;
    while ((row = nextRow()) != null) {
      if (_primaryComparator.compare(row, runFirstRow) == 0) {
        SelectionOperatorUtils.addToPriorityQueue(row, runHeap, _numRowsToKeep);
      } else {
        // Run boundary: this row starts the next run, keep it for the next call.
        _pendingRow = row;
        break;
      }
    }
    List<Object[]> rows = drainAscending(runHeap);
    // A segment never contributes more than 'limit + offset' rows to the global result, and they are a prefix of its
    // local sorted order, so cap the total emitted across runs at the remaining budget (the rows are ascending, keep
    // the smallest 'remaining').
    if (rows.size() > remaining) {
      rows = rows.subList(0, remaining);
    }
    _numRowsEmitted += rows.size();
    return rows;
  }

  /**
   * Pulls the next row of the forward scan (across project blocks), materialized as an {@code Object[_numExpressions]}.
   * For two-phase the document id is stashed at index {@code _numOrderByExpressions} (overwritten in the second pass).
   * Returns {@code null} when the project operator is exhausted.
   */
  @Nullable
  private Object[] nextRow() {
    while (true) {
      if (_currentBlock == null || _currentPos >= _currentNumDocs) {
        if (_projectExhausted) {
          return null;
        }
        _currentBlock = _projectOperator.nextBlock();
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
        _currentDocIds = _twoPhase ? _currentBlock.getDocIds() : null;
        if (_nullHandlingEnabled) {
          _currentNullBitmaps = new RoaringBitmap[_numPhase1Columns];
          for (int i = 0; i < _numPhase1Columns; i++) {
            _currentNullBitmaps[i] = blockValSets[i].getNullBitmap();
          }
        }
        _currentPos = 0;
        _numDocsScanned += _currentNumDocs;
        _numEntriesScannedPostFilter += (long) _currentNumDocs * _projectOperator.getNumColumnsProjected();
        if (_currentNumDocs == 0) {
          _currentBlock = null;
          continue;
        }
      }
      int rowId = _currentPos++;
      return materializeRow(_currentFetcher, _currentDocIds, _currentNullBitmaps, rowId);
    }
  }

  /**
   * Materializes a single phase-1 row (deep-copied out of the value block buffers) from the given fetcher.
   */
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

  /**
   * Drains a max-heap (created with the reversed comparator) into an ascending list, mutable so the second pass can
   * fill non-order-by values in place.
   */
  private List<Object[]> drainAscending(PriorityQueue<Object[]> heap) {
    int numRows = heap.size();
    Object[][] sortedRows = new Object[numRows][];
    for (int i = numRows - 1; i >= 0; i--) {
      sortedRows[i] = heap.poll();
    }
    return Arrays.asList(sortedRows);
  }

  /**
   * Second pass of the two-phase fetch: fills the non-order-by expression values for the rows of a single emitted
   * block.
   * The rows keep their final (comparator) order; the fill iterates a document-id-sorted view that shares the same row
   * instances, mirroring {@link SelectionOrderByOperator#computePartiallyOrdered()}.
   */
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
        rowBaseId += numDocsFetched;
      }

      if (_dataSchema == null) {
        _dataSchema = buildTwoPhaseDataSchema(transformOperator);
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

  private DataSchema buildTwoPhaseDataSchema(TransformOperator transformOperator) {
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
      ColumnContext columnContext = transformOperator.getResultColumnContext(_nonOrderByExpressions.get(i));
      columnDataTypes[_numOrderByExpressions + i] =
          DataSchema.ColumnDataType.fromDataType(columnContext.getDataType(), columnContext.isSingleValue());
    }
    return new DataSchema(columnNames, columnDataTypes);
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
}
