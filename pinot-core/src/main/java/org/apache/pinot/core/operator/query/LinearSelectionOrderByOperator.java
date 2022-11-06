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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.segment.spi.IndexSegment;
import org.roaringbitmap.RoaringBitmap;


/**
 * A selection Operator used when the first expressions in the order by are identifier expressions of columns that are
 * already sorted (either ascendingly or descendingly), even if the tail of order by expressions are not sorted.
 *
 * ie: SELECT ... FROM Table WHERE predicates ORDER BY sorted_column DESC LIMIT 10 OFFSET 5
 * or: SELECT ... FROM Table WHERE predicates ORDER BY sorted_column, not_sorted LIMIT 10 OFFSET 5
 * but not SELECT ... FROM Table WHERE predicates ORDER BY not_sorted, sorted_column LIMIT 10 OFFSET 5
 *
 * Operators that derives from this class are going to have an almost linear cost instead of the usual NlogN when actual
 * sorting must be done, where N is the number of rows in the segment.
 * There is a degraded scenario when the cost is actually NlogL (where L is the limit of the query) when all the first L
 * rows have the exact same value for the prefix of the sorted columns. Even in that case, L should be quite smaller
 * than N, so this implementation is algorithmically better than the normal solution.
 */
public abstract class LinearSelectionOrderByOperator extends BaseOperator<SelectionResultsBlock> {
  protected final IndexSegment _indexSegment;

  protected final boolean _nullHandlingEnabled;
  // Deduped order-by expressions followed by output expressions from SelectionOperatorUtils.extractExpressions()
  protected final List<ExpressionContext> _expressions;
  protected final List<ExpressionContext> _alreadySorted;
  protected final List<ExpressionContext> _toSort;

  protected final TransformOperator _transformOperator;
  protected final List<OrderByExpressionContext> _orderByExpressions;
  protected final TransformResultMetadata[] _expressionsMetadata;
  protected final int _numRowsToKeep;
  private final Supplier<ListBuilder> _listBuilderSupplier;
  protected boolean _used = false;
  /**
   * The comparator used to build the resulting {@link SelectionResultsBlock}, which sorts rows in reverse order to the
   * one specified in the query.
   */
  protected Comparator<Object[]> _comparator;

  /**
   * @param expressions Order-by expressions must be at the head of the list.
   * @param numSortedExpressions Number of expressions in the order-by expressions that are sorted.
   */
  public LinearSelectionOrderByOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, TransformOperator transformOperator, int numSortedExpressions) {
    _indexSegment = indexSegment;
    _nullHandlingEnabled = queryContext.isNullHandlingEnabled();
    _expressions = expressions;
    _transformOperator = transformOperator;

    _orderByExpressions = queryContext.getOrderByExpressions();
    assert _orderByExpressions != null;
    int numOrderByExpressions = _orderByExpressions.size();

    _alreadySorted = expressions.subList(0, numSortedExpressions);
    _toSort = expressions.subList(numSortedExpressions, numOrderByExpressions);

    _expressionsMetadata = new TransformResultMetadata[_expressions.size()];
    for (int i = 0; i < _expressionsMetadata.length; i++) {
      ExpressionContext expression = _expressions.get(i);
      _expressionsMetadata[i] = _transformOperator.getResultMetadata(expression);
    }

    _numRowsToKeep = queryContext.getOffset() + queryContext.getLimit();

    if (_toSort.isEmpty()) {
      _listBuilderSupplier = () -> new TotallySortedListBuilder(_numRowsToKeep);
    } else {
      Comparator<Object[]> sortedComparator =
          OrderByComparatorFactory.getComparator(_orderByExpressions, _expressionsMetadata, false, _nullHandlingEnabled,
              0, numSortedExpressions);
      Comparator<Object[]> unsortedComparator =
          OrderByComparatorFactory.getComparator(_orderByExpressions, _expressionsMetadata, true, _nullHandlingEnabled,
              numSortedExpressions, numOrderByExpressions);
      _listBuilderSupplier = () -> new PartiallySortedListBuilder(_numRowsToKeep, sortedComparator, unsortedComparator);
    }

    _comparator =
        OrderByComparatorFactory.getComparator(_orderByExpressions, _expressionsMetadata, true, _nullHandlingEnabled);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    int numDocsScanned = getNumDocsScanned();
    long numEntriesScannedPostFilter = (long) numDocsScanned * _transformOperator.getNumColumnsProjected();
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        numTotalDocs);
  }

  protected IntFunction<Object[]> fetchBlock(TransformBlock transformBlock, BlockValSet[] blockValSets) {
    int numExpressions = _expressions.size();

    for (int i = 0; i < numExpressions; i++) {
      ExpressionContext expression = _expressions.get(i);
      blockValSets[i] = transformBlock.getBlockValueSet(expression);
    }
    RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);

    if (!_nullHandlingEnabled) {
      return blockValueFetcher::getRow;
    }
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      nullBitmaps[i] = blockValSets[i].getNullBitmap();
    }
    return (docId) -> {
      Object[] row = blockValueFetcher.getRow(docId);
      for (int colId = 0; colId < nullBitmaps.length; colId++) {
        if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(docId)) {
          row[colId] = null;
        }
      }
      return row;
    };
  }

  protected abstract int getNumDocsScanned();

  /**
   * Returns a list of rows sorted that:
   * <ul>
   *   <li>At least contains all the rows that fulfill the predicate</li>
   *   <li>Rows are sorted in a way that is compatible with the given list builder supplier</li>
   * </ul>
   *
   * That means that the result may contain more rows than required.
   *
   * @param listBuilderSupplier a {@link ListBuilder} supplier that should be used to create the result. Each time is
   *                            called a new {@link ListBuilder} will be returned. All returned instances use the same
   *                            comparator logic.
   */
  protected abstract List<Object[]> fetch(Supplier<ListBuilder> listBuilderSupplier);

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_transformOperator);
  }

  protected abstract String getExplainName();

  @Override
  public String toExplainString() {
    StringBuilder sb = new StringBuilder(getExplainName());

    sb.append("(sortedList: ");
    concatList(sb, _alreadySorted);

    sb.append(", unsortedList: ");
    concatList(sb, _toSort);

    sb.append(", rest: ");
    concatList(sb, _expressions.subList(_alreadySorted.size() + _toSort.size(), _expressions.size()));

    sb.append(')');
    return sb.toString();
  }

  private void concatList(StringBuilder sb, List<?> list) {
    sb.append('(');
    Iterator<?> it = list.iterator();
    if (it.hasNext()) {
      sb.append(it.next());
      while (it.hasNext()) {
        sb.append(", ").append(it.next());
      }
    }
    sb.append(')');
  }

  @Override
  protected SelectionResultsBlock getNextBlock() {
    Preconditions.checkState(!_used, "nextBlock was called more than once");
    _used = true;
    List<Object[]> list = fetch(_listBuilderSupplier);

    DataSchema dataSchema = createDataSchema();

    if (list.size() > _numRowsToKeep) {
      list = new ArrayList<>(list.subList(0, _numRowsToKeep));
    }

    return new SelectionResultsBlock(dataSchema, list, _comparator);
  }

  protected DataSchema createDataSchema() {
    int numExpressions = _expressions.size();

    // Create the data schema
    String[] columnNames = new String[numExpressions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numExpressions];
    for (int i = 0; i < columnNames.length; i++) {
      columnNames[i] = _expressions.get(i).toString();
    }
    for (int i = 0; i < numExpressions; i++) {
      TransformResultMetadata expressionMetadata = _expressionsMetadata[i];
      columnDataTypes[i] =
          DataSchema.ColumnDataType.fromDataType(expressionMetadata.getDataType(), expressionMetadata.isSingleValue());
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  /**
   * A private class used to build a partially sorted list by adding partially sorted data.
   *
   * Specifically, this class has been designed to receive successive calls to {@link #add(Object[])} follow by a single
   * call to {@link #build()}.
   *
   * Rows must be inserted in ascending order accordingly to the partial order specified by a comparator.
   * This comparator will define <i>partitions</i> of rows. All the rows in the same partition are considered equal
   * by that comparator.
   *
   * When calling {@link #add(Object[])} with a row that doesn't belong to the current partition, the previous partition
   * is <em>closed</em> and a new one is started.
   */
  protected interface ListBuilder {

    /**
     * Adds the given row to this object. The new column must be equal or higher than previous inserted elements
     * according to the partition comparator. No more rows should be added once enough rows have been collected.
     *
     * @param row The row to add. The values of the already sorted columns must be equal or higher than the last added
     *           row, if any.
     * @return true if and only if enough rows have been collected
     */
    boolean add(Object[] row);

    /**
     * Builds the sorted list. The current partition will be <em>closed</em>. Once this method is called, the builder
     * should not be used.
     */
    List<Object[]> build();
  }

  /**
   * This is the faster {@link ListBuilder} but also the most restrictive one. It can only be used when data is inserted
   * in total order and therefore each element belong to its own partition.
   *
   * This builder cannot be used to implement order-by queries where there is at least one expression
   * that is not sorted. In such case {@link PartiallySortedListBuilder} should be used.
   *
   * This implementation is just a wrapper over an ArrayList and therefore the average costs of its methods is constant.
   */
  @VisibleForTesting
  static class TotallySortedListBuilder implements ListBuilder {
    private final ArrayList<Object[]> _list;
    private final int _maxNumRows;

    public TotallySortedListBuilder(int maxNumRows) {
      _maxNumRows = maxNumRows;
      _list = new ArrayList<>(Integer.min(maxNumRows, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY));
    }

    @Override
    public boolean add(Object[] row) {
      _list.add(row);
      return _list.size() == _maxNumRows;
    }

    @Override
    public List<Object[]> build() {
      return _list;
    }
  }

  /**
   * This {@link ListBuilder} is size bound and requires two comparators: The first defines the partitions and the
   * second defines an order inside each partition.
   *
   * This class does never store more than the requested number of elements. In case more elements are inserted:
   * <ul>
   *   <li>If the new element belongs to a higher partition, it is discarded.</li>
   *   <li>If the new element belongs to the last included partition, the last partition is treated as a priority queue
   *   sorted by the in-partition comparator. If the new element is lower than some of the already inserted elements,
   *   the new replace the older.</li>
   * </ul>
   *
   * This class can be used to implement order-by queries that include one or more not sorted expressions.
   * In cases where all expressions are sorted, {@link TotallySortedListBuilder} should be used because its performance
   * is better.
   *
   * As usual, elements are sorted by partition and there is no order guarantee inside each partition. The second
   * comparator is only used to keep the lower elements in the last partition.
   */
  @VisibleForTesting
  static class PartiallySortedListBuilder implements ListBuilder {
    /**
     * A list with all the elements that have been already sorted.
     */
    private final ArrayList<Object[]> _sorted;
    /**
     * This attribute is used to store the last partition when the builder already contains {@link #_maxNumRows} rows.
     */
    private PriorityQueue<Object[]> _lastPartitionQueue;
    /**
     * The comparator that defines the partitions and the one that impose in which order add has to be called.
     */
    private final Comparator<Object[]> _partitionComparator;
    /**
     * The comparator that sorts different rows on each partition, which sorts rows in reverse order to the one
     * specified in the query.
     */
    private final Comparator<Object[]> _unsortedComparator;

    private final int _maxNumRows;

    private Object[] _lastPartitionRow;
    private int _numSortedRows;

    public PartiallySortedListBuilder(int maxNumRows, Comparator<Object[]> partitionComparator,
        Comparator<Object[]> unsortedComparator) {
      _maxNumRows = maxNumRows;
      _sorted = new ArrayList<>(Integer.min(maxNumRows, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY));
      _partitionComparator = partitionComparator;
      _unsortedComparator = unsortedComparator;
    }

    @Override
    public boolean add(Object[] row) {
      if (_lastPartitionRow == null) {
        _lastPartitionRow = row;
        _sorted.add(row);
        return false;
      }
      int cmp = _partitionComparator.compare(row, _lastPartitionRow);
      if (cmp < 0) {
        throw new IllegalArgumentException(
            "Row with docId " + _sorted.size() + " is not sorted compared to the previous one");
      }

      boolean newPartition = cmp > 0;
      if (_sorted.size() < _maxNumRows) {
        // we don't have enough rows yet
        if (newPartition) {
          _lastPartitionRow = row;
          _numSortedRows = _sorted.size();
        }
        // just add the new row to the result list
        _sorted.add(row);
        return false;
      }

      // enough rows have been collected
      assert _sorted.size() == _maxNumRows;
      if (newPartition) { // and the new element belongs to a new partition, so we can just ignore it
        return true;
      }
      // new element doesn't belong to a new partition, so we may need to add it
      if (_lastPartitionQueue == null) { // we have exactly _numRows rows, and the new belongs to the last partition
        // we need to prepare the priority queue
        int numRowsInPriorityQueue = _maxNumRows - _numSortedRows;
        _lastPartitionQueue = new PriorityQueue<>(numRowsInPriorityQueue, _unsortedComparator);
        _lastPartitionQueue.addAll(_sorted.subList(_numSortedRows, _maxNumRows));
      }
      // add the new element if it is lower than the greatest element stored in the partition
      if (_unsortedComparator.compare(row, _lastPartitionQueue.peek()) > 0) {
        _lastPartitionQueue.poll();
        _lastPartitionQueue.offer(row);
      }
      return false;
    }

    @Override
    public List<Object[]> build() {
      if (_lastPartitionQueue != null) {
        assert _lastPartitionQueue.size() == _maxNumRows - _numSortedRows;
        Iterator<Object[]> lastPartitionIt = _lastPartitionQueue.iterator();
        for (int i = _numSortedRows; i < _maxNumRows; i++) {
          _sorted.set(i, lastPartitionIt.next());
        }
      }
      return _sorted;
    }
  }
}
