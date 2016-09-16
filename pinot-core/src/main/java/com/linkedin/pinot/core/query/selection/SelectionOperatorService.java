/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.selection;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.SelectionResults;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.selection.comparator.CompositeDocIdValComparator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import javax.annotation.Nonnull;


/**
 * The <code>SelectionOperatorService</code> class provides the services for selection queries with
 * <code>ORDER BY</code>.
 * <p>Expected behavior:
 * <ul>
 *   <li>Return selection results with the same order of columns as user passed in.</li>
 *   <ul>
 *     <li>Eg. <code>SELECT colB, colA, colC FROM table -> [valB, valA, valC]</code></li>
 *   </ul>
 *   <li>For <code>'SELECT *'</code>, return columns with alphabetically order.</li>
 *   <ul>
 *     <li>Eg. <code>SELECT * FROM table -> [valA, valB, valC]</code></li>
 *   </ul>
 *   <li>Order by does not change the order of columns in selection results.</li>
 *   <ul>
 *     <li>Eg. <code>SELECT colB, colA, colC FROM table ORDER BY calC -> [valB, valA, valC]</code></li>
 *   </ul>
 * </ul>
 */
public class SelectionOperatorService {

  private final List<String> _selectionColumns;
  private final List<SelectionSort> _sortSequence;
  private final DataSchema _dataSchema;
  private final int _selectionOffset;
  private final int _maxRowSize;
  private final PriorityQueue<Serializable[]> _rowEventsSet;

  private long _numDocsScanned = 0;

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link IndexSegment}. (Server side)
   *
   * @param selection selection query.
   * @param indexSegment index segment.
   */
  public SelectionOperatorService(@Nonnull Selection selection, @Nonnull IndexSegment indexSegment) {
    _selectionColumns = SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), indexSegment);
    _sortSequence = selection.getSelectionSortSequence();
    // For highest performance, only allow selection queries with ORDER BY.
    Preconditions.checkState(_sortSequence != null && !_sortSequence.isEmpty());
    _dataSchema = SelectionOperatorUtils.extractDataSchema(_sortSequence, _selectionColumns, indexSegment);

    // TODO: selectionSize should never be 0 on this layer, address LIMIT 0 selection queries on upper layer.
    int selectionSize = selection.getSize();
    if (selectionSize == 0) {
      // No need to select any row.
      _selectionOffset = 0;
      _maxRowSize = 0;
      _rowEventsSet = new PriorityQueue<>(1);
    } else {
      // Select rows from offset to offset + size.
      _selectionOffset = selection.getOffset();
      _maxRowSize = _selectionOffset + selectionSize;
      _rowEventsSet = new PriorityQueue<>(_maxRowSize, getComparator());
    }
  }

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link DataSchema}. (Broker side)
   *
   * @param selection selection query.
   * @param dataSchema data schema.
   */
  public SelectionOperatorService(@Nonnull Selection selection, @Nonnull DataSchema dataSchema) {
    _selectionColumns = SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), dataSchema);
    _sortSequence = selection.getSelectionSortSequence();
    // For highest performance, only allow selection queries with ORDER BY.
    Preconditions.checkState(_sortSequence != null && !_sortSequence.isEmpty());
    _dataSchema = dataSchema;

    // TODO: selectionSize should never be 0 on this layer, address LIMIT 0 selection queries on upper layer.
    int selectionSize = selection.getSize();
    if (selectionSize == 0) {
      // No need to select any row.
      _selectionOffset = 0;
      _maxRowSize = 0;
      _rowEventsSet = new PriorityQueue<>(1);
    } else {
      // Select rows from offset to offset + size.
      _selectionOffset = selection.getOffset();
      _maxRowSize = _selectionOffset + selectionSize;
      _rowEventsSet = new PriorityQueue<>(_maxRowSize, getComparator());
    }
  }

  /**
   * Helper method to get the {@link Comparator} for selection rows.
   *
   * @return {@link Comparator} for selection rows.
   */
  private Comparator<Serializable[]> getComparator() {
    return new Comparator<Serializable[]>() {
      @Override
      public int compare(Serializable[] o1, Serializable[] o2) {
        int numSortColumns = _sortSequence.size();
        for (int i = 0; i < numSortColumns; i++) {
          int ret = 0;
          // Only compare single-value columns.
          switch (_dataSchema.getColumnType(i)) {
            case INT:
              if (!_sortSequence.get(i).isIsAsc()) {
                ret = ((Integer) o1[i]).compareTo((Integer) o2[i]);
              } else {
                ret = ((Integer) o2[i]).compareTo((Integer) o1[i]);
              }
              break;
            case LONG:
              if (!_sortSequence.get(i).isIsAsc()) {
                ret = ((Long) o1[i]).compareTo((Long) o2[i]);
              } else {
                ret = ((Long) o2[i]).compareTo((Long) o1[i]);
              }
              break;
            case FLOAT:
              if (!_sortSequence.get(i).isIsAsc()) {
                ret = ((Float) o1[i]).compareTo((Float) o2[i]);
              } else {
                ret = ((Float) o2[i]).compareTo((Float) o1[i]);
              }
              break;
            case DOUBLE:
              if (!_sortSequence.get(i).isIsAsc()) {
                ret = ((Double) o1[i]).compareTo((Double) o2[i]);
              } else {
                ret = ((Double) o2[i]).compareTo((Double) o1[i]);
              }
              break;
            case STRING:
              if (!_sortSequence.get(i).isIsAsc()) {
                ret = ((String) o1[i]).compareTo((String) o2[i]);
              } else {
                ret = ((String) o2[i]).compareTo((String) o1[i]);
              }
              break;
            default:
              break;
          }
          if (ret != 0) {
            return ret;
          }
        }
        return 0;
      }
    };
  }

  /**
   * Get the {@link DataSchema}.
   *
   * @return data schema.
   */
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /**
   * Get the selection results.
   *
   * @return selection results.
   */
  public PriorityQueue<Serializable[]> getRowEventsSet() {
    return _rowEventsSet;
  }

  /**
   * Get number of documents scanned.
   *
   * @return number of documents scanned.
   */
  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  /**
   * Iterate over {@link Block}s, extract values from them and merge the values to the selection results for selection
   * queries with <code>ORDER BY</code>. (Server side)
   *
   * @param blockDocIdIterator block document id iterator.
   * @param blocks {@link Block} array.
   */
  public void iterateOnBlocksWithOrdering(@Nonnull BlockDocIdIterator blockDocIdIterator, @Nonnull Block[] blocks) {
    if (_maxRowSize > 0) {
      Comparator<Integer> rowDocIdComparator = new CompositeDocIdValComparator(_sortSequence, blocks);
      PriorityQueue<Integer> rowDocIdPriorityQueue = new PriorityQueue<>(_maxRowSize, rowDocIdComparator);
      int docId;
      while ((docId = blockDocIdIterator.next()) != Constants.EOF) {
        _numDocsScanned++;
        addToPriorityQueue(docId, rowDocIdPriorityQueue);
      }

      SelectionFetcher selectionFetcher = new SelectionFetcher(blocks, _dataSchema);
      Collection<Serializable[]> rowEventsSet = new ArrayList<>(rowDocIdPriorityQueue.size());
      for (int rowDocId : rowDocIdPriorityQueue) {
        rowEventsSet.add(selectionFetcher.getRow(rowDocId));
      }
      mergeWithOrdering(_rowEventsSet, rowEventsSet);
    }
  }

  /**
   * Merge two partial results for selection queries with <code>ORDER BY</code>. (Server side)
   *
   * @param mergedRowEventsSet partial results 1.
   * @param toMergeRowEventsSet partial results 2.
   */
  public void mergeWithOrdering(@Nonnull Collection<Serializable[]> mergedRowEventsSet,
      @Nonnull Collection<Serializable[]> toMergeRowEventsSet) {
    if (_maxRowSize > 0) {
      PriorityQueue<Serializable[]> mergedPriorityQueue = (PriorityQueue<Serializable[]>) mergedRowEventsSet;
      for (Serializable[] row : toMergeRowEventsSet) {
        addToPriorityQueue(row, mergedPriorityQueue);
      }
    }
  }

  /**
   * Reduce a collection of {@link DataTable}s to selection results for selection queries with <code>ORDER BY</code>.
   * (Broker side)
   *
   * @param selectionResults {@link Map} from {@link ServerInstance} to {@link DataTable}.
   * @return reduced results.
   */
  public PriorityQueue<Serializable[]> reduceWithOrdering(@Nonnull Map<ServerInstance, DataTable> selectionResults) {
    if (_maxRowSize > 0) {
      for (DataTable dataTable : selectionResults.values()) {
        int numRows = dataTable.getNumberOfRows();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Serializable[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          addToPriorityQueue(row, _rowEventsSet);
        }
      }
    }
    return _rowEventsSet;
  }

  /**
   * Render the final selection results to a {@link SelectionResults} object for selection queries with
   * <code>ORDER BY</code>. (Broker side)
   * <p>{@link SelectionResults} object will be used in building the BrokerResponse.
   *
   * @param finalResults final selection results.
   * @return {@link SelectionResults} object results.
   */
  public SelectionResults renderSelectionResultsWithOrdering(@Nonnull Collection<Serializable[]> finalResults) {
    LinkedList<Serializable[]> rowEventsSet = new LinkedList<>();

    PriorityQueue<Serializable[]> finalResultsPriorityQueue = (PriorityQueue<Serializable[]>) finalResults;
    while (finalResultsPriorityQueue.size() > _selectionOffset) {
      rowEventsSet.addFirst(
          SelectionOperatorUtils.getFormattedRow(finalResultsPriorityQueue.poll(), _selectionColumns, _dataSchema));
    }

    return new SelectionResults(_selectionColumns, rowEventsSet);
  }

  /**
   * Helper method to add a value to a {@link PriorityQueue}.
   *
   * @param value value to be added.
   * @param queue priority queue.
   * @param <T> type for the value.
   */
  private <T> void addToPriorityQueue(@Nonnull T value, @Nonnull PriorityQueue<T> queue) {
    if (queue.size() < _maxRowSize) {
      queue.add(value);
    } else if (queue.comparator().compare(queue.peek(), value) < 0) {
      queue.poll();
      queue.add(value);
    }
  }
}
