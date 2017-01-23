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

import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.SelectionResults;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.selection.comparator.CompositeDocIdValComparator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * The <code>SelectionOperatorService</code> class provides the services for selection queries with
 * <code>ORDER BY</code>.
 * <p>Expected behavior:
 * <ul>
 *   <li>
 *     Return selection results with the same order of columns as user passed in.
 *     <ul>
 *       <li>Eg. SELECT colB, colA, colC FROM table -> [valB, valA, valC]</li>
 *     </ul>
 *   </li>
 *   <li>
 *     For 'SELECT *', return columns with alphabetically order.
 *     <ul>
 *       <li>Eg. SELECT * FROM table -> [valA, valB, valC]</li>
 *     </ul>
 *   </li>
 *   <li>
 *     Order by does not change the order of columns in selection results.
 *     <ul>
 *       <li>Eg. SELECT colB, colA, colC FROM table ORDER BY calC -> [valB, valA, valC]</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class SelectionOperatorService {
  private final List<String> _selectionColumns;
  private final List<SelectionSort> _sortSequence;
  private final DataSchema _dataSchema;
  private final int _selectionOffset;
  private final int _maxNumRows;
  private final PriorityQueue<Serializable[]> _rows;

  private long _numDocsScanned = 0;

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link IndexSegment}. (Inner segment)
   *
   * @param selection selection query.
   * @param indexSegment index segment.
   */
  public SelectionOperatorService(@Nonnull Selection selection, @Nonnull IndexSegment indexSegment) {
    _selectionColumns = SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), indexSegment);
    _sortSequence = getSortSequence(selection.getSelectionSortSequence());
    _dataSchema = SelectionOperatorUtils.extractDataSchema(_sortSequence, _selectionColumns, indexSegment);
    // Select rows from offset to offset + size.
    _selectionOffset = selection.getOffset();
    _maxNumRows = _selectionOffset + selection.getSize();
    _rows = new PriorityQueue<>(_maxNumRows, getStrictComparator());
  }

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link DataSchema}. (Inter segment)
   *
   * @param selection selection query.
   * @param dataSchema data schema.
   */
  public SelectionOperatorService(@Nonnull Selection selection, @Nonnull DataSchema dataSchema) {
    _selectionColumns = SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), dataSchema);
    _sortSequence = getSortSequence(selection.getSelectionSortSequence());
    _dataSchema = dataSchema;
    // Select rows from offset to offset + size.
    _selectionOffset = selection.getOffset();
    _maxNumRows = _selectionOffset + selection.getSize();
    _rows = new PriorityQueue<>(_maxNumRows, getTypeCompatibleComparator());
  }

  /**
   * Helper method to handle duplicate sort columns.
   *
   * @return de-duplicated list of sort sequences.
   */
  @Nonnull
  private List<SelectionSort> getSortSequence(List<SelectionSort> selectionSorts) {
    List<SelectionSort> deDupedSelectionSorts = new ArrayList<>();
    Set<String> sortColumns = new HashSet<>();
    for (SelectionSort selectionSort : selectionSorts) {
      String sortColumn = selectionSort.getColumn();
      if (!sortColumns.contains(sortColumn)) {
        deDupedSelectionSorts.add(selectionSort);
        sortColumns.add(sortColumn);
      }
    }
    return deDupedSelectionSorts;
  }

  /**
   * Helper method to get the strict {@link Comparator} for selection rows. (Inner segment)
   * <p>Strict comparator does not allow any schema mismatch (more performance driven).
   *
   * @return strict {@link Comparator} for selection rows.
   */
  @Nonnull
  private Comparator<Serializable[]> getStrictComparator() {
    return new Comparator<Serializable[]>() {
      @Override
      public int compare(Serializable[] o1, Serializable[] o2) {
        int numSortColumns = _sortSequence.size();
        for (int i = 0; i < numSortColumns; i++) {
          int ret = 0;
          SelectionSort selectionSort = _sortSequence.get(i);
          Serializable v1 = o1[i];
          Serializable v2 = o2[i];

          // Only compare single-value columns.
          switch (_dataSchema.getColumnType(i)) {
            case INT:
              if (!selectionSort.isIsAsc()) {
                ret = ((Integer) v1).compareTo((Integer) v2);
              } else {
                ret = ((Integer) v2).compareTo((Integer) v1);
              }
              break;
            case LONG:
              if (!selectionSort.isIsAsc()) {
                ret = ((Long) v1).compareTo((Long) v2);
              } else {
                ret = ((Long) v2).compareTo((Long) v1);
              }
              break;
            case FLOAT:
              if (!selectionSort.isIsAsc()) {
                ret = ((Float) v1).compareTo((Float) v2);
              } else {
                ret = ((Float) v2).compareTo((Float) v1);
              }
              break;
            case DOUBLE:
              if (!selectionSort.isIsAsc()) {
                ret = ((Double) v1).compareTo((Double) v2);
              } else {
                ret = ((Double) v2).compareTo((Double) v1);
              }
              break;
            case STRING:
              if (!selectionSort.isIsAsc()) {
                ret = ((String) v1).compareTo((String) v2);
              } else {
                ret = ((String) v2).compareTo((String) v1);
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
   * Helper method to get the type-compatible {@link Comparator} for selection rows. (Inter segment)
   * <p>Type-compatible comparator allows compatible types to compare with each other.
   *
   * @return flexible {@link Comparator} for selection rows.
   */
  @Nonnull
  private Comparator<Serializable[]> getTypeCompatibleComparator() {
    return new Comparator<Serializable[]>() {
      @Override
      public int compare(Serializable[] o1, Serializable[] o2) {
        int numSortColumns = _sortSequence.size();
        for (int i = 0; i < numSortColumns; i++) {
          int ret = 0;
          SelectionSort selectionSort = _sortSequence.get(i);
          Serializable v1 = o1[i];
          Serializable v2 = o2[i];

          // Only compare single-value columns.
          if (v1 instanceof Number) {
            if (!selectionSort.isIsAsc()) {
              ret = Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());
            } else {
              ret = Double.compare(((Number) v2).doubleValue(), ((Number) v1).doubleValue());
            }
          } else if (v1 instanceof String) {
            if (!selectionSort.isIsAsc()) {
              ret = ((String) v1).compareTo((String) v2);
            } else {
              ret = ((String) v2).compareTo((String) v1);
            }
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
  @Nonnull
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /**
   * Get the selection results.
   *
   * @return selection results.
   */
  @Nonnull
  public PriorityQueue<Serializable[]> getRows() {
    return _rows;
  }

  /**
   * Get number of documents scanned. (Inner segment)
   *
   * @return number of documents scanned.
   */
  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  /**
   * Iterate over {@link Block}s, extract values from them and merge the values to the selection results for selection
   * queries with <code>ORDER BY</code>. (Inner segment)
   *
   * @param blockDocIdIterator block document id iterator.
   * @param blocks {@link Block} array.
   */
  public void iterateOnBlocksWithOrdering(@Nonnull BlockDocIdIterator blockDocIdIterator, @Nonnull Block[] blocks) {
    Comparator<Integer> rowDocIdComparator = new CompositeDocIdValComparator(_sortSequence, blocks);
    PriorityQueue<Integer> rowDocIdPriorityQueue = new PriorityQueue<>(_maxNumRows, rowDocIdComparator);
    int docId;
    while ((docId = blockDocIdIterator.next()) != Constants.EOF) {
      _numDocsScanned++;
      SelectionOperatorUtils.addToPriorityQueue(docId, rowDocIdPriorityQueue, _maxNumRows);
    }

    SelectionFetcher selectionFetcher = new SelectionFetcher(blocks, _dataSchema);
    Collection<Serializable[]> rows = new ArrayList<>(rowDocIdPriorityQueue.size());
    for (int rowDocId : rowDocIdPriorityQueue) {
      rows.add(selectionFetcher.getRow(rowDocId));
    }
    SelectionOperatorUtils.mergeWithOrdering(_rows, rows, _maxNumRows);
  }

  /**
   * Reduce a collection of {@link DataTable}s to selection rows for selection queries with <code>ORDER BY</code>.
   * (Broker side)
   *
   * @param selectionResults {@link Map} from {@link ServerInstance} to {@link DataTable}.
   */
  public void reduceWithOrdering(@Nonnull Map<ServerInstance, DataTable> selectionResults) {
    for (DataTable dataTable : selectionResults.values()) {
      int numRows = dataTable.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        Serializable[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
        SelectionOperatorUtils.addToPriorityQueue(row, _rows, _maxNumRows);
      }
    }
  }

  /**
   * Render the unformatted selection rows to a formatted {@link SelectionResults} object for selection queries with
   * <code>ORDER BY</code>. (Broker side)
   * <p>{@link SelectionResults} object will be used to build the broker response.
   * <p>Should be called after method "reduceWithOrdering()".
   *
   * @return {@link SelectionResults} object results.
   */
  @Nonnull
  public SelectionResults renderSelectionResultsWithOrdering() {
    LinkedList<Serializable[]> rowsInSelectionResults = new LinkedList<>();

    int[] columnIndices = getColumnIndices();
    while (_rows.size() > _selectionOffset) {
      rowsInSelectionResults.addFirst(getFormattedRowWithOrdering(_rows.poll(), columnIndices));
    }

    return new SelectionResults(_selectionColumns, rowsInSelectionResults);
  }

  /**
   * Helper method to get each selection column index in data schema.
   *
   * @return column indices.
   */
  private int[] getColumnIndices() {
    int numSelectionColumns = _selectionColumns.size();
    int[] columnIndices = new int[numSelectionColumns];

    int numColumnsInDataSchema = _dataSchema.size();
    Map<String, Integer> dataSchemaIndices = new HashMap<>(numColumnsInDataSchema);
    for (int i = 0; i < numColumnsInDataSchema; i++) {
      dataSchemaIndices.put(_dataSchema.getColumnName(i), i);
    }

    for (int i = 0; i < numSelectionColumns; i++) {
      columnIndices[i] = dataSchemaIndices.get(_selectionColumns.get(i));
    }

    return columnIndices;
  }

  /**
   * Helper method to format a selection row, make all values string or string array type based on data schema passed in
   * for selection queries with <code>ORDER BY</code>. (Broker side)
   * <p>Formatted row is used to build the {@link SelectionResults}.
   *
   * @param row selection row to be formatted.
   * @param columnIndices column indices of original rows.
   * @return formatted selection row.
   */
  @Nonnull
  private Serializable[] getFormattedRowWithOrdering(@Nonnull Serializable[] row, @Nonnull int[] columnIndices) {
    int numColumns = columnIndices.length;
    Serializable[] formattedRow = new Serializable[numColumns];
    for (int i = 0; i < numColumns; i++) {
      int columnIndex = columnIndices[i];
      formattedRow[i] =
          SelectionOperatorUtils.getFormattedValue(row[columnIndex], _dataSchema.getColumnType(columnIndex));
    }
    return formattedRow;
  }
}
