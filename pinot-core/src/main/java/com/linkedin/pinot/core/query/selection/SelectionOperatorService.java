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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * SelectionOperatorService provides the services for selection query with capability of both un-sorting and sorting.
 *
 * Expected behavior:
 * - Return selection results with the same order of columns as user passed in.
 *   Eg. SELECT colB, colA, colC FROM table -> [valB, valA, valC]
 * - For 'select *', return columns with alphabetically order.
 *   Eg. SELECT * FROM table -> [valA, valB, valC]
 * - Order by does not change the order of columns in selection results.
 *   Eg. SELECT colB, colA, colC FROM table ORDER BY calC -> [valB, valA, valC]
 */
public class SelectionOperatorService {

  private int _numDocsScanned = 0;

  private final List<SelectionSort> _sortSequence;
  private final boolean _doOrdering;
  private final List<String> _selectionColumns;
  private final int _selectionOffset;
  private final int _maxRowSize;
  private final DataSchema _dataSchema;
  private final Comparator<Serializable[]> _rowComparator;
  private final Collection<Serializable[]> _rowEventsSet;

  /**
   * Constructor for SelectionOperatorService using index segment. (Server side)
   *
   * @param selections selection query.
   * @param indexSegment index segment.
   */
  public SelectionOperatorService(Selection selections, IndexSegment indexSegment) {
    _sortSequence = selections.getSelectionSortSequence();
    _doOrdering = _sortSequence != null && !_sortSequence.isEmpty();
    _selectionColumns = SelectionOperatorUtils.getSelectionColumns(selections.getSelectionColumns(), indexSegment);
    int selectionSize = selections.getSize();
    int selectionOffset = selections.getOffset();
    if (selectionSize == 0) {
      // No need to select any row.
      _selectionOffset = 0;
      _maxRowSize = 0;
    } else {
      // Select rows from offset to offset + size.
      _selectionOffset = selectionOffset;
      _maxRowSize = _selectionOffset + selectionSize;
    }
    _dataSchema = SelectionOperatorUtils.extractDataSchema(_sortSequence, _selectionColumns, indexSegment);
    _rowComparator = getComparator();
    if (_doOrdering) {
      _rowEventsSet = new PriorityQueue<>(Math.max(_maxRowSize, 1), _rowComparator);
    } else {
      _rowEventsSet = new ArrayList<>(_maxRowSize);
    }
  }

  /**
   * Constructor for SelectionOperatorService using data schema. (Broker side)
   *
   * @param selections selection query.
   * @param dataSchema data schema.
   */
  public SelectionOperatorService(Selection selections, DataSchema dataSchema) {
    _sortSequence = selections.getSelectionSortSequence();
    _doOrdering = _sortSequence != null && !_sortSequence.isEmpty();
    _selectionColumns = SelectionOperatorUtils.getSelectionColumns(selections.getSelectionColumns(), dataSchema);
    int selectionSize = selections.getSize();
    int selectionOffset = selections.getOffset();
    if (selectionSize == 0) {
      // No need to select any row.
      _selectionOffset = 0;
      _maxRowSize = 0;
    } else {
      // Select rows from offset to offset + size.
      _selectionOffset = selectionOffset;
      _maxRowSize = _selectionOffset + selectionSize;
    }
    _dataSchema = dataSchema;
    _rowComparator = getComparator();
    if (_doOrdering) {
      _rowEventsSet = new PriorityQueue<>(Math.max(_maxRowSize, 1), _rowComparator);
    } else {
      _rowEventsSet = new ArrayList<>(_maxRowSize);
    }
  }

  /**
   * Helper method to get the comparator for sorting selection rows.
   *
   * @return comparator for sorting selection rows.
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
            case BOOLEAN:
              if (!_sortSequence.get(i).isIsAsc()) {
                ret = Boolean.valueOf((String) o1[i]).compareTo(Boolean.valueOf((String) o2[i]));
              } else {
                ret = Boolean.valueOf((String) o2[i]).compareTo(Boolean.valueOf((String) o1[i]));
              }
              break;
            case BYTE:
              if (!_sortSequence.get(i).isIsAsc()) {
                ret = ((Byte) o1[i]).compareTo((Byte) o2[i]);
              } else {
                ret = ((Byte) o2[i]).compareTo((Byte) o1[i]);
              }
              break;
            case CHAR:
              if (!_sortSequence.get(i).isIsAsc()) {
                ret = ((Character) o1[i]).compareTo((Character) o2[i]);
              } else {
                ret = ((Character) o2[i]).compareTo((Character) o1[i]);
              }
              break;
            case SHORT:
              if (!_sortSequence.get(i).isIsAsc()) {
                ret = ((Short) o1[i]).compareTo((Short) o2[i]);
              } else {
                ret = ((Short) o2[i]).compareTo((Short) o1[i]);
              }
              break;
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
            // Do not support Object and Array type.
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
   * Get the selection results.
   *
   * @return collection of selection rows.
   */
  public Collection<Serializable[]> getRowEventsSet() {
    return _rowEventsSet;
  }

  /**
   * Get the data schema.
   *
   * @return data schema.
   */
  public DataSchema getDataSchema() {
    return _dataSchema;
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
   * Merge two partial results for selection query.
   *
   * @param rowEventsSet1 partial results 1.
   * @param rowEventsSet2 partial results 2.
   * @return merged result.
   */
  public Collection<Serializable[]> merge(Collection<Serializable[]> rowEventsSet1,
      Collection<Serializable[]> rowEventsSet2) {
    if (rowEventsSet1 == null) {
      return rowEventsSet2;
    }
    if (rowEventsSet2 == null) {
      return rowEventsSet1;
    }
    if (_doOrdering) {
      PriorityQueue<Serializable[]> queue1 = (PriorityQueue<Serializable[]>) rowEventsSet1;
      PriorityQueue<Serializable[]> queue2 = (PriorityQueue<Serializable[]>) rowEventsSet2;
      for (Serializable[] row : queue2) {
        addToPriorityQueue(row, queue1, _rowComparator);
      }
    } else {
      Iterator<Serializable[]> iterator = rowEventsSet2.iterator();
      while (rowEventsSet1.size() < _maxRowSize && iterator.hasNext()) {
        rowEventsSet1.add(iterator.next());
      }
    }
    return rowEventsSet1;
  }

  /**
   * Reduce a collection of data tables to selection results for selection query.
   *
   * @param selectionResults map from server instance to data table.
   * @return reduced result.
   */
  public Collection<Serializable[]> reduce(Map<ServerInstance, DataTable> selectionResults) {
    _rowEventsSet.clear();
    if (_doOrdering) {
      PriorityQueue<Serializable[]> queue = (PriorityQueue<Serializable[]>) _rowEventsSet;
      for (DataTable dataTable : selectionResults.values()) {
        int numRows = dataTable.getNumberOfRows();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Serializable[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          addToPriorityQueue(row, queue, _rowComparator);
        }
      }
    } else {
      for (DataTable dataTable : selectionResults.values()) {
        int numRows = dataTable.getNumberOfRows();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Serializable[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          if (_rowEventsSet.size() < _maxRowSize) {
            _rowEventsSet.add(row);
          } else {
            break;
          }
        }
      }
    }
    return _rowEventsSet;
  }

  /**
   * Helper method to add a value to the priority queue.
   *
   * @param value value to be added.
   * @param queue priority queue.
   * @param comparator comparator for the value.
   * @param <T> type for the value.
   */
  private <T> void addToPriorityQueue(T value, PriorityQueue<T> queue, Comparator<T> comparator) {
    if (queue.size() < _maxRowSize) {
      queue.add(value);
    } else if (comparator.compare(queue.peek(), value) < 0) {
      queue.poll();
      queue.add(value);
    }
  }

  /**
   * Render the final selection results to a JSONObject for selection query.
   * Only render the selection rows from offset to offset + selection size.
   *
   * @param finalResults final selection results.
   * @return final JSONObject result.
   * @throws Exception
   */
  public JSONObject render(Collection<Serializable[]> finalResults)
      throws Exception {
    LinkedList<JSONArray> rowEventsJsonList = new LinkedList<>();

    if (_doOrdering) {
      PriorityQueue<Serializable[]> queue = (PriorityQueue<Serializable[]>) finalResults;
      while (queue.size() > _selectionOffset) {
        rowEventsJsonList.addFirst(
            SelectionOperatorUtils.getJSonArrayFromRow(queue.poll(), _selectionColumns, _dataSchema));
      }
    } else {
      List<Serializable[]> list = (List<Serializable[]>) finalResults;
      int numRows = list.size();
      for (int i = _selectionOffset; i < numRows; i++) {
        rowEventsJsonList.add(SelectionOperatorUtils.getJSonArrayFromRow(list.get(i), _selectionColumns, _dataSchema));
      }
    }

    JSONObject resultJsonObject = new JSONObject();
    resultJsonObject.put("results", new JSONArray(rowEventsJsonList));
    resultJsonObject.put("columns", new JSONArray(_selectionColumns));
    return resultJsonObject;
  }

  /**
   * Render the final selection results to a SelectionResults object for selection query.
   * SelectionResults object will be used in building the BrokerResponse.
   *
   * @param finalResults final selection results.
   * @return SelectionResults object result.
   * @throws Exception
   */
  public SelectionResults renderSelectionResults(Collection<Serializable[]> finalResults)
      throws Exception {
    LinkedList<Serializable[]> rowEvents = new LinkedList<>();

    if (_doOrdering) {
      PriorityQueue<Serializable[]> queue = (PriorityQueue<Serializable[]>) finalResults;
      while (queue.size() > _selectionOffset) {
        rowEvents.addFirst(
            SelectionOperatorUtils.getFormattedRow(queue.poll(), _selectionColumns, _dataSchema));
      }
    } else {
      List<Serializable[]> list = (List<Serializable[]>) finalResults;
      int numRows = list.size();
      for (int i = _selectionOffset; i < numRows; i++) {
        rowEvents.add(SelectionOperatorUtils.getFormattedRow(list.get(i), _selectionColumns, _dataSchema));
      }
    }

    return new SelectionResults(_selectionColumns, rowEvents);
  }

  /**
   * Iterate over a list of blocks, extract values from them and merge the values to the selection results.
   *
   * @param blockDocIdIterator block document id iterator.
   * @param blocks list of blocks.
   * @throws Exception
   */
  public void iterateOnBlock(BlockDocIdIterator blockDocIdIterator, Block[] blocks)
      throws Exception {
    if (_maxRowSize > 0) {
      SelectionFetcher selectionFetcher = new SelectionFetcher(blocks, _dataSchema);
      int docId;
      if (_doOrdering) {
        Comparator<Integer> rowDocIdComparator = new CompositeDocIdValComparator(_sortSequence, blocks);
        PriorityQueue<Integer> queue = new PriorityQueue<>(_maxRowSize, rowDocIdComparator);
        while ((docId = blockDocIdIterator.next()) != Constants.EOF) {
          _numDocsScanned++;
          addToPriorityQueue(docId, queue, rowDocIdComparator);
        }
        PriorityQueue<Serializable[]> rowEventsPriorityQueue = new PriorityQueue<>(queue.size(), _rowComparator);
        while (!queue.isEmpty()) {
          rowEventsPriorityQueue.add(selectionFetcher.getRow(queue.poll()));
        }
        merge(_rowEventsSet, rowEventsPriorityQueue);
      } else {
        List<Serializable[]> rowEventsList = new ArrayList<>(_maxRowSize);
        while ((docId = blockDocIdIterator.next()) != Constants.EOF) {
          _numDocsScanned++;
          if (rowEventsList.size() < _maxRowSize) {
            rowEventsList.add(selectionFetcher.getRow(docId));
          } else {
            break;
          }
        }
        merge(_rowEventsSet, rowEventsList);
      }
    }
  }
}
