/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import javax.activation.UnsupportedDataTypeException;

import org.json.JSONArray;
import org.json.JSONObject;

import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * SelectionOperator provides the apis for selection query.
 *
 * @author xiafu
 *
 */
public class SelectionOperatorService {

  private int _numDocsScanned = 0;
  private final List<SelectionSort> _sortSequence;
  private final List<String> _selectionColumns;
  private final int _selectionSize;
  private final int _selectionOffset;
  private final int _maxRowSize;
  private final DataSchema _dataSchema;
  private final Comparator<Serializable[]> _rowComparator;
  private final Collection<Serializable[]> _rowEventsSet;

  private Comparator<Integer> _rowDocIdComparator;
  private Collection<Integer> _rowDocIdSet;

  private final IndexSegment _indexSegment;

  public SelectionOperatorService(Selection selections, IndexSegment indexSegment) {
    _indexSegment = indexSegment;
    _sortSequence = selections.getSelectionSortSequence();
    _selectionColumns = getSelectionColumns(selections.getSelectionColumns());
    _selectionSize = selections.getSize();
    _selectionOffset = selections.getOffset();
    _maxRowSize = _selectionOffset + _selectionSize;
    _dataSchema = SelectionOperatorUtils.extractDataSchema(_sortSequence, _selectionColumns, _indexSegment);
    _rowComparator = getComparator(_sortSequence, _dataSchema);
    _rowEventsSet = new PriorityQueue<Serializable[]>(_maxRowSize, _rowComparator);
    _rowDocIdComparator = null;
    _rowDocIdSet = null;
  }

  private List<String> getSelectionColumns(List<String> selectionColumns) {
    if ((selectionColumns.size() == 1) && selectionColumns.get(0).equals("*")) {
      final List<String> newSelectionColumns = new ArrayList<String>();
      for (final String columnName : _indexSegment.getColumnNames()) {
        newSelectionColumns.add(columnName);
      }
      return newSelectionColumns;
    }
    return selectionColumns;
  }

  public SelectionOperatorService(Selection selections, DataSchema dataSchema) {
    _indexSegment = null;
    _sortSequence = selections.getSelectionSortSequence();
    _selectionColumns = getSelectionColumns(selections.getSelectionColumns(), dataSchema);
    _selectionSize = selections.getSize();
    _selectionOffset = selections.getOffset();
    _maxRowSize = _selectionOffset + _selectionSize;
    _dataSchema = dataSchema;
    _rowComparator = getComparator(_sortSequence, _dataSchema);
    _rowEventsSet = new PriorityQueue<Serializable[]>(_maxRowSize, _rowComparator);
    _rowDocIdComparator = null;
    _rowDocIdSet = null;
  }

  private List<String> getSelectionColumns(List<String> selectionColumns, DataSchema dataSchema) {
    if ((selectionColumns.size() == 1) && selectionColumns.get(0).equals("*")) {
      final List<String> newSelectionColumns = new ArrayList<String>();
      for (int i = 0; i < dataSchema.size(); ++i) {
        newSelectionColumns.add(dataSchema.getColumnName(i));
      }
      return newSelectionColumns;
    }
    return selectionColumns;
  }

  public Collection<Serializable[]> merge(Collection<Serializable[]> rowEventsSet1,
      Collection<Serializable[]> rowEventsSet2) {
    PriorityQueue<Serializable[]> queue1 = (PriorityQueue<Serializable[]>) rowEventsSet1;
    PriorityQueue<Serializable[]> queue2 = (PriorityQueue<Serializable[]>) rowEventsSet2;
    final Iterator<Serializable[]> iterator = queue2.iterator();
    while (iterator.hasNext()) {
      final Serializable[] row = iterator.next();
      if (queue1.size() < _maxRowSize) {
        queue1.add(row);
      } else {
        if (_rowComparator.compare(queue1.peek(), row) < 0) {
          queue1.add(row);
          queue1.poll();
        }
      }
    }
    return rowEventsSet1;
  }

  public Collection<Serializable[]> reduce(Map<ServerInstance, DataTable> selectionResults) {
    _rowEventsSet.clear();
    PriorityQueue<Serializable[]> queue = (PriorityQueue<Serializable[]>) _rowEventsSet;
    for (final DataTable dt : selectionResults.values()) {
      for (int rowId = 0; rowId < dt.getNumberOfRows(); ++rowId) {
        final Serializable[] row = SelectionOperatorUtils.extractRowFromDataTable(dt, rowId);
        if (queue.size() < _maxRowSize) {
          queue.add(row);
        } else {
          if (_rowComparator.compare(queue.peek(), row) < 0) {
            queue.add(row);
            queue.poll();
          }
        }
      }
    }
    return _rowEventsSet;
  }

  public JSONObject render(Collection<Serializable[]> finalResults, DataSchema dataSchema, int offset) throws Exception {
    final LinkedList<JSONArray> rowEventsJSonList = new LinkedList<JSONArray>();
    if (finalResults instanceof PriorityQueue<?>) {
      PriorityQueue<Serializable[]> queue = (PriorityQueue<Serializable[]>) finalResults;
      while (finalResults.size() > offset) {
        rowEventsJSonList.addFirst(SelectionOperatorUtils.getJSonArrayFromRow(queue.poll(), _selectionColumns, dataSchema));
      }
    } else if (finalResults instanceof ArrayList<?>) {
      List<Serializable[]> list = (List<Serializable[]>) finalResults;
      //TODO: check if the offset is inclusive or exclusive
      for (int i = offset; i < list.size(); i++) {
        rowEventsJSonList.add(SelectionOperatorUtils.getJSonArrayFromRow(list.get(i), _selectionColumns, dataSchema));
      }
    } else {
      throw new UnsupportedDataTypeException("type of results Expected: (PriorityQueue| ArrayList)) actual:"
          + finalResults.getClass());
    }
    final JSONObject resultJsonObject = new JSONObject();
    resultJsonObject.put("results", new JSONArray(rowEventsJSonList));
    resultJsonObject.put("columns", getSelectionColumnsFromDataSchema(dataSchema));
    return resultJsonObject;
  }

  private JSONArray getSelectionColumnsFromDataSchema(DataSchema dataSchema) {
    final JSONArray jsonArray = new JSONArray();
    for (int idx = 0; idx < dataSchema.size(); ++idx) {
      if (_selectionColumns.contains(dataSchema.getColumnName(idx))) {
        jsonArray.put(dataSchema.getColumnName(idx));
      }
    }
    return jsonArray;
  }

  public Collection<Serializable[]> getRowEventsSet() {
    return _rowEventsSet;
  }

  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  private Comparator<Serializable[]> getComparator(final List<SelectionSort> sortSequence, final DataSchema dataSchema) {
    return new Comparator<Serializable[]>() {
      @Override
      public int compare(Serializable[] o1, Serializable[] o2) {
        for (int i = 0; i < sortSequence.size(); ++i) {
          switch (dataSchema.getColumnType(i)) {
            case INT:
              if (!sortSequence.get(i).isIsAsc()) {
                return ((Integer) o1[i]).compareTo((Integer) o2[i]);
              } else {
                return ((Integer) o2[i]).compareTo((Integer) o1[i]);
              }
            case SHORT:
              if (!sortSequence.get(i).isIsAsc()) {
                return ((Short) o1[i]).compareTo((Short) o2[i]);
              } else {
                return ((Short) o2[i]).compareTo((Short) o1[i]);
              }
            case LONG:
              if (!sortSequence.get(i).isIsAsc()) {
                return ((Long) o1[i]).compareTo((Long) o2[i]);
              } else {
                return ((Long) o2[i]).compareTo((Long) o1[i]);
              }
            case FLOAT:
              if (!sortSequence.get(i).isIsAsc()) {
                return ((Float) o1[i]).compareTo((Float) o2[i]);
              } else {
                return ((Float) o2[i]).compareTo((Float) o1[i]);
              }
            case DOUBLE:
              if (!sortSequence.get(i).isIsAsc()) {
                return ((Double) o1[i]).compareTo((Double) o2[i]);
              } else {
                return ((Double) o2[i]).compareTo((Double) o1[i]);
              }
            case STRING:
              if (!sortSequence.get(i).isIsAsc()) {
                return ((String) o1[i]).compareTo((String) o2[i]);
              } else {
                return ((String) o2[i]).compareTo((String) o1[i]);
              }
            default:
              break;
          }
        }
        return 0;
      };
    };
  }

  public JSONObject render(Collection<Serializable[]> reduceResults) throws Exception {
    return render(reduceResults, _dataSchema, _selectionOffset);
  }

  public void iterateOnBlock(BlockDocIdIterator blockDocIdIterator, Block[] blocks) throws Exception {
    int docId = 0;
    _rowDocIdComparator = getDocIdComparator(_sortSequence, _dataSchema, blocks);
    _rowDocIdSet = new PriorityQueue<Integer>(_maxRowSize, _rowDocIdComparator);
    while ((docId = blockDocIdIterator.next()) != Constants.EOF) {
      _numDocsScanned++;
      if (_rowDocIdSet.size() < _maxRowSize) {
        _rowDocIdSet.add(docId);
      } else {
        PriorityQueue<Integer> queue = (PriorityQueue<Integer>) _rowDocIdSet;
        if (_rowDocIdComparator.compare(docId, queue.peek()) > 0) {
          queue.add(docId);
          queue.poll();
        }
      }
    }
    mergeToRowEventsSet(blocks);
  }

  public Collection<Serializable[]> mergeToRowEventsSet(Block[] blocks) throws Exception {
    final PriorityQueue<Serializable[]> rowEventsPriorityQueue =
        new PriorityQueue<Serializable[]>(_maxRowSize, _rowComparator);
    PriorityQueue<Integer> queue = (PriorityQueue<Integer>) _rowDocIdSet;
    while (!queue.isEmpty()) {
      Serializable[] rowFromBlockValSets = SelectionOperatorUtils.collectRowFromBlockValSets(queue.poll(), blocks, _dataSchema);
      rowEventsPriorityQueue.add(rowFromBlockValSets);
    }
    merge(_rowEventsSet, rowEventsPriorityQueue);
    return _rowEventsSet;
  }

  private Comparator<Integer> getDocIdComparator(final List<SelectionSort> sortSequence, final DataSchema dataSchema,
      final Block[] blocks) {

    return new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        for (int i = 0; i < sortSequence.size(); ++i) {
          // Don't compare multi-value column.
          if (!blocks[i].getMetadata().isSingleValue()) {
            continue;
          }
          if (blocks[i].getMetadata().hasDictionary()) {
            final BlockSingleValIterator blockValSetIterator =
                (BlockSingleValIterator) blocks[i].getBlockValueSet().iterator();
            blockValSetIterator.skipTo(o1);
            int v1 = blockValSetIterator.nextIntVal();
            blockValSetIterator.skipTo(o2);
            int v2 = blockValSetIterator.nextIntVal();
            if (v1 > v2) {
              if (!sortSequence.get(i).isIsAsc()) {
                return 1;
              } else {
                return -1;
              }
            }
            if (v1 < v2) {
              if (!sortSequence.get(i).isIsAsc()) {
                return -1;
              } else {
                return 1;
              }
            }
          } else {
            final BlockSingleValIterator blockValSetIterator =
                (BlockSingleValIterator) blocks[i].getBlockValueSet().iterator();
            switch (blocks[i].getMetadata().getDataType()) {
              case INT:
                blockValSetIterator.skipTo(o1);
                int i1 = blockValSetIterator.nextIntVal();
                blockValSetIterator.skipTo(o2);
                int i2 = blockValSetIterator.nextIntVal();
                if (i1 > i2) {
                  if (!sortSequence.get(i).isIsAsc()) {
                    return 1;
                  } else {
                    return -1;
                  }
                }
                if (i1 < i2) {
                  if (!sortSequence.get(i).isIsAsc()) {
                    return -1;
                  } else {
                    return 1;
                  }
                }
                break;
              case FLOAT:
                blockValSetIterator.skipTo(o1);
                float f1 = blockValSetIterator.nextIntVal();
                blockValSetIterator.skipTo(o2);
                float f2 = blockValSetIterator.nextIntVal();
                if (f1 > f2) {
                  if (!sortSequence.get(i).isIsAsc()) {
                    return 1;
                  } else {
                    return -1;
                  }
                }
                if (f1 < f2) {
                  if (!sortSequence.get(i).isIsAsc()) {
                    return -1;
                  } else {
                    return 1;
                  }
                }
                break;
              case LONG:
                blockValSetIterator.skipTo(o1);
                long l1 = blockValSetIterator.nextLongVal();
                blockValSetIterator.skipTo(o2);
                long l2 = blockValSetIterator.nextLongVal();
                if (l1 > l2) {
                  if (!sortSequence.get(i).isIsAsc()) {
                    return 1;
                  } else {
                    return -1;
                  }
                }
                if (l1 < l2) {
                  if (!sortSequence.get(i).isIsAsc()) {
                    return -1;
                  } else {
                    return 1;
                  }
                }
                break;
              case DOUBLE:
                blockValSetIterator.skipTo(o1);
                double d1 = blockValSetIterator.nextDoubleVal();
                blockValSetIterator.skipTo(o2);
                double d2 = blockValSetIterator.nextDoubleVal();
                if (d1 > d2) {
                  if (!sortSequence.get(i).isIsAsc()) {
                    return 1;
                  } else {
                    return -1;
                  }
                }
                if (d1 < d2) {
                  if (!sortSequence.get(i).isIsAsc()) {
                    return -1;
                  } else {
                    return 1;
                  }
                }
                break;

              default:
                break;
            }
          }
        }
        return 0;
      };
    };
  }
}
