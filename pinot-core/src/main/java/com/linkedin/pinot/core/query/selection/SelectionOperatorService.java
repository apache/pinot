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

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.SelectionResults;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.blocks.MultiValueBlock;
import com.linkedin.pinot.core.operator.blocks.RealtimeMultiValueBlock;
import com.linkedin.pinot.core.operator.blocks.RealtimeSingleValueBlock;
import com.linkedin.pinot.core.operator.blocks.SortedSingleValueBlock;
import com.linkedin.pinot.core.operator.blocks.UnSortedSingleValueBlock;
import com.linkedin.pinot.core.query.selection.comparator.CompositeDocIdValComparator;
import com.linkedin.pinot.core.realtime.impl.dictionary.DoubleMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.FloatMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.IntMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.LongMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.StringMutableDictionary;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import javax.activation.UnsupportedDataTypeException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * SelectionOperator provides the apis for selection query.
 *
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
  private final boolean _doOrdering;

  public static final Map<DataType, DecimalFormat> DEFAULT_FORMAT_STRING_MAP = new HashMap<DataType, DecimalFormat>();

  static {
    DEFAULT_FORMAT_STRING_MAP
        .put(DataType.INT, new DecimalFormat("##########", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP
        .put(DataType.LONG, new DecimalFormat("####################", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP
        .put(DataType.FLOAT, new DecimalFormat("##########.#####", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.DOUBLE,
        new DecimalFormat("####################.##########", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP
        .put(DataType.INT_ARRAY, new DecimalFormat("##########", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.LONG_ARRAY,
        new DecimalFormat("####################", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP
        .put(DataType.FLOAT_ARRAY, new DecimalFormat("##########.#####", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.DOUBLE_ARRAY,
        new DecimalFormat("####################.##########", DecimalFormatSymbols.getInstance(Locale.US)));
  }

  public SelectionOperatorService(Selection selections, IndexSegment indexSegment) {
    _indexSegment = indexSegment;
    if ((selections.getSelectionSortSequence() == null) || selections.getSelectionSortSequence().isEmpty()) {
      _doOrdering = false;
    } else {
      _doOrdering = true;
    }
    _sortSequence = selections.getSelectionSortSequence();
    _selectionColumns = getSelectionColumns(selections.getSelectionColumns());
    _selectionSize = selections.getSize();
    _selectionOffset = selections.getOffset();
    _maxRowSize = _selectionOffset + _selectionSize;
    _dataSchema = getDataSchema(_sortSequence, _selectionColumns, _indexSegment);
    _rowComparator = getComparator(_sortSequence, _dataSchema);
    if (_doOrdering) {
      _rowEventsSet = new PriorityQueue<Serializable[]>(_maxRowSize, _rowComparator);
    } else {
      _rowEventsSet = new ArrayList<Serializable[]>(_maxRowSize);
    }
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
    if ((selections.getSelectionSortSequence() == null) || selections.getSelectionSortSequence().isEmpty()) {
      _doOrdering = false;
    } else {
      _doOrdering = true;
    }
    _sortSequence = selections.getSelectionSortSequence();
    _selectionColumns = getSelectionColumns(selections.getSelectionColumns(), dataSchema);
    _selectionSize = selections.getSize();
    _selectionOffset = selections.getOffset();
    _maxRowSize = _selectionOffset + _selectionSize;
    _dataSchema = dataSchema;
    _rowComparator = getComparator(_sortSequence, _dataSchema);
    if (_doOrdering) {
      _rowEventsSet = new PriorityQueue<Serializable[]>(_maxRowSize, _rowComparator);
    } else {
      _rowEventsSet = new ArrayList<Serializable[]>(_maxRowSize);
    }
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
    if (rowEventsSet1 == null) {
      return rowEventsSet2;
    }
    if (rowEventsSet2 == null) {
      return rowEventsSet1;
    }
    if (_doOrdering) {
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
    } else {
      final Iterator<Serializable[]> iterator = rowEventsSet2.iterator();
      while (rowEventsSet1.size() < _maxRowSize && iterator.hasNext()) {
        final Serializable[] row = iterator.next();
        rowEventsSet1.add(row);
      }
    }
    return rowEventsSet1;
  }

  public Collection<Serializable[]> reduce(Map<ServerInstance, DataTable> selectionResults) {
    _rowEventsSet.clear();
    if (_doOrdering) {
      PriorityQueue<Serializable[]> queue = (PriorityQueue<Serializable[]>) _rowEventsSet;
      for (final DataTable dt : selectionResults.values()) {
        for (int rowId = 0; rowId < dt.getNumberOfRows(); ++rowId) {
          final Serializable[] row = getRowFromDataTable(dt, rowId);
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
    } else {
      for (final DataTable dt : selectionResults.values()) {
        for (int rowId = 0; rowId < dt.getNumberOfRows(); ++rowId) {
          final Serializable[] row = getRowFromDataTable(dt, rowId);
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

  public JSONObject render(Collection<Serializable[]> finalResults, DataSchema dataSchema, int offset)
      throws Exception {
    final LinkedList<JSONArray> rowEventsJSonList = new LinkedList<JSONArray>();
    if (finalResults instanceof PriorityQueue<?>) {
      PriorityQueue<Serializable[]> queue = (PriorityQueue<Serializable[]>) finalResults;
      while (finalResults.size() > offset) {
        rowEventsJSonList.addFirst(getJSonArrayFromRow(queue.poll(), dataSchema));
      }
    } else if (finalResults instanceof ArrayList<?>) {
      List<Serializable[]> list = (List<Serializable[]>) finalResults;
      //TODO: check if the offset is inclusive or exclusive
      for (int i = offset; i < list.size(); i++) {
        rowEventsJSonList.add(getJSonArrayFromRow(list.get(i), dataSchema));
      }
    } else {
      throw new UnsupportedDataTypeException(
          "type of results Expected: (PriorityQueue| ArrayList)) actual:" + finalResults.getClass());
    }
    final JSONObject resultJsonObject = new JSONObject();
    resultJsonObject.put("results", new JSONArray(rowEventsJSonList));
    resultJsonObject.put("columns", getSelectionColumnsJsonArrayFromDataSchema(dataSchema));
    return resultJsonObject;
  }

  /**
   * Given a collection of selection results from broker, translate them to SelectionResults object
   * to be used for building BrokerResponse.
   *
   * @param reducedResults
   * @param dataSchema
   * @param offset
   * @return
   * @throws Exception
   */
  public SelectionResults renderSelectionResults(Collection<Serializable[]> reducedResults, DataSchema dataSchema,
      int offset)
      throws Exception {
    final LinkedList<Serializable[]> rows = new LinkedList<Serializable[]>();

    if (reducedResults instanceof PriorityQueue<?>) {
      PriorityQueue<Serializable[]> queue = (PriorityQueue<Serializable[]>) reducedResults;
      while (reducedResults.size() > offset) {
        rows.addFirst(SelectionOperatorUtils.getFormattedRow(queue.poll(), _selectionColumns, dataSchema));
      }
    } else if (reducedResults instanceof ArrayList<?>) {
      List<Serializable[]> list = (List<Serializable[]>) reducedResults;
      for (int i = offset; i < list.size(); i++) {
        rows.add(SelectionOperatorUtils.getFormattedRow(list.get(i), _selectionColumns, dataSchema));
      }
    } else {
      throw new UnsupportedDataTypeException(
          "type of results Expected: (PriorityQueue| ArrayList)) actual:" + reducedResults.getClass());
    }

    List<String> columns = getSelectionColumnsFromDataSchema(dataSchema);
    return new SelectionResults(columns, rows);
  }

  private JSONArray getSelectionColumnsJsonArrayFromDataSchema(DataSchema dataSchema) {
    final JSONArray jsonArray = new JSONArray();
    for (int idx = 0; idx < dataSchema.size(); ++idx) {
      if (_selectionColumns.contains(dataSchema.getColumnName(idx))) {
        jsonArray.put(dataSchema.getColumnName(idx));
      }
    }
    return jsonArray;
  }

  private List<String> getSelectionColumnsFromDataSchema(DataSchema dataSchema) {
    List<String> columns = new ArrayList<String>();

    for (int i = 0; i < dataSchema.size(); i++) {
      if (_selectionColumns.contains(dataSchema.getColumnName(i))) {
        columns.add(dataSchema.getColumnName(i));
      }
    }
    return columns;
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

  private Comparator<Serializable[]> getComparator(final List<SelectionSort> sortSequence,
      final DataSchema dataSchema) {
    return new Comparator<Serializable[]>() {
      @Override
      public int compare(Serializable[] o1, Serializable[] o2) {
        for (int i = 0; i < sortSequence.size(); ++i) {
          int ret = 0;
          switch (dataSchema.getColumnType(i)) {
            case INT:
              if (!sortSequence.get(i).isIsAsc()) {
                ret = ((Integer) o1[i]).compareTo((Integer) o2[i]);
              } else {
                ret = ((Integer) o2[i]).compareTo((Integer) o1[i]);
              }
              break;
            case SHORT:
              if (!sortSequence.get(i).isIsAsc()) {
                ret = ((Short) o1[i]).compareTo((Short) o2[i]);
              } else {
                ret = ((Short) o2[i]).compareTo((Short) o1[i]);
              }
              break;
            case LONG:
              if (!sortSequence.get(i).isIsAsc()) {
                ret = ((Long) o1[i]).compareTo((Long) o2[i]);
              } else {
                ret = ((Long) o2[i]).compareTo((Long) o1[i]);
              }
              break;
            case FLOAT:
              if (!sortSequence.get(i).isIsAsc()) {
                ret = ((Float) o1[i]).compareTo((Float) o2[i]);
              } else {
                ret = ((Float) o2[i]).compareTo((Float) o1[i]);
              }
              break;
            case DOUBLE:
              if (!sortSequence.get(i).isIsAsc()) {
                ret = ((Double) o1[i]).compareTo((Double) o2[i]);
              } else {
                ret = ((Double) o2[i]).compareTo((Double) o1[i]);
              }
              break;
            case STRING:
              if (!sortSequence.get(i).isIsAsc()) {
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

      ;
    };
  }

  public JSONObject render(Collection<Serializable[]> reduceResults)
      throws Exception {
    return render(reduceResults, _dataSchema, _selectionOffset);
  }

  /**
   * Translate the selection results from broker to SelectionResults object to be used
   * for building BrokerResponse.
   *
   * @param reduceResults
   * @return
   * @throws Exception
   */
  public SelectionResults renderSelectionResults(Collection<Serializable[]> reduceResults)
      throws Exception {
    return renderSelectionResults(reduceResults, _dataSchema, _selectionOffset);
  }

  private DataSchema getDataSchema(List<SelectionSort> sortSequence, List<String> selectionColumns,
      IndexSegment indexSegment) {
    final List<String> columns = new ArrayList<String>();

    if (sortSequence != null && !sortSequence.isEmpty()) {
      for (final SelectionSort selectionSort : sortSequence) {
        columns.add(selectionSort.getColumn());
      }
    }
    String[] selectionColumnArray = selectionColumns.toArray(new String[selectionColumns.size()]);
    Arrays.sort(selectionColumnArray);
    for (int i = 0; i < selectionColumnArray.length; ++i) {
      String selectionColumn = selectionColumnArray[i];
      if (!columns.contains(selectionColumn)) {
        columns.add(selectionColumn);
      }
    }
    final DataType[] dataTypes = new DataType[columns.size()];
    for (int i = 0; i < dataTypes.length; ++i) {
      DataSource ds = indexSegment.getDataSource(columns.get(i));
      DataSourceMetadata m = ds.getDataSourceMetadata();
      dataTypes[i] = m.getDataType();
      if (!m.isSingleValue()) {
        dataTypes[i] = DataType.valueOf(dataTypes[i] + "_ARRAY");
      }
    }
    return new DataSchema(columns.toArray(new String[0]), dataTypes);
  }

  public void iterateOnBlock(BlockDocIdIterator blockDocIdIterator, Block[] blocks)
      throws Exception {
    int docId = 0;
    _rowDocIdComparator = getDocValBasedComparator(_sortSequence, _dataSchema, blocks);
    if (_doOrdering) {
      _rowDocIdSet = new PriorityQueue<Integer>(_maxRowSize, _rowDocIdComparator);
    } else {
      _rowDocIdSet = new ArrayList<Integer>(_maxRowSize);
    }
    while ((docId = blockDocIdIterator.next()) != Constants.EOF) {
      _numDocsScanned++;
      if (_rowDocIdSet.size() < _maxRowSize) {
        _rowDocIdSet.add(docId);
      } else {
        if (!_doOrdering) {
          break;
        }
        PriorityQueue<Integer> queue = (PriorityQueue<Integer>) _rowDocIdSet;
        if (_doOrdering && (_rowDocIdComparator.compare(docId, queue.peek()) > 0)) {
          queue.add(docId);
          queue.poll();
        }
      }
    }
    mergeToRowEventsSet(blocks);
  }

  public Collection<Serializable[]> mergeToRowEventsSet(Block[] blocks)
      throws Exception {
    SelectionFetcher selectionFetcher = new SelectionFetcher(blocks, _dataSchema);
    if (_doOrdering) {
      final PriorityQueue<Serializable[]> rowEventsPriorityQueue =
          new PriorityQueue<Serializable[]>(_maxRowSize, _rowComparator);
      PriorityQueue<Integer> queue = (PriorityQueue<Integer>) _rowDocIdSet;
      while (!queue.isEmpty()) {
        Serializable[] rowFromBlockValSets = selectionFetcher.getRow(queue.poll());
        rowEventsPriorityQueue.add(rowFromBlockValSets);
      }
      merge(_rowEventsSet, rowEventsPriorityQueue);
    } else {
      final List<Serializable[]> rowEventsList = new ArrayList<Serializable[]>(_maxRowSize);
      List<Integer> list = (List<Integer>) _rowDocIdSet;
      for (int i = 0; i < Math.min(_maxRowSize, list.size()); i++) {
        Serializable[] rowFromBlockValSets = selectionFetcher.getRow(list.get(i));
        rowEventsList.add(rowFromBlockValSets);
      }
      merge(_rowEventsSet, rowEventsList);
    }

    return _rowEventsSet;
  }

  private Comparator<Integer> getDocValBasedComparator(final List<SelectionSort> sortSequence,
      final DataSchema dataSchema, final Block[] blocks) {

    return new CompositeDocIdValComparator(sortSequence, blocks);
  }

  @Deprecated
  private Serializable[] getRowFromBlockValSets(int docId, Block[] blocks)
      throws Exception {

    final Serializable[] row = new Serializable[_dataSchema.size()];
    int j = 0;
    for (int i = 0; i < _dataSchema.size(); ++i) {
      if (blocks[j] instanceof RealtimeSingleValueBlock) {
        if (blocks[j].getMetadata().hasDictionary()) {
          Dictionary dictionaryReader = blocks[j].getMetadata().getDictionary();
          BlockSingleValIterator bvIter = (BlockSingleValIterator) blocks[j].getBlockValueSet().iterator();
          bvIter.skipTo(docId);
          switch (_dataSchema.getColumnType(i)) {
            case INT:
              row[i] = (Integer) ((IntMutableDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case FLOAT:
              row[i] = (Float) ((FloatMutableDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case LONG:
              row[i] = (Long) ((LongMutableDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case DOUBLE:
              row[i] = (Double) ((DoubleMutableDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case STRING:
              row[i] = (String) ((StringMutableDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            default:
              break;
          }
        } else {
          BlockSingleValIterator bvIter = (BlockSingleValIterator) blocks[j].getBlockValueSet().iterator();
          bvIter.skipTo(docId);
          switch (_dataSchema.getColumnType(i)) {
            case INT:
              row[i] = bvIter.nextIntVal();
              break;
            case FLOAT:
              row[i] = bvIter.nextFloatVal();
              break;
            case LONG:
              row[i] = bvIter.nextLongVal();
              break;
            case DOUBLE:
              row[i] = bvIter.nextDoubleVal();
              break;
            default:
              break;
          }
        }
      } else if (blocks[j] instanceof RealtimeMultiValueBlock) {
        Dictionary dictionaryReader = blocks[j].getMetadata().getDictionary();
        BlockMultiValIterator bvIter = (BlockMultiValIterator) blocks[j].getBlockValueSet().iterator();
        bvIter.skipTo(docId);
        int[] dictIds = new int[blocks[j].getMetadata().getMaxNumberOfMultiValues()];
        int dictSize;
        switch (_dataSchema.getColumnType(i)) {
          case INT_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            int[] rawIntRow = new int[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawIntRow[dictIdx] = (Integer) ((IntMutableDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawIntRow;
            break;
          case FLOAT_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Float[] rawFloatRow = new Float[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawFloatRow[dictIdx] = (Float) ((FloatMutableDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawFloatRow;
            break;
          case LONG_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Long[] rawLongRow = new Long[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawLongRow[dictIdx] = (Long) ((LongMutableDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawLongRow;
            break;
          case DOUBLE_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Double[] rawDoubleRow = new Double[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawDoubleRow[dictIdx] = (Double) ((DoubleMutableDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawDoubleRow;
            break;
          case STRING_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            String[] rawStringRow = new String[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawStringRow[dictIdx] = (String) (((StringMutableDictionary) dictionaryReader).get(dictIds[dictIdx]));
            }
            row[i] = rawStringRow;
            break;
          default:
            break;
        }
      } else if (blocks[j] instanceof UnSortedSingleValueBlock || blocks[j] instanceof SortedSingleValueBlock) {
        if (blocks[j].getMetadata().hasDictionary()) {
          Dictionary dictionaryReader = blocks[j].getMetadata().getDictionary();
          BlockSingleValIterator bvIter = (BlockSingleValIterator) blocks[j].getBlockValueSet().iterator();
          bvIter.skipTo(docId);
          switch (_dataSchema.getColumnType(i)) {
            case INT:
              int dicId = bvIter.nextIntVal();
              row[i] = ((IntDictionary) dictionaryReader).get(dicId);
              break;
            case FLOAT:
              row[i] = ((FloatDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case LONG:
              row[i] = ((LongDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case DOUBLE:
              row[i] = ((DoubleDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case STRING:
              row[i] = ((StringDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            default:
              break;
          }
        } else {
          BlockSingleValIterator bvIter = (BlockSingleValIterator) blocks[j].getBlockValueSet().iterator();
          bvIter.skipTo(docId);
          switch (_dataSchema.getColumnType(i)) {
            case INT:
              row[i] = new Integer(bvIter.nextIntVal());
              break;
            case FLOAT:
              row[i] = new Float(bvIter.nextFloatVal());
              break;
            case LONG:
              row[i] = new Long(bvIter.nextLongVal());
              break;
            case DOUBLE:
              row[i] = new Double(bvIter.nextDoubleVal());
              break;
            default:
              break;
          }
        }
      } else if (blocks[j] instanceof MultiValueBlock) {
        Dictionary dictionaryReader = blocks[j].getMetadata().getDictionary();
        BlockMultiValIterator bvIter = (BlockMultiValIterator) blocks[j].getBlockValueSet().iterator();
        bvIter.skipTo(docId);
        int[] dictIds = new int[blocks[j].getMetadata().getMaxNumberOfMultiValues()];
        int dictSize;
        switch (_dataSchema.getColumnType(i)) {
          case INT_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            int[] rawIntRow = new int[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawIntRow[dictIdx] = ((IntDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawIntRow;
            break;
          case FLOAT_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Float[] rawFloatRow = new Float[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawFloatRow[dictIdx] = ((FloatDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawFloatRow;
            break;
          case LONG_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Long[] rawLongRow = new Long[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawLongRow[dictIdx] = ((LongDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawLongRow;
            break;
          case DOUBLE_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Double[] rawDoubleRow = new Double[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawDoubleRow[dictIdx] = ((DoubleDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawDoubleRow;
            break;
          case STRING_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            String[] rawStringRow = new String[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawStringRow[dictIdx] = (((StringDictionary) dictionaryReader).get(dictIds[dictIdx]));
            }
            row[i] = rawStringRow;
            break;
          default:
            break;
        }
      }
      j++;
    }
    return row;
  }

  private JSONArray getJSonArrayFromRow(Serializable[] poll, DataSchema dataSchema)
      throws JSONException {

    final JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataSchema.size(); ++i) {
      if (_selectionColumns.contains(dataSchema.getColumnName(i))) {
        if (dataSchema.getColumnType(i).isSingleValue()) {
          if (dataSchema.getColumnType(i) == DataType.STRING) {
            jsonArray.put(poll[i]);
          } else {
            jsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(poll[i]));
          }
        } else {
          // Multi-value;

          JSONArray stringJsonArray = new JSONArray();
          //          stringJsonArray.put(poll[i]);
          switch (dataSchema.getColumnType(i)) {
            case STRING_ARRAY:
              String[] stringValues = (String[]) poll[i];
              for (String s : stringValues) {
                stringJsonArray.put(s);
              }
              break;
            case INT_ARRAY:
              int[] intValues = (int[]) poll[i];
              for (int s : intValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(s));
              }
              break;
            case FLOAT_ARRAY:
              float[] floatValues = (float[]) poll[i];
              for (float s : floatValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(s));
              }
              break;
            case LONG_ARRAY:
              long[] longValues = (long[]) poll[i];
              for (long s : longValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(s));
              }
              break;
            case DOUBLE_ARRAY:
              double[] doubleValues = (double[]) poll[i];
              for (double s : doubleValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(s));
              }
              break;
            default:
              break;
          }
          jsonArray.put(stringJsonArray);
        }
      }
    }
    return jsonArray;
  }


  private static Serializable[] getRowFromDataTable(DataTable dt, int rowId) {
    final Serializable[] row = new Serializable[dt.getDataSchema().size()];
    for (int i = 0; i < dt.getDataSchema().size(); ++i) {
      if (dt.getDataSchema().getColumnType(i).isSingleValue()) {
        switch (dt.getDataSchema().getColumnType(i)) {
          case INT:
            row[i] = dt.getInt(rowId, i);
            break;
          case LONG:
            row[i] = dt.getLong(rowId, i);
            break;
          case DOUBLE:
            row[i] = dt.getDouble(rowId, i);
            break;
          case FLOAT:
            row[i] = dt.getFloat(rowId, i);
            break;
          case STRING:
            row[i] = dt.getString(rowId, i);
            break;
          case SHORT:
            row[i] = dt.getShort(rowId, i);
            break;
          case CHAR:
            row[i] = dt.getChar(rowId, i);
            break;
          case BYTE:
            row[i] = dt.getByte(rowId, i);
            break;
          default:
            row[i] = dt.getObject(rowId, i);
            break;
        }
      } else {
        switch (dt.getDataSchema().getColumnType(i)) {
          case INT_ARRAY:
            row[i] = dt.getIntArray(rowId, i);
            break;
          case LONG_ARRAY:
            row[i] = dt.getLongArray(rowId, i);
            break;
          case DOUBLE_ARRAY:
            row[i] = dt.getDoubleArray(rowId, i);
            break;
          case FLOAT_ARRAY:
            row[i] = dt.getFloatArray(rowId, i);
            break;
          case STRING_ARRAY:
            row[i] = dt.getStringArray(rowId, i);
            break;
          case CHAR_ARRAY:
            row[i] = dt.getCharArray(rowId, i);
            break;
          case BYTE_ARRAY:
            row[i] = dt.getByteArray(rowId, i);
            break;
          default:
            row[i] = dt.getObject(rowId, i);
            break;
        }
      }
    }
    return row;
  }
}
