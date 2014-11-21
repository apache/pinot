package com.linkedin.pinot.core.query.selection;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockValSet;
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
  private final PriorityQueue<Serializable[]> _rowEventsSet;

  private Comparator<Integer> _rowDocIdComparator;
  private PriorityQueue<Integer> _rowDocIdSet;

  private final IndexSegment _indexSegment;
  private final boolean _doOrdering;

  public static final Map<DataType, DecimalFormat> DEFAULT_FORMAT_STRING_MAP = new HashMap<DataType, DecimalFormat>();
  static {
    DEFAULT_FORMAT_STRING_MAP.put(DataType.INT, new DecimalFormat("##########"));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.LONG, new DecimalFormat("####################"));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.FLOAT, new DecimalFormat("##########.#####"));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.DOUBLE, new DecimalFormat("####################.##########"));
  }

  public SelectionOperatorService(Selection selections, IndexSegment indexSegment) {
    _indexSegment = indexSegment;
    if ((selections.getSelectionSortSequence() == null) || selections.getSelectionSortSequence().isEmpty()) {
      _doOrdering = false;
    } else {
      _doOrdering = true;
    }
    _sortSequence = appendNatureOrdering(selections.getSelectionSortSequence());
    _selectionColumns = getSelectionColumns(selections.getSelectionColumns());
    _selectionSize = selections.getSize();
    _selectionOffset = selections.getOffset();
    _maxRowSize = _selectionOffset + _selectionSize;
    _dataSchema = getDataSchema(_sortSequence, _selectionColumns, _indexSegment);
    _rowComparator = getComparator(_sortSequence, _dataSchema);
    _rowEventsSet = new PriorityQueue<Serializable[]>(_maxRowSize, _rowComparator);
    _rowDocIdComparator = null;
    _rowDocIdSet = null;
  }

  private List<String> getSelectionColumns(List<String> selectionColumns) {
    if ((selectionColumns.size() == 1) && selectionColumns.get(0).equals("*")) {
      final List<String> newSelectionColumns = new ArrayList<String>();
      for (final String columnName : _indexSegment.getSegmentMetadata().getSchema().getColumnNames()) {
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
    _sortSequence = appendNatureOrdering(selections.getSelectionSortSequence());
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

  public PriorityQueue<Serializable[]> merge(PriorityQueue<Serializable[]> rowEventsSet1,
      PriorityQueue<Serializable[]> rowEventsSet2) {
    final Iterator<Serializable[]> iterator = rowEventsSet2.iterator();
    while (iterator.hasNext()) {
      final Serializable[] row = iterator.next();
      if (rowEventsSet1.size() < _maxRowSize) {
        rowEventsSet1.add(row);
      } else {
        if (_rowComparator.compare(rowEventsSet1.peek(), row) < 0) {
          rowEventsSet1.add(row);
          rowEventsSet1.poll();
        }
      }
    }
    return rowEventsSet1;
  }

  public PriorityQueue<Serializable[]> reduce(Map<ServerInstance, DataTable> selectionResults) {
    _rowEventsSet.clear();
    for (final DataTable dt : selectionResults.values()) {
      for (int rowId = 0; rowId < dt.getNumberOfRows(); ++rowId) {
        final Serializable[] row = getRowFromDataTable(dt, rowId);
        if (_rowEventsSet.size() < _maxRowSize) {
          _rowEventsSet.add(row);
        } else {
          if (_rowComparator.compare(_rowEventsSet.peek(), row) < 0) {
            _rowEventsSet.add(row);
            _rowEventsSet.poll();
          }
        }
      }
    }
    return _rowEventsSet;
  }

  public JSONObject render(PriorityQueue<Serializable[]> finalResults, DataSchema dataSchema, int offset)
      throws Exception {
    final List<JSONArray> rowEventsJSonList = new LinkedList<JSONArray>();
    while (finalResults.size() > offset) {
      ((LinkedList<JSONArray>) rowEventsJSonList).addFirst(getJSonArrayFromRow(finalResults.poll(), dataSchema));
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

  public static DataTable transformRowSetToDataTable(PriorityQueue<Serializable[]> rowEventsSet1, DataSchema dataSchema)
      throws Exception {
    final DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
    dataTableBuilder.open();
    final Iterator<Serializable[]> iterator = rowEventsSet1.iterator();
    while (iterator.hasNext()) {
      final Serializable[] row = iterator.next();
      dataTableBuilder.startRow();
      for (int i = 0; i < dataSchema.size(); ++i) {
        switch (dataSchema.getColumnType(i)) {
          case INT:
            dataTableBuilder.setColumn(i, ((Integer) row[i]).intValue());
            break;
          case LONG:
            dataTableBuilder.setColumn(i, ((Long) row[i]).longValue());
            break;
          case DOUBLE:
            dataTableBuilder.setColumn(i, ((Double) row[i]).doubleValue());
            break;
          case FLOAT:
            dataTableBuilder.setColumn(i, ((Float) row[i]).floatValue());
            break;
          case STRING:
            dataTableBuilder.setColumn(i, ((String) row[i]));
            break;
          default:
            dataTableBuilder.setColumn(i, row[i]);
            break;
        }
      }
      dataTableBuilder.finishRow();
    }
    dataTableBuilder.seal();
    return dataTableBuilder.build();
  }

  public PriorityQueue<Serializable[]> getRowEventsSet() {
    return _rowEventsSet;
  }

  public PriorityQueue<Serializable[]> mergeToRowEventsSet(BlockValSet[] blockValSets) {
    final PriorityQueue<Serializable[]> rowEventsPriorityQueue =
        new PriorityQueue<Serializable[]>(_maxRowSize, _rowComparator);
        while (!_rowDocIdSet.isEmpty()) {
          rowEventsPriorityQueue.add(getRowFromBlockValSets(_rowDocIdSet.poll(), blockValSets));
        }
        merge(_rowEventsSet, rowEventsPriorityQueue);
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
              if (((Integer) o1[i]) > ((Integer) o2[i])) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (((Integer) o1[i]) < ((Integer) o2[i])) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case SHORT:
              if (((Short) o1[i]) > ((Short) o2[i])) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (((Short) o1[i]) < ((Short) o2[i])) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case LONG:
              if (((Long) o1[i]) > ((Long) o2[i])) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (((Long) o1[i]) < ((Long) o2[i])) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case FLOAT:
              if (((Float) o1[i]) > ((Float) o2[i])) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (((Float) o1[i]) < ((Float) o2[i])) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case DOUBLE:
              if (((Double) o1[i]) > ((Double) o2[i])) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (((Double) o1[i]) < ((Double) o2[i])) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case STRING:
              if (((String) o1[i]).compareTo(((String) o2[i])) > 0) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (((String) o1[i]).compareTo(((String) o2[i])) < 0) {
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
        return 0;
      };
    };
  }

  private JSONArray getJSonArrayFromRow(Serializable[] poll, DataSchema dataSchema) throws JSONException {

    final JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataSchema.size(); ++i) {
      if (_selectionColumns.contains(dataSchema.getColumnName(i))) {
        if (dataSchema.getColumnType(i) == DataType.STRING) {
          jsonArray.put(poll[i]);
        } else {
          jsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(poll[i]));
        }
      }
    }
    return jsonArray;
  }

  private static Serializable[] getRowFromDataTable(DataTable dt, int rowId) {
    final Serializable[] row = new Serializable[dt.getDataSchema().size()];
    for (int i = 0; i < dt.getDataSchema().size(); ++i) {
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
    }
    return row;
  }

  public JSONObject render(PriorityQueue<Serializable[]> reduceResults) throws Exception {
    return render(reduceResults, _dataSchema, _selectionOffset);
  }

  private List<SelectionSort> appendNatureOrdering(final List<SelectionSort> selectionSorts) {
    final List<SelectionSort> newSelectionSorts = new ArrayList<SelectionSort>();
    if (selectionSorts != null) {
      newSelectionSorts.addAll(selectionSorts);
    }

    final SelectionSort selectionSort0 = new SelectionSort();
    selectionSort0.setColumn("_segmentId");
    selectionSort0.setIsAsc(true);
    newSelectionSorts.add(selectionSort0);
    final SelectionSort selectionSort1 = new SelectionSort();
    selectionSort1.setColumn("_docId");
    selectionSort1.setIsAsc(true);
    newSelectionSorts.add(selectionSort1);
    return newSelectionSorts;
  }

  private DataSchema getDataSchema(List<SelectionSort> sortSequence, List<String> selectionColumns,
      IndexSegment indexSegment) {
    final List<String> columns = new ArrayList<String>();

    for (final SelectionSort selectionSort : sortSequence) {
      columns.add(selectionSort.getColumn());
    }
    for (final String selectionColumn : selectionColumns) {
      if (!columns.contains(selectionColumn)) {
        columns.add(selectionColumn);
      }
    }
    final DataType[] dataTypes = new DataType[columns.size()];
    for (int i = 0; i < dataTypes.length; ++i) {
      if (columns.get(i).equals("_segmentId") || (columns.get(i).equals("_docId"))) {
        dataTypes[i] = DataType.INT;
      } else {
        dataTypes[i] = indexSegment.getColumnarReader(columns.get(i)).getDataType();
      }
    }
    return new DataSchema(columns.toArray(new String[0]), dataTypes);
  }

  public void iterateOnBlock(BlockDocIdIterator blockDocIdIterator, BlockValSet[] blockValSets) {
    int docId = 0;
    _rowDocIdComparator = getDocIdComparator(_sortSequence, _dataSchema, blockValSets);
    _rowDocIdSet = new PriorityQueue<Integer>(_maxRowSize, _rowDocIdComparator);
    while ((docId = blockDocIdIterator.next()) != Constants.EOF) {
      _numDocsScanned++;
      if (_rowDocIdSet.size() < _maxRowSize) {
        _rowDocIdSet.add(docId);
      } else {
        if (_doOrdering && (_rowDocIdComparator.compare(docId, _rowDocIdSet.peek()) > 0)) {
          _rowDocIdSet.add(docId);
          _rowDocIdSet.poll();
        }
      }
    }
    mergeToRowEventsSet(blockValSets);
  }

  private Serializable[] getRowFromBlockValSets(int docId, BlockValSet[] blockValSets) {

    final Serializable[] row = new Serializable[_dataSchema.size()];
    int j = 0;
    for (int i = 0; i < _dataSchema.size(); ++i) {
      if (_dataSchema.getColumnName(i).equals("_segmentId")) {
        row[i] = _indexSegment.getSegmentName().hashCode();
        continue;
      }
      if (_dataSchema.getColumnName(i).equals("_docId")) {
        row[i] = docId;
        continue;
      }
      switch (_dataSchema.getColumnType(i)) {
        case INT:
          row[i] = (blockValSets[j]).getIntValueAt(blockValSets[j].getDictionaryId(docId));
          break;
        case FLOAT:
          row[i] = (blockValSets[j]).getFloatValueAt(blockValSets[j].getDictionaryId(docId));
          break;
        case LONG:
          row[i] = (blockValSets[j]).getLongValueAt(blockValSets[j].getDictionaryId(docId));
          break;
        case DOUBLE:
          row[i] = (blockValSets[j]).getDoubleValueAt(blockValSets[j].getDictionaryId(docId));
          break;
        case STRING:
          row[i] = (blockValSets[j]).getStringValueAt(blockValSets[j].getDictionaryId(docId));
          break;
        default:
          break;
      }
      j++;
    }
    return row;
  }

  private Comparator<Integer> getDocIdComparator(final List<SelectionSort> sortSequence, final DataSchema dataSchema,
      final BlockValSet[] blockValSets) {

    return new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        for (int i = 0; i < sortSequence.size(); ++i) {
          if ((sortSequence.get(i).getColumn().equals("_segmentId"))
              || (sortSequence.get(i).getColumn().equals("_docId"))) {
            return (o2 - o1);
          }
          final BlockValSet blockValSet = blockValSets[i];
          switch (dataSchema.getColumnType(i)) {
            case INT:
              if (blockValSet.getIntValueAt(blockValSet.getDictionaryId(o1)) > blockValSet.getIntValueAt(blockValSet
                  .getDictionaryId(o2))) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (blockValSet.getIntValueAt(blockValSet.getDictionaryId(o1)) < blockValSet.getIntValueAt(blockValSet
                  .getDictionaryId(o2))) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case LONG:
              if (blockValSet.getLongValueAt(blockValSet.getDictionaryId(o1)) > blockValSet.getLongValueAt(blockValSet
                  .getDictionaryId(o2))) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (blockValSet.getLongValueAt(blockValSet.getDictionaryId(o1)) < blockValSet.getLongValueAt(blockValSet
                  .getDictionaryId(o2))) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case FLOAT:
              if (blockValSet.getFloatValueAt(blockValSet.getDictionaryId(o1)) > blockValSet
                  .getFloatValueAt(blockValSet.getDictionaryId(o2))) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (blockValSet.getFloatValueAt(blockValSet.getDictionaryId(o1)) < blockValSet
                  .getFloatValueAt(blockValSet.getDictionaryId(o2))) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case DOUBLE:
              if (blockValSet.getDoubleValueAt(blockValSet.getDictionaryId(o1)) > blockValSet
                  .getDoubleValueAt(blockValSet.getDictionaryId(o2))) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (blockValSet.getDoubleValueAt(blockValSet.getDictionaryId(o1)) < blockValSet
                  .getDoubleValueAt(blockValSet.getDictionaryId(o2))) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case STRING:
              if (blockValSet.getStringValueAt(blockValSet.getDictionaryId(o1)).compareTo(
                  blockValSet.getStringValueAt(blockValSet.getDictionaryId(o2))) > 0) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (blockValSet.getStringValueAt(blockValSet.getDictionaryId(o1)).compareTo(
                  blockValSet.getStringValueAt(blockValSet.getDictionaryId(o2))) < 0) {
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
        return 0;
      };
    };
  }

}
