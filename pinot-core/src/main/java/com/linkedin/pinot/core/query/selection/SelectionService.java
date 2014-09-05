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

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.indexsegment.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class SelectionService {

  private int _numDocsScanned = 0;
  private final List<SelectionSort> _sortSequence;
  private final List<String> _selectionColumns;
  private final int _selectionSize;
  private final int _selectionOffset;
  private final int _maxRowSize;
  private final DataSchema _dataSchema;
  private final Comparator<Serializable[]> _rowComparator;
  private final Comparator<Integer> _rowDocIdComparator;
  private final PriorityQueue<Serializable[]> _rowEventsSet;
  private final PriorityQueue<Integer> _rowDocIdSet;
  private final ColumnarReader[] _orderingColumnarReaders;
  private final IndexSegment _indexSegment;
  private final boolean _doOrdering;

  public static final Map<DataType, DecimalFormat> DEFAULT_FORMAT_STRING_MAP = new HashMap<DataType, DecimalFormat>();
  static {
    DEFAULT_FORMAT_STRING_MAP.put(DataType.INT, new DecimalFormat("##########"));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.LONG, new DecimalFormat("####################"));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.FLOAT, new DecimalFormat("##########.#####"));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.DOUBLE, new DecimalFormat("####################.##########"));
  }

  public SelectionService(Selection selections, IndexSegment indexSegment) {
    _indexSegment = indexSegment;
    if ((selections.getSelectionSortSequence() == null) || selections.getSelectionSortSequence().isEmpty()) {
      _doOrdering = false;
    } else {
      _doOrdering = true;
    }
    _sortSequence = appendNatureOrdering(selections.getSelectionSortSequence());
    _selectionColumns = selections.getSelectionColumns();
    _selectionSize = selections.getSize();
    _selectionOffset = selections.getOffset();
    _maxRowSize = _selectionOffset + _selectionSize;
    _dataSchema = getDataSchema(_sortSequence, _selectionColumns, _indexSegment);
    _rowComparator = getComparator(_sortSequence, _dataSchema);
    _orderingColumnarReaders = getOrderingColumnarReaders(_sortSequence, _dataSchema, _indexSegment);
    _rowDocIdComparator = getDocIdComparator(_sortSequence, _dataSchema, _orderingColumnarReaders);
    _rowEventsSet = new PriorityQueue<Serializable[]>(_maxRowSize, _rowComparator);
    _rowDocIdSet = new PriorityQueue<Integer>(_maxRowSize, _rowDocIdComparator);
  }

  public SelectionService(Selection selections, DataSchema dataSchema) {
    _indexSegment = null;
    if ((selections.getSelectionSortSequence() == null) || selections.getSelectionSortSequence().isEmpty()) {
      _doOrdering = false;
    } else {
      _doOrdering = true;
    }
    _sortSequence = appendNatureOrdering(selections.getSelectionSortSequence());
    _selectionColumns = selections.getSelectionColumns();
    _selectionSize = selections.getSize();
    _selectionOffset = selections.getOffset();
    _maxRowSize = _selectionOffset + _selectionSize;
    _dataSchema = dataSchema;
    _rowComparator = getComparator(_sortSequence, _dataSchema);
    _orderingColumnarReaders = null;
    _rowDocIdComparator = null;
    _rowEventsSet = new PriorityQueue<Serializable[]>(_maxRowSize, _rowComparator);
    _rowDocIdSet = null;
  }

  public void mapDoc(int docId) {
    if (_rowDocIdSet.size() < _maxRowSize) {
      _rowDocIdSet.add(docId);
    } else {
      if (_doOrdering && (_rowDocIdComparator.compare(docId, _rowDocIdSet.peek()) > 0)) {
        _rowDocIdSet.add(docId);
        _rowDocIdSet.poll();
      }
    }
    _numDocsScanned++;
  }

  public PriorityQueue<Serializable[]> merge(PriorityQueue<Serializable[]> rowEventsSet1,
      PriorityQueue<Serializable[]> rowEventsSet2) {
    Iterator<Serializable[]> iterator = rowEventsSet2.iterator();
    while (iterator.hasNext()) {
      Serializable[] row = iterator.next();
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
    for (DataTable dt : selectionResults.values()) {
      for (int rowId = 0; rowId < dt.getNumberOfRows(); ++rowId) {
        Serializable[] row = getRowFromDataTable(dt, rowId);
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

  public List<JSONObject> render(PriorityQueue<Serializable[]> finalResults, DataSchema dataSchema, int offset)
      throws Exception {
    List<JSONObject> resultJSonList = new LinkedList<JSONObject>();
    while (finalResults.size() > offset) {
      ((LinkedList<JSONObject>) resultJSonList).addFirst(getJSonObjectFromRow(finalResults.poll(), dataSchema));
    }
    return resultJSonList;
  }

  public static DataTable transformRowSetToDataTable(PriorityQueue<Serializable[]> rowEventsSet1, DataSchema dataSchema)
      throws Exception {
    DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
    dataTableBuilder.open();
    Iterator<Serializable[]> iterator = rowEventsSet1.iterator();
    while (iterator.hasNext()) {
      Serializable[] row = iterator.next();
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
    if (_rowEventsSet.isEmpty()) {
      Iterator<Integer> matchedDocIdIterator = _rowDocIdSet.iterator();
      while (matchedDocIdIterator.hasNext()) {
        _rowEventsSet.add(getRowFromDocId(matchedDocIdIterator.next()));
      }
    }
    return _rowEventsSet;
  }

  private Serializable[] getRowFromDocId(Integer docId) {
    Serializable[] row = new Serializable[_dataSchema.size()];
    for (int i = 0; i < _dataSchema.size(); ++i) {
      if (_dataSchema.getColumnName(i).equals("_segmentId")) {
        row[i] = (Serializable) _orderingColumnarReaders[_orderingColumnarReaders.length - 2].getRawValue(docId);
        continue;
      }
      if (_dataSchema.getColumnName(i).equals("_docId")) {
        row[i] = (Serializable) _orderingColumnarReaders[_orderingColumnarReaders.length - 1].getRawValue(docId);
        continue;
      }
      row[i] = (Serializable) _indexSegment.getColumnarReader(_dataSchema.getColumnName(i)).getRawValue(docId);
    }
    return row;
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

  private Comparator<Serializable[]> getEmptyComparator() {
    return new Comparator<Serializable[]>() {
      @Override
      public int compare(Serializable[] o1, Serializable[] o2) {
        return 0;
      };
    };
  }

  private Comparator<Integer> getEmptyDocIdComparator() {
    return new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return 0;
      };
    };
  }

  private Comparator<Integer> getDocIdComparator(final List<SelectionSort> sortSequence, final DataSchema dataSchema,
      final ColumnarReader[] columnarReaders) {
    return new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        for (int i = 0; i < sortSequence.size(); ++i) {
          //          if (columnarReaders[i].getDictionaryId(o1) > columnarReaders[i].getDictionaryId(o2)) {
          //            if (sortSequence.get(i).isIsAsc()) {
          //              return 1;
          //            } else {
          //              return -1;
          //            }
          //          }
          //          if (columnarReaders[i].getDictionaryId(o1) < columnarReaders[i].getDictionaryId(o2)) {
          //            if (sortSequence.get(i).isIsAsc()) {
          //              return -1;
          //            } else {
          //              return 1;
          //            }
          //          }

          switch (dataSchema.getColumnType(i)) {
            case INT:
              if (columnarReaders[i].getIntegerValue(o1) > columnarReaders[i].getIntegerValue(o2)) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (columnarReaders[i].getIntegerValue(o1) < columnarReaders[i].getIntegerValue(o2)) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case SHORT:
              if (columnarReaders[i].getIntegerValue(o1) > columnarReaders[i].getIntegerValue(o2)) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (columnarReaders[i].getIntegerValue(o1) < columnarReaders[i].getIntegerValue(o2)) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case LONG:
              if (columnarReaders[i].getLongValue(o1) > columnarReaders[i].getLongValue(o2)) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (columnarReaders[i].getLongValue(o1) < columnarReaders[i].getLongValue(o2)) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case FLOAT:
              if (columnarReaders[i].getFloatValue(o1) > columnarReaders[i].getFloatValue(o2)) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (columnarReaders[i].getFloatValue(o1) < columnarReaders[i].getFloatValue(o2)) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case DOUBLE:
              if (columnarReaders[i].getDoubleValue(o1) > columnarReaders[i].getDoubleValue(o2)) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (columnarReaders[i].getDoubleValue(o1) < columnarReaders[i].getDoubleValue(o2)) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return -1;
                } else {
                  return 1;
                }
              }
              break;
            case STRING:
              if (columnarReaders[i].getStringValue(o1).compareTo(columnarReaders[i].getStringValue(o2)) > 0) {
                if (!sortSequence.get(i).isIsAsc()) {
                  return 1;
                } else {
                  return -1;
                }
              }
              if (columnarReaders[i].getStringValue(o1).compareTo(columnarReaders[i].getStringValue(o2)) < 0) {
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

  private JSONObject getJSonObjectFromRow(Serializable[] poll, DataSchema dataSchema) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    for (int i = 0; i < dataSchema.size(); ++i) {
      if (_selectionColumns.contains(dataSchema.getColumnName(i))) {
        if (dataSchema.getColumnType(i) == DataType.STRING) {
          jsonObject.put(dataSchema.getColumnName(i), poll[i]);
        } else {
          jsonObject.put(dataSchema.getColumnName(i), DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i))
              .format(poll[i]));
        }
      }
    }
    return jsonObject;
  }

  private static Serializable[] getRowFromDataTable(DataTable dt, int rowId) {
    Serializable[] row = new Serializable[dt.getDataSchema().size()];
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

  public List<JSONObject> render(PriorityQueue<Serializable[]> reduceResults) throws Exception {
    return render(reduceResults, _dataSchema, _selectionOffset);
  }

  private List<SelectionSort> appendNatureOrdering(final List<SelectionSort> selectionSorts) {
    List<SelectionSort> newSelectionSorts = new ArrayList<SelectionSort>();
    if (selectionSorts != null) {
      newSelectionSorts.addAll(selectionSorts);
    }

    SelectionSort selectionSort0 = new SelectionSort();
    selectionSort0.setColumn("_segmentId");
    selectionSort0.setIsAsc(true);
    newSelectionSorts.add(selectionSort0);
    SelectionSort selectionSort1 = new SelectionSort();
    selectionSort1.setColumn("_docId");
    selectionSort1.setIsAsc(true);
    newSelectionSorts.add(selectionSort1);
    return newSelectionSorts;
  }

  private ColumnarReader[] getOrderingColumnarReaders(List<SelectionSort> sortSequence, DataSchema dataSchema,
      final IndexSegment indexSegment) {

    ColumnarReader[] columnarReaders = new ColumnarReader[sortSequence.size()];
    for (int i = 0; i < (sortSequence.size() - 2); ++i) {
      columnarReaders[i] = indexSegment.getColumnarReader(sortSequence.get(i).getColumn());
    }

    columnarReaders[sortSequence.size() - 2] = new ColumnarReader() {

      @Override
      public String getStringValue(int docId) {
        return indexSegment.getSegmentName().hashCode() + "";
      }

      @Override
      public Object getRawValue(int docId) {
        return indexSegment.getSegmentName().hashCode();
      }

      @Override
      public long getLongValue(int docId) {
        return indexSegment.getSegmentName().hashCode();
      }

      @Override
      public int getIntegerValue(int docId) {
        return indexSegment.getSegmentName().hashCode();
      }

      @Override
      public float getFloatValue(int docId) {
        return indexSegment.getSegmentName().hashCode();
      }

      @Override
      public double getDoubleValue(int docId) {
        return indexSegment.getSegmentName().hashCode();
      }

      @Override
      public int getDictionaryId(int docId) {
        return indexSegment.getSegmentName().hashCode();
      }

      @Override
      public DataType getDataType() {
        return DataType.INT;
      }

      @Override
      public String getStringValueFromDictId(int dictId) {
        return dictId + "";
      }

    };
    columnarReaders[sortSequence.size() - 1] = new ColumnarReader() {

      @Override
      public String getStringValue(int docId) {
        return docId + "";
      }

      @Override
      public Object getRawValue(int docId) {
        return docId;
      }

      @Override
      public long getLongValue(int docId) {
        return docId;
      }

      @Override
      public int getIntegerValue(int docId) {
        return docId;
      }

      @Override
      public float getFloatValue(int docId) {
        return docId;
      }

      @Override
      public double getDoubleValue(int docId) {
        return docId;
      }

      @Override
      public int getDictionaryId(int docId) {
        return docId;
      }

      @Override
      public DataType getDataType() {
        return DataType.INT;
      }

      @Override
      public String getStringValueFromDictId(int dictId) {
        return dictId + "";
      }

    };

    return columnarReaders;

  }

  private DataSchema getDataSchema(List<SelectionSort> sortSequence, List<String> selectionColumns,
      IndexSegment indexSegment) {
    List<String> columns = new ArrayList<String>();

    for (SelectionSort selectionSort : sortSequence) {
      columns.add(selectionSort.getColumn());
    }
    for (String selectionColumn : selectionColumns) {
      if (!columns.contains(selectionColumn)) {
        columns.add(selectionColumn);
      }
    }
    DataType[] dataTypes = new DataType[columns.size()];
    for (int i = 0; i < dataTypes.length; ++i) {
      if (columns.get(i).equals("_segmentId") || (columns.get(i).equals("_docId"))) {
        dataTypes[i] = DataType.INT;
      } else {
        dataTypes[i] = indexSegment.getColumnarReader(columns.get(i)).getDataType();
      }
    }
    return new DataSchema(columns.toArray(new String[0]), dataTypes);
  }

}
