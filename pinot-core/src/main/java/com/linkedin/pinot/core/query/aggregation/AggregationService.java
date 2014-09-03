package com.linkedin.pinot.core.query.aggregation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.utils.DefaultIntArray;
import com.linkedin.pinot.core.query.utils.IntArray;


/**
 * Given a docId set, AggregationService will apply aggregation functions to those docs and gather
 * intermediate results.
 *
 */
public class AggregationService {
  private List<AggregationFunction> _aggregationFunctionList;
  private Map<AggregationFunction, List<Serializable>> _aggregationResultsMap;
  private List<List<Serializable>> _aggregationResultsList;

  private int _maxDocPerAggregation = 5000;
  private int[] _docIds = null;
  private IntArray _intArray = null;
  private int _pos = 0;
  private int _numDocsScanned = 0;

  public AggregationService(List<AggregationFunction> aggregationFunctionList) {
    this._aggregationFunctionList = aggregationFunctionList;
    _aggregationResultsMap = new HashMap<AggregationFunction, List<Serializable>>();
    for (AggregationFunction aggregationFunction : _aggregationFunctionList) {
      _aggregationResultsMap.put(aggregationFunction, new ArrayList<Serializable>());
    }
    _aggregationResultsList = new ArrayList<List<Serializable>>();
    for (int i = 0; i < _aggregationFunctionList.size(); ++i) {
      _aggregationResultsList.add(new ArrayList<Serializable>());
    }
    _docIds = new int[_maxDocPerAggregation];
    _intArray = new DefaultIntArray(_docIds);
    _numDocsScanned = 0;
  }

  public List<AggregationFunction> getAggregationFunctionList() {
    return _aggregationFunctionList;
  }

  public void mapDoc(int docId, IndexSegment indexSegment) {
    _docIds[_pos++] = docId;
    if (_pos == _maxDocPerAggregation) {
      kickOffAggregateJob(_docIds, _pos, indexSegment);
      _pos = 0;
    }
  }

  public void finializeMap(IndexSegment indexSegment) {
    if (_pos > 0) {
      kickOffAggregateJob(_docIds, _pos, indexSegment);
    }
  }

  public List<List<Serializable>> aggregateOnSegment(Iterator<Integer> docIdIterator, IndexSegment indexSegment) {
    int i = 0;
    while (docIdIterator.hasNext()) {
      _docIds[i++] = docIdIterator.next();
      if (i == _maxDocPerAggregation) {
        kickOffAggregateJob(_docIds, i, indexSegment);
        i = 0;
      }
    }
    kickOffAggregateJob(_docIds, i, indexSegment);
    return _aggregationResultsList;
  }

  public void kickOffAggregateJob(int[] docIds, int docIdCount, IndexSegment indexSegment) {
    // System.out.println("kickOffAggregateJob with " + docIdCount + " docs");
    for (int i = 0; i < _aggregationFunctionList.size(); ++i) {
      _aggregationResultsList.get(i)
          .add(_aggregationFunctionList.get(i).aggregate(_intArray, docIdCount, indexSegment));
    }
    _numDocsScanned += docIdCount;
  }

  public List<List<Serializable>> getAggregationResultsList() {
    return _aggregationResultsList;
  }

  public DataTable getAggregationResultsDataTable() throws Exception {
    DataSchema schema = getAggregationResultsDataSchema(_aggregationFunctionList);
    DataTableBuilder builder = new DataTableBuilder(schema);
    builder.open();
    for (List<Serializable> aggregationResults : _aggregationResultsList) {
      builder.startRow();
      for (int i = 0; i < aggregationResults.size(); ++i) {
        switch (_aggregationFunctionList.get(i).aggregateResultDataType()) {
          case LONG:
            builder.setColumn(i, ((Long) aggregationResults.get(i)).longValue());
            break;
          case DOUBLE:
            builder.setColumn(i, ((Double) aggregationResults.get(i)).doubleValue());
            break;
          case OBJECT:
            builder.setColumn(i, aggregationResults.get(i));
            break;
          default:
            throw new UnsupportedOperationException("Shouldn't reach here in getAggregationResultsList()");
        }
      }
      builder.finishRow();
    }
    builder.seal();
    return builder.build();
  }

  public static DataSchema getAggregationResultsDataSchema(List<AggregationFunction> aggregationFunctionList)
      throws Exception {
    String[] columnNames = new String[aggregationFunctionList.size()];
    DataType[] columnTypes = new DataType[aggregationFunctionList.size()];
    for (int i = 0; i < aggregationFunctionList.size(); ++i) {
      columnNames[i] = aggregationFunctionList.get(i).getFunctionName();
      columnTypes[i] = aggregationFunctionList.get(i).aggregateResultDataType();
    }
    return new DataSchema(columnNames, columnTypes);
  }

  public int getNumDocsScanned() {
    return _numDocsScanned;
  }
}
