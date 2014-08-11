package com.linkedin.pinot.core.query.aggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.response.AggregationResult;
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
  private Map<AggregationFunction, List<AggregationResult>> _aggregationResultsMap;
  private List<List<AggregationResult>> _aggregationResultsList;

  private int _maxDocPerAggregation = 5000;
  private int[] _docIds = null;
  private IntArray _intArray = null;
  private int _pos = 0;
  private int _numDocsScanned = 0;

  public AggregationService(List<AggregationFunction> aggregationFunctionList) {
    this._aggregationFunctionList = aggregationFunctionList;
    _aggregationResultsMap = new HashMap<AggregationFunction, List<AggregationResult>>();
    for (AggregationFunction aggregationFunction : _aggregationFunctionList) {
      _aggregationResultsMap.put(aggregationFunction, new ArrayList<AggregationResult>());
    }
    _aggregationResultsList = new ArrayList<List<AggregationResult>>();
    for (int i = 0; i < _aggregationFunctionList.size(); ++i) {
      _aggregationResultsList.add(new ArrayList<AggregationResult>());
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

  public List<List<AggregationResult>> aggregateOnSegment(Iterator<Integer> docIdIterator, IndexSegment indexSegment) {
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

  public List<List<AggregationResult>> getAggregationResultsList() {
    return _aggregationResultsList;
  }

  public int getNumDocsScanned() {
    return _numDocsScanned;
  }
}
