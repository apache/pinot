package com.linkedin.pinot.query.executor;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.query.aggregation.AggregationResult;
import com.linkedin.pinot.query.aggregation.AggregationService;
import com.linkedin.pinot.query.aggregation.CombineLevel;
import com.linkedin.pinot.query.aggregation.CombineReduceService;
import com.linkedin.pinot.query.request.Query;


public class SingleThreadMultiSegmentsWorker implements Callable<List<List<AggregationResult>>> {

  private int _uid = 0;
  private AggregationService _aggregationService = null;
  private List<IndexSegment> _indexSegmentList = null;
  private Query _query = null;

  public SingleThreadMultiSegmentsWorker(int uid, List<IndexSegment> indexSegmentList, Query query) {
    _uid = uid;
    _aggregationService = new AggregationService(query.getAggregationFunction());
    _indexSegmentList = indexSegmentList;
    _query = query;
  }

  @Override
  public List<List<AggregationResult>> call() throws Exception {
    List<List<AggregationResult>> segmentResultList = null;
    for (int i = 0; i < _indexSegmentList.size(); ++i) {
      Iterator<Integer> docIdIterator = _indexSegmentList.get(i).getDocIdIterator(_query.getFilterQuery());
      while (docIdIterator.hasNext()) {
        int doc = docIdIterator.next();
        _aggregationService.mapDoc(doc, _indexSegmentList.get(i));
      }
      _aggregationService.finializeMap(_indexSegmentList.get(i));
      segmentResultList = _aggregationService.getAggregationResultsList();
      CombineReduceService.combine(_aggregationService.getAggregationFunctionList(), segmentResultList,
          CombineLevel.SEGMENT);
      // System.out.println(_uid + " : " + i + " : " + segmentResultList.get(0).get(0).toString());
    }
    return segmentResultList;
  }
}
