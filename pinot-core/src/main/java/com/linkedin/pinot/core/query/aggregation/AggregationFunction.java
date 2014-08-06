package com.linkedin.pinot.core.query.aggregation;

import java.io.Serializable;
import java.util.List;

import org.json.JSONObject;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.response.AggregationResult;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.utils.IntArray;


/**
 * By extending this interface, one can access the index segment data, produce intermediate results for a given
 * segment, then aggregate those results on instance and router level.
 * 
 */
public interface AggregationFunction extends Serializable {

  /**
   * Initialized the aggregation funtion from aggregation info
   * @param aggregationInfo
   */
  public void init(AggregationInfo aggregationInfo);

  /**
   * The map function. It can get the docId  from the docIds array containing value from 0 to docIdCount.
   * All the docIds with array indexes >= docIdCount should be ignored
   * 
   * @param docIds
   * @param docIdCount
   * @param indexSegment
   * @return arbitrary map function results
   */
  AggregationResult aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment);

  /**
   * Take a list of intermediate results and do intermediate merge.
   * 
   * @param aggregationResultList
   * @param combineLevel
   * @return intermediate merge results
   */
  AggregationResult combine(List<AggregationResult> aggregationResultList, CombineLevel combineLevel);

  /**
   * Take a list of intermediate results and merge them.
   * 
   * @param aggregationResultList
   * @return final merged results
   */
  AggregationResult reduce(List<AggregationResult> aggregationResultList);

  /**
   * Return a JsonObject representation for the final aggregation result.
   * 
   * @param finalAggregationResult
   * @return final results in Json format
   */
  JSONObject render(AggregationResult finalAggregationResult);

}
