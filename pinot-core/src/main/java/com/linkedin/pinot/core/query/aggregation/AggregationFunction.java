package com.linkedin.pinot.core.query.aggregation;

import java.io.Serializable;
import java.util.List;

import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.utils.IntArray;


/**
 * By extending this interface, one can access the index segment data, produce intermediate results for a given
 * segment, then aggregate those results on instance and router level.
 * 
 */
public interface AggregationFunction<AggregateResult extends Serializable, ReduceResult extends Serializable> extends
    Serializable {

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
  AggregateResult aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment);

  /**
   * A map function used by GroupBy query. It gets one docId and merge it into an existed AggregateResult.
   * 
   * @param currentResult
   * @param docId
   * @param indexSegment
   * @return arbitrary map function results
   */
  AggregateResult aggregate(AggregateResult currentResult, int docId, IndexSegment indexSegment);

  /**
   * Take a list of intermediate results and do intermediate merge.
   * 
   * @param aggregationResultList
   * @param combineLevel
   * @return intermediate merge results
   */
  List<AggregateResult> combine(List<AggregateResult> aggregationResultList, CombineLevel combineLevel);

  /**
   * Take two intermediate results and do merge.
   * 
   * @param aggregationResult0
   * @param aggregationResult1
   * @return intermediate merge results
   */
  AggregateResult combineTwoValues(AggregateResult aggregationResult0, AggregateResult aggregationResult1);

  /**
   * Take a list of intermediate results and merge them.
   * 
   * @param aggregationResultList
   * @return final merged results
   */
  ReduceResult reduce(List<AggregateResult> combinedResultList);

  /**
   * Return a JsonObject representation for the final aggregation result.
   * 
   * @param finalAggregationResult
   * @return final results in Json format
   */
  JSONObject render(ReduceResult finalAggregationResult);

  /**
   * Return data type of aggregateResult.
   * 
   * @return DataType
   */
  DataType aggregateResultDataType();

  /**
   * Return function name + column name. Should be unique in one query.
   * 
   * @return functionName
   */
  String getFunctionName();
}
